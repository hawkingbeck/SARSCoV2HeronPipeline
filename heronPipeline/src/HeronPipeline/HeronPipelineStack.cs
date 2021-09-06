using Amazon.CDK;
using Amazon.CDK.AWS.S3;
using Amazon.CDK.AWS.ECS;
using Amazon.CDK.AWS.EC2;
using Amazon.CDK.AWS.ECR;
using Amazon.CDK.AWS.IAM;
using Amazon.CDK.AWS.Logs;
using Amazon.CDK.AWS.EFS;


namespace HeronPipeline
{
    public class HeronPipelineStack : Stack
    {
        internal HeronPipelineStack(Construct scope, string id, IStackProps props = null) : base(scope, id, props)
        {
            //++++++++++++++++++++++++++++++++++++++++++
            // VPC
            //++++++++++++++++++++++++++++++++++++++++++
            var vpc = new Vpc(this, "vpc", new VpcProps
            {
                MaxAzs = 1, ///TODO: Increase this once EIP's are freed
                Cidr = "11.0.0.0/16",
                NatGateways = 1,
                SubnetConfiguration = new[]{
                    new SubnetConfiguration {
                        CidrMask = 24,
                        Name = "ingress",
                        SubnetType = SubnetType.PUBLIC
                    },
                    new SubnetConfiguration {
                        CidrMask = 24,
                        Name = "application",
                        SubnetType = SubnetType.PRIVATE
                    }},
            });

            var secGroup = new SecurityGroup(this, "vpcSecurityGroup", new SecurityGroupProps
            {
                Vpc = vpc,
                AllowAllOutbound = true
            });
            secGroup.AddIngressRule(Peer.AnyIpv4(), Port.AllIcmp(), "All Incoming");
            secGroup.Node.AddDependency(vpc);

            //++++++++++++++++++++++++++++++++++++++++++
            // File System (EFS)
            //++++++++++++++++++++++++++++++++++++++++++
            var pipelineEFS = new Amazon.CDK.AWS.EFS.FileSystem(this, "pipelineEFS", new FileSystemProps
            {
                Vpc = vpc,
                ThroughputMode = ThroughputMode.PROVISIONED,
                ProvisionedThroughputPerSecond = Size.Mebibytes(20),
                PerformanceMode = PerformanceMode.GENERAL_PURPOSE,
                RemovalPolicy = RemovalPolicy.DESTROY,
                Encrypted = false
                //SecurityGroup = vpc.VpcDefaultSecurityGroup
            });


            //var pipelineEFSMountTarget = new CfnMountTarget(this, "pipelineEFSMountTarget", new CfnMountTargetProps
            //{
            //    FileSystemId = pipelineEFS.FileSystemId,
            //    SecurityGroups = new string[] { vpc.VpcDefaultSecurityGroup },
            //    SubnetId = vpc.PrivateSubnets[0].SubnetId
            //});

            var pipelineEFSAccessPoint = new AccessPoint(this, "pipelineEFSAccessPoint", new AccessPointProps
            {
                FileSystem = pipelineEFS,
                PosixUser = new PosixUser { Gid = "1000", Uid = "1000"},
                CreateAcl = new Acl { OwnerUid = "1000", OwnerGid = "1000", Permissions="0777"},
                Path = "/efs"
            });
            pipelineEFSAccessPoint.Node.AddDependency(pipelineEFS);
            //pipelineEFSAccessPoint.Node.AddDependency(pipelineEFSMountTarget);

            var volume1 = new Amazon.CDK.AWS.ECS.Volume();
            volume1.EfsVolumeConfiguration = new EfsVolumeConfiguration
            {
                FileSystemId = pipelineEFS.FileSystemId,
                AuthorizationConfig = new AuthorizationConfig
                {
                    AccessPointId = pipelineEFSAccessPoint.AccessPointId,
                    Iam = "ENABLED"
                },
                TransitEncryption = "ENABLED"
            };
            volume1.Name = "efsVolume";
            //volume1.
            

            //++++++++++++++++++++++++++++++++++++++++++
            //+++++++++++++++ Storage ++++++++++++++++++
            //++++++++++++++++++++++++++++++++++++++++++

            new Bucket(this, "dataBucket", new BucketProps
            {
                Versioned = true,
                RemovalPolicy = RemovalPolicy.DESTROY,
                AutoDeleteObjects = true
            });


            //++++++++++++++++++++++++++++++++++++++++++
            //Fargate Cluster
            //++++++++++++++++++++++++++++++++++++++++++
            var ecsExecutionRole = new Role(this, "fargateExecutionRole", new RoleProps{
                Description = "Role for fargate execution",
                AssumedBy = new ServicePrincipal("ec2.amazonaws.com"), //The service that needs to use this role
            });
            ecsExecutionRole.AddManagedPolicy(ManagedPolicy.FromAwsManagedPolicyName("AmazonEC2FullAccess"));
            ecsExecutionRole.AddManagedPolicy(ManagedPolicy.FromAwsManagedPolicyName("AmazonSQSFullAccess"));
            ecsExecutionRole.AddManagedPolicy(ManagedPolicy.FromAwsManagedPolicyName("AmazonS3FullAccess"));
            ecsExecutionRole.AddManagedPolicy(ManagedPolicy.FromAwsManagedPolicyName("AmazonDynamoDBFullAccess"));
            ecsExecutionRole.AddManagedPolicy(ManagedPolicy.FromManagedPolicyArn(this, "ecsExecutionRolePolicy", "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"));
            ecsExecutionRole.AddManagedPolicy(ManagedPolicy.FromAwsManagedPolicyName("CloudWatchEventsFullAccess"));

            var policyStatement = new PolicyStatement(new PolicyStatementProps
            {
                Effect = Effect.ALLOW,
                Actions = new string[] { "sts:AssumeRole" },
                Principals = new ServicePrincipal[] { new ServicePrincipal("ecs-tasks.amazonaws.com") }
            });
            
            ecsExecutionRole.AssumeRolePolicy.AddStatements(policyStatement);

            var cluster = new Cluster(this, "heronCluster", new ClusterProps
            {
                Vpc = vpc,
                EnableFargateCapacityProviders = true
            });

            // +++++++++++++++++++++++++++++++++++++++++++++
            // +++++++++++++++++++++++++++++++++++++++++++++
            // +++++++++++++ TASK DEFINTIONS +++++++++++++++
            // +++++++++++++++++++++++++++++++++++++++++++++
            // +++++++++++++++++++++++++++++++++++++++++++++

            // +++++++++++++++++++++++++++++++++++++++++++++
            // Task defintion for LQP metadata prepreation
            // +++++++++++++++++++++++++++++++++++++++++++++
            var lqpDownloadMetaDataTask = new TaskDefinition(this, "lqpDownloadMetaDataTask", new TaskDefinitionProps
            {
                Family = "lqpDownloadMetaDataTask",
                Cpu = "1024",
                MemoryMiB = "4096",
                EphemeralStorageGiB = 50,
                NetworkMode = NetworkMode.AWS_VPC,
                Compatibility = Compatibility.FARGATE,
                ExecutionRole = ecsExecutionRole,
                TaskRole = ecsExecutionRole,
                Volumes = new Amazon.CDK.AWS.ECS.Volume[] { volume1 }

            });

            var lqpDownloadMetaDataLogGroup = new LogGroup(this, "lqpDownloadMetaDataLogGroup", new LogGroupProps
            {
                LogGroupName = "lqpDownloadMetaDataLogGroup",
                Retention = RetentionDays.ONE_WEEK,
                RemovalPolicy = RemovalPolicy.DESTROY
            });

            lqpDownloadMetaDataTask.AddContainer("lqpContainer", new ContainerDefinitionOptions
            {
                Image = ContainerImage.FromEcrRepository(
                    Repository.FromRepositoryArn(this, "id", "arn:aws:ecr:eu-west-1:889562587392:low_quality_placement/low_quality_placement"),
                    "latest"),
                Logging = new AwsLogDriver(new AwsLogDriverProps { StreamPrefix = "lqpDownloadMetaData", LogGroup = lqpDownloadMetaDataLogGroup }),
                EntryPoint = new string[] { "sh", "/home/app/lqp-fargateDataPrep.sh" }
                
            });
            var container = lqpDownloadMetaDataTask.FindContainer("lqpContainer");
            container.AddMountPoints(new MountPoint[] {
                    new MountPoint
                    {
                        SourceVolume = "efsVolume",
                        ContainerPath = "/mnt/efs0",
                        ReadOnly = false,
                    }
                });

            //lqpDownloadMetaDataTask.DefaultContainer.AddMountPoints(new MountPoint[] {
            //        new MountPoint
            //        {
            //            SourceVolume = "efsVolume",
            //            ContainerPath = "/mnt/efs0",
            //            ReadOnly = false,
            //        }
            //    });

            //if (lqpDownloadMetaDataTask.DefaultContainer != null) {
            //    lqpDownloadMetaDataTask.DefaultContainer.AddMountPoints(new MountPoint[] {
            //        new MountPoint
            //        {
            //            SourceVolume = "efsVolume",
            //            ContainerPath = "/mnt/efs0",
            //            ReadOnly = false,
            //        }
            //    });
            //}
        }
    }
}
