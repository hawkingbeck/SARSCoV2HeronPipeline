using Amazon.CDK;
using Amazon.CDK.AWS.S3;
using Amazon.CDK.AWS.ECS;
using Amazon.CDK.AWS.EC2;
using Amazon.CDK.AWS.ECR;
using Amazon.CDK.AWS.IAM;
using Amazon.CDK.AWS.Logs;
using Amazon.CDK.AWS.EFS;
using Amazon.CDK.AWS.StepFunctions;
using Amazon.CDK.AWS.StepFunctions.Tasks;
using Amazon.CDK.AWS.Lambda;
using Amazon.CDK.AWS.Lambda.Python;


// /home/ec2-user/.nvm/versions/node/v16.3.0
namespace HeronPipeline
{
    public class HeronPipelineStack : Stack
    {
        internal HeronPipelineStack(Construct scope, string id, IStackProps props = null) : base(scope, id, props)
        {

            //++++++++++++++++++++++++++++++++++++++++++
            // VPC
            //++++++++++++++++++++++++++++++++++++++++++
            var vpc = new Vpc(this, "vpc", new VpcProps{
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

            var secGroup = new SecurityGroup(this, "vpcSecurityGroup", new SecurityGroupProps{
                Vpc = vpc,
                AllowAllOutbound = true
            });
            secGroup.AddIngressRule(Peer.AnyIpv4(), Port.AllIcmp(), "All Incoming");
            secGroup.AddIngressRule(Peer.AnyIpv4(), Port.Tcp(2049), "EFS Port");
            secGroup.AddIngressRule(Peer.AnyIpv4(), Port.AllTraffic(), "All Traffic");
            secGroup.Node.AddDependency(vpc);

            //++++++++++++++++++++++++++++++++++++++++++
            // File System (EFS)
            //++++++++++++++++++++++++++++++++++++++++++
            var pipelineEFS = new Amazon.CDK.AWS.EFS.FileSystem(this, "pipelineEFS", new FileSystemProps{
                Vpc = vpc,
                ThroughputMode = ThroughputMode.PROVISIONED,
                ProvisionedThroughputPerSecond = Size.Mebibytes(20),
                PerformanceMode = PerformanceMode.GENERAL_PURPOSE,
                RemovalPolicy = RemovalPolicy.DESTROY,
                Encrypted = false,
                SecurityGroup = secGroup
            });

            var pipelineEFSAccessPoint = new AccessPoint(this, "pipelineEFSAccessPoint", new AccessPointProps{
                FileSystem = pipelineEFS,
                PosixUser = new PosixUser { Gid = "1000", Uid = "1000" },
                CreateAcl = new Acl { OwnerUid = "1000", OwnerGid = "1000", Permissions = "0777" },
                Path = "/efs"
            });
            pipelineEFSAccessPoint.Node.AddDependency(pipelineEFS);

            var volume1 = new Amazon.CDK.AWS.ECS.Volume();
            volume1.EfsVolumeConfiguration = new EfsVolumeConfiguration{
                FileSystemId = pipelineEFS.FileSystemId,
                AuthorizationConfig = new AuthorizationConfig{
                    AccessPointId = pipelineEFSAccessPoint.AccessPointId,
                    Iam = "ENABLED"
                },
                TransitEncryption = "ENABLED"
            };
            volume1.Name = "efsVolume";


            //++++++++++++++++++++++++++++++++++++++++++
            //+++++++++++++++ Storage ++++++++++++++++++
            //++++++++++++++++++++++++++++++++++++++++++
            new Bucket(this, "dataBucket", new BucketProps{
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

            var policyStatement = new PolicyStatement(new PolicyStatementProps{
                Effect = Effect.ALLOW,
                Actions = new string[] { "sts:AssumeRole" },
                Principals = new ServicePrincipal[] { new ServicePrincipal("ecs-tasks.amazonaws.com") }
            });

            ecsExecutionRole.AssumeRolePolicy.AddStatements(policyStatement);

            var cluster = new Cluster(this, "heronCluster", new ClusterProps{
                Vpc = vpc,
                EnableFargateCapacityProviders = true
            });

            // +++++++++++++++++++++++++++++++++++++++++++++
            // +++++++++++++++++++++++++++++++++++++++++++++
            // +++++++++++++ TASK DEFINTIONS +++++++++++++++
            // +++++++++++++++++++++++++++++++++++++++++++++
            // +++++++++++++++++++++++++++++++++++++++++++++

            // +++++++++++++++++++++++++++++++++++++++++++++
            // Task definition for LQP metadata prepreation
            // +++++++++++++++++++++++++++++++++++++++++++++
            var lqpImage = ContainerImage.FromEcrRepository(
                    Repository.FromRepositoryArn(this, "lqpImage", "arn:aws:ecr:eu-west-1:889562587392:low_quality_placement/low_quality_placement"),
                    "latest");
            var lqpDownloadMetaDataTaskDefinition = new TaskDefinition(this, "lqpDownloadMetaDataTask", new TaskDefinitionProps{
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

            lqpDownloadMetaDataTaskDefinition.AddContainer("lqpDownLoadMetaDataContainer", new Amazon.CDK.AWS.ECS.ContainerDefinitionOptions
            {
                Image = lqpImage,
                Logging = new AwsLogDriver(new AwsLogDriverProps
                {
                    StreamPrefix = "lqpDownloadMetaData",
                    LogGroup = new LogGroup(this, "lqpDownloadMetaDataLogGroup", new LogGroupProps
                    {
                        LogGroupName = "lqpDownloadMetaDataLogGroup",
                        Retention = RetentionDays.ONE_WEEK,
                        RemovalPolicy = RemovalPolicy.DESTROY
                    })
                }),
                EntryPoint = new string[] { "sh", "/home/app/lqp-fargateDataPrep.sh" }
            });

            var lqpDownloadMetaDataContainer = lqpDownloadMetaDataTaskDefinition.FindContainer("lqpDownLoadMetaDataContainer");
            lqpDownloadMetaDataContainer.AddMountPoints(new MountPoint[] {
                    new MountPoint {
                        SourceVolume = "efsVolume",
                        ContainerPath = "/mnt/efs0",
                        ReadOnly = false,
                    }
                });

            var lqpPrepareMetaDataTask = new EcsRunTask(this, "lqpPrepareMetaDataECSTask", new EcsRunTaskProps{
                IntegrationPattern = IntegrationPattern.RUN_JOB,
                Cluster = cluster,
                TaskDefinition = lqpDownloadMetaDataTaskDefinition,
                AssignPublicIp = true,
                LaunchTarget = new EcsFargateLaunchTarget(),
                ContainerOverrides = new ContainerOverride[] {
                    new ContainerOverride {
                        ContainerDefinition = lqpDownloadMetaDataContainer,
                        Environment = new TaskEnvironmentVariable[] {
                            new TaskEnvironmentVariable {
                                Name = "LQP_DATA_ROOT",
                                Value = "/mnt/efs0/lqpModel/metaData"
                            },
                            new TaskEnvironmentVariable {
                                Name = "DATE_PARTITION",
                                Value = "$.date"
                            }
                        }
                    }
                },
                ResultPath = null
            });

            // +++++++++++++++++++++++++++++++++++++++++++++
            // Task definition for LQP metadata processing
            // +++++++++++++++++++++++++++++++++++++++++++++
            var lqpRunBaseTaskDefinition = new TaskDefinition(this, "lqpRunBaseTask", new TaskDefinitionProps{
                Family = "lqpRunBaseTask",
                Cpu = "4096",
                MemoryMiB = "30720",
                EphemeralStorageGiB = 50,
                NetworkMode = NetworkMode.AWS_VPC,
                Compatibility = Compatibility.FARGATE,
                ExecutionRole = ecsExecutionRole,
                TaskRole = ecsExecutionRole,
                Volumes = new Amazon.CDK.AWS.ECS.Volume[] { volume1 }
            });

            lqpRunBaseTaskDefinition.AddContainer("lqpRunBaseContainer", new Amazon.CDK.AWS.ECS.ContainerDefinitionOptions{
                Image = lqpImage,
                Logging = new AwsLogDriver(new AwsLogDriverProps{
                    StreamPrefix = "lqpRunBase",
                    LogGroup = new LogGroup(this, "lqpRunBaseLogGroup", new LogGroupProps{
                        LogGroupName = "lqpRunBaseLogGroup",
                        Retention = RetentionDays.ONE_WEEK,
                        RemovalPolicy = RemovalPolicy.DESTROY
                    })
                }),
                EntryPoint = new string[] { "sh", "/home/app/lqp-fargateMpBase.sh" }
            });
            var lqpRunBaseContainer = lqpRunBaseTaskDefinition.FindContainer("lqpRunBaseContainer");
            lqpRunBaseContainer.AddMountPoints(new MountPoint[] {
                    new MountPoint {
                        SourceVolume = "efsVolume",
                        ContainerPath = "/mnt/efs0",
                        ReadOnly = false,
                    }
                });

            var lqpRunBaseTask = new EcsRunTask(this, "lqpRunBaseECSTask", new EcsRunTaskProps{
                IntegrationPattern = IntegrationPattern.RUN_JOB,
                Cluster = cluster,
                TaskDefinition = lqpRunBaseTaskDefinition,
                AssignPublicIp = true,
                LaunchTarget = new EcsFargateLaunchTarget(),
                ContainerOverrides = new ContainerOverride[] {
                    new ContainerOverride {
                        ContainerDefinition = lqpRunBaseContainer,
                        Environment = new TaskEnvironmentVariable[] {
                            new TaskEnvironmentVariable {
                                Name = "LQP_DATA_ROOT",
                                Value = "/mnt/efs0/lqpModel/metaData"
                            },
                            new TaskEnvironmentVariable {
                                Name = "DATE_PARTITION",
                                Value = "$.date"
                            }
                        }
                    }
                },
                ResultPath = null
            });

            // +++++++++++++++++++++++++++++++++++++++++++++
            // +++++++++++++++++++++++++++++++++++++++++++++
            // ++++++++++++ Lambda Functions +++++++++++++++
            // +++++++++++++++++++++++++++++++++++++++++++++
            // +++++++++++++++++++++++++++++++++++++++++++++
            var createRunBaseConfigFunction = new PythonFunction(this, "createRunBaseConfigFunction", new PythonFunctionProps{
                Entry = "src/functions/createRunBaseConfig",
                Runtime = Runtime.PYTHON_3_7,
                Index = "app.py",
                Handler = "lambda_handler"
            });
            var lqpCreateRunBaseConfigTask = new LambdaInvoke(this, "lqpCreateRunBaseConfig", new LambdaInvokeProps{
                LambdaFunction = createRunBaseConfigFunction
            });

            // +++++++++++++++++++++++++++++++++++++++++++++
            // +++++++++++++++++++++++++++++++++++++++++++++
            // +++++++++++++ State Machines ++++++++++++++++
            // +++++++++++++++++++++++++++++++++++++++++++++
            // +++++++++++++++++++++++++++++++++++++++++++++
            var lqpRunBaseMapState = new Map(this, "lqpRunBaseMap", new MapProps{
                InputPath = "$",
                ItemsPath = "$.runbaseConfig.batches",
                ResultPath = "null"
                // Parameters = {
                // {"", ""} 
              //}
            });
            // lqpRunBaseMapState.Iterator = 


            var chain = Chain
                .Start(lqpPrepareMetaDataTask)
                .Next(lqpCreateRunBaseConfigTask);


            var lqpPrepareMetaDataStateMachine = new StateMachine(this, "lqpPrepMetaDataStateMachine", new StateMachineProps{
                Definition = chain
            });
        }
    }
}
