using System.Collections;
using System.Collections.Generic;
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
using Amazon.CDK.AWS.DynamoDB;
using Amazon.CDK.AWS.SQS;
using Stack = Amazon.CDK.Stack;
using Queue = Amazon.CDK.AWS.SQS.Queue;


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
            var pipelineBucket = new Bucket(this, "dataBucket", new BucketProps{
                Versioned = true,
                RemovalPolicy = RemovalPolicy.DESTROY,
                AutoDeleteObjects = true
            });


            var samplesTable = new Table(this, "samplesTable", new TableProps{
                BillingMode = BillingMode.PAY_PER_REQUEST,
                PartitionKey = new Attribute { Name = "cogUkId", Type = AttributeType.STRING},
                SortKey = new Attribute { Name = "runCompleteDate", Type = AttributeType.NUMBER}
            });

            samplesTable.AddGlobalSecondaryIndex(new GlobalSecondaryIndexProps {
                IndexName = "lastChangedDate",
                PartitionKey = new Attribute { Name = "cogUkId", Type = AttributeType.STRING},
                SortKey = new Attribute { Name = "lastChangedDate", Type = AttributeType.NUMBER},
                ProjectionType = ProjectionType.ALL
            });

            var sequencesTable = new Table(this, "sequencesTable", new TableProps {
                BillingMode = BillingMode.PAY_PER_REQUEST,
                PartitionKey = new Attribute { Name = "seqHash", Type = AttributeType.STRING}
            });

            //++++++++++++++++++++++++++++++++++++++++++
            //SQS Queues
            //++++++++++++++++++++++++++++++++++++++++++
            var dailyProcessingQueue = new Queue(this, "dailyProcessingQueue", new QueueProps {
                ContentBasedDeduplication = true,
                Fifo = true,
                FifoThroughputLimit = FifoThroughputLimit.PER_MESSAGE_GROUP_ID,
                DeduplicationScope = DeduplicationScope.MESSAGE_GROUP
            });

            var reprocessingQueue = new Queue(this, "reprocessingQueue", new QueueProps {
                ContentBasedDeduplication = true,
                Fifo = true,
                FifoThroughputLimit = FifoThroughputLimit.PER_MESSAGE_GROUP_ID,
                DeduplicationScope = DeduplicationScope.MESSAGE_GROUP
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
            var retryItem = new RetryProps {
              BackoffRate = 5,
              Interval = Duration.Seconds(1),
              MaxAttempts = 5,
              Errors = new string[] {"States.ALL"}
            };
            // +++++++++++++++++++++++++++++++++++++++++++++
            // Task definition for LQP metadata preparation
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
                                Value = JsonPath.StringAt("$.date") //"$.date"
                            }
                        }
                    }
                },
                ResultPath = JsonPath.DISCARD
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

            var lqpRunBaseTask = new EcsRunTask(this, "lqpRunBaseStepFunctionTask", new EcsRunTaskProps{
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
                                Name = "DATE",
                                Value = JsonPath.StringAt("$.date")
                            },
                            new TaskEnvironmentVariable {
                              Name = "LSB_JOBINDEX",
                              Value = JsonPath.StringAt("$.partition")
                            }
                            
                        }
                    }
                },
                ResultPath = JsonPath.DISCARD
            });
            lqpRunBaseTask.AddRetry(retryItem);

            // +++++++++++++++++++++++++++++++++++++++++++++
            // Task definition for LQP tidy
            // +++++++++++++++++++++++++++++++++++++++++++++
            var lqpTidyTaskDefinition = new TaskDefinition(this, "lqpTidyTask", new TaskDefinitionProps{
                Family = "lqpTidyTask",
                Cpu = "512",
                MemoryMiB = "2048",
                EphemeralStorageGiB = 50,
                NetworkMode = NetworkMode.AWS_VPC,
                Compatibility = Compatibility.FARGATE,
                ExecutionRole = ecsExecutionRole,
                TaskRole = ecsExecutionRole,
                Volumes = new Amazon.CDK.AWS.ECS.Volume[] { volume1 }
            });
            lqpTidyTaskDefinition.AddContainer("lqpTidyContainer", new Amazon.CDK.AWS.ECS.ContainerDefinitionOptions
            {
                Image = lqpImage,
                Logging = new AwsLogDriver(new AwsLogDriverProps
                {
                    StreamPrefix = "lqpTidy",
                    LogGroup = new LogGroup(this, "lqpTidyLogGroup", new LogGroupProps
                    {
                        LogGroupName = "lqpTidyLogGroup",
                        Retention = RetentionDays.ONE_WEEK,
                        RemovalPolicy = RemovalPolicy.DESTROY
                    })
                }),
                EntryPoint = new string[] { "python", "/home/app/lqp-fargateTidy.py" }
            });
            var lqpTidyContainer = lqpTidyTaskDefinition.FindContainer("lqpTidyContainer");
            lqpTidyContainer.AddMountPoints(new MountPoint[] {
                    new MountPoint {
                        SourceVolume = "efsVolume",
                        ContainerPath = "/mnt/efs0",
                        ReadOnly = false,
                    }
                });

            var lqpTidyTask = new EcsRunTask(this, "lqpTidyStepFunctionTask", new EcsRunTaskProps {
                IntegrationPattern = IntegrationPattern.RUN_JOB,
                Cluster = cluster,
                TaskDefinition = lqpTidyTaskDefinition,
                AssignPublicIp = true,
                LaunchTarget = new EcsFargateLaunchTarget(),
                ContainerOverrides = new ContainerOverride[] {
                    new ContainerOverride {
                        ContainerDefinition = lqpTidyContainer,
                        Environment = new TaskEnvironmentVariable[] {
                            new TaskEnvironmentVariable {
                                Name = "LQP_DATA_ROOT",
                                Value = "/mnt/efs0/lqpModel/metaData"
                            },
                            new TaskEnvironmentVariable {
                                Name = "DATE_PARTITION",
                                Value = JsonPath.StringAt("$.date")
                            },
                            new TaskEnvironmentVariable {
                              Name = "PARTITION_COUNT",
                              Value = "119"
                            },
                            new TaskEnvironmentVariable {
                              Name = "HERON_SAMPLES_BUCKET",
                              Value = pipelineBucket.BucketName
                            }
                        }
                    }
                },
                ResultPath = JsonPath.DISCARD
            });


            // +++++++++++++++++++++++++++++++++++++++++++++
            // Task definition for adding squences to queue
            // +++++++++++++++++++++++++++++++++++++++++++++

            var addSequencesToQueueImage = ContainerImage.FromAsset("src/images/addSequencesToQueue");
            var addSequencesToQueueTaskDefinition = new TaskDefinition(this, "addSequencesToQueueTaskDefinition", new TaskDefinitionProps{
                Family = "addSequencesToQueue",
                Cpu = "256",
                MemoryMiB = "512",
                NetworkMode = NetworkMode.AWS_VPC,
                Compatibility = Compatibility.FARGATE,
                ExecutionRole = ecsExecutionRole,
                TaskRole = ecsExecutionRole
            });
            addSequencesToQueueTaskDefinition.AddContainer("addSequencesToQueueContainer", new Amazon.CDK.AWS.ECS.ContainerDefinitionOptions
            {
                Image = addSequencesToQueueImage,
                Logging = new AwsLogDriver(new AwsLogDriverProps
                {
                    StreamPrefix = "addSequencesToQueue",
                    LogGroup = new LogGroup(this, "addSequencesToQueueLogGroup", new LogGroupProps
                    {
                        LogGroupName = "addSequencesToQueueLogGroup",
                        Retention = RetentionDays.ONE_WEEK,
                        RemovalPolicy = RemovalPolicy.DESTROY
                    })
                }),
                EntryPoint = new string[] { "python", "/home/app/app.py" }
            });
            var addSequencesToQueueContainer = addSequencesToQueueTaskDefinition.FindContainer("addSequencesToQueueContainer");

            var addSequencesToQueueTask = new EcsRunTask(this, "addSequencesToQueueTask", new EcsRunTaskProps
            {
                IntegrationPattern = IntegrationPattern.RUN_JOB,
                Cluster = cluster,
                TaskDefinition = addSequencesToQueueTaskDefinition,
                AssignPublicIp = true,
                LaunchTarget = new EcsFargateLaunchTarget(),
                ContainerOverrides = new ContainerOverride[] {
                    new ContainerOverride {
                        ContainerDefinition = addSequencesToQueueContainer,
                        Environment = new TaskEnvironmentVariable[] {
                            new TaskEnvironmentVariable{
                                Name = "EXECUTION_MODE",
                                Value = JsonPath.StringAt("$.executionMode")
                             },
                            new TaskEnvironmentVariable{
                                Name = "HERON_SEQUENCES_TABLE",
                                Value = sequencesTable.TableName
                            },
                            new TaskEnvironmentVariable{
                                Name = "HERON_PROCESSING_QUEUE",
                                Value = reprocessingQueue.QueueName
                            },
                            new TaskEnvironmentVariable{
                              Name = "HERON_DAILY_PROCESSING_QUEUE",
                              Value = dailyProcessingQueue.QueueName
                            }
                        }
                    }
                },
                ResultPath = JsonPath.DISCARD
            });

            // +++++++++++++++++++++++++++++++++++++++++++++
            // +++++++++++++++++++++++++++++++++++++++++++++
            // ++++++++++++ Lambda Functions +++++++++++++++
            // +++++++++++++++++++++++++++++++++++++++++++++
            // +++++++++++++++++++++++++++++++++++++++++++++
            var fileSystemConfig = new FileSystemConfig();
            fileSystemConfig.Arn = pipelineEFSAccessPoint.AccessPointArn;
            fileSystemConfig.LocalMountPath = "/mnt/efs0";

            var s3CrudpPolicyStatement = new PolicyStatement(new PolicyStatementProps
            {
                Effect = Effect.ALLOW,
                Actions = new string[] { "sts:AssumeRole" },
                Principals = new ServicePrincipal[] { new ServicePrincipal("ecs-tasks.amazonaws.com") }
            });

            var lambdaPipelineFileSystem = new Amazon.CDK.AWS.Lambda.FileSystem(fileSystemConfig);

            var createRunBaseConfigFunction = new PythonFunction(this, "createRunBaseConfigFunction", new PythonFunctionProps{
                Entry = "src/functions/createRunBaseConfig",
                Runtime = Runtime.PYTHON_3_7,
                Index = "app.py",
                Handler = "lambda_handler",
            });
            var lqpCreateRunBaseConfigTask = new LambdaInvoke(this, "lqpCreateRunBaseConfig", new LambdaInvokeProps{
                LambdaFunction = createRunBaseConfigFunction,
                ResultPath = "$.runbaseConfig",
                PayloadResponseOnly = true
            });
            var checkLqpMetaDataIsPresentFunction = new PythonFunction(this, "checkLqpMetaDataIsPresentFunction", new PythonFunctionProps {
                Entry = "src/functions/checkLqpMetaDataIsPresent",
                Runtime = Runtime.PYTHON_3_7,
                Index = "app.py",
                Handler = "lambda_handler",
                Environment = new Dictionary<string, string> {
                  {"LQP_DATA_ROOT","/mnt/efs0/lqpModel/metaData"},
                  {"HERON_SAMPLES_BUCKET", pipelineBucket.BucketName}
                },
                Filesystem = lambdaPipelineFileSystem,
                Vpc = vpc
            });

            checkLqpMetaDataIsPresentFunction.Node.AddDependency(pipelineBucket);

            var checkLqpMetaDataIsPresentTask = new LambdaInvoke(this, "checkLqpMetaDataIsPresentTask", new LambdaInvokeProps {
                LambdaFunction = checkLqpMetaDataIsPresentFunction,
                ResultPath = "$.metaDataPresent",
                PayloadResponseOnly = true
            });

            // +++++++++++++++++++++++++++++++++++++++++++++
            // +++++++++++++++++++++++++++++++++++++++++++++
            // +++++++++++++ State Machines ++++++++++++++++
            // +++++++++++++++++++++++++++++++++++++++++++++
            // +++++++++++++++++++++++++++++++++++++++++++++

            // +++++++++++++++++++++++++++++++++++++++++++++
            // ++++ LQP Prepare MetaData RunBase Nested ++++
            // +++++++++++++++++++++++++++++++++++++++++++++
            var runBaseParameters = new Dictionary<string, object>();
            runBaseParameters.Add("date.$", "$.date");
            runBaseParameters.Add("partition.$", "$$.Map.Item.Value.partition");
            var lqpRunBaseMapState = new Map(this, "lqpRunBaseMapState", new MapProps{
                InputPath = "$",
                ItemsPath = "$.partitions",
                ResultPath = JsonPath.DISCARD,
                Parameters = runBaseParameters
                
            });
            lqpRunBaseMapState.Iterator(Chain.Start(lqpRunBaseTask));

            var runBaseChain = Chain
              .Start(lqpRunBaseMapState);
            
            var lqpPrepareMetaDataRunbaseStateMachine = new StateMachine(this, "lqpPrepMetaDataRunBaseStateMachine", new StateMachineProps{
                Definition = runBaseChain
            });


            var runLqpMetaDataRunBaseStateMachineTask = new StepFunctionsStartExecution(this, "runLqpMetaDataRunBaseStateMachineTask", new StepFunctionsStartExecutionProps 
            {
              IntegrationPattern = IntegrationPattern.RUN_JOB,
              StateMachine = lqpPrepareMetaDataRunbaseStateMachine,
              InputPath = "$",
              ResultPath = JsonPath.DISCARD
            });

            var parameters = new Dictionary<string, object>();
            parameters.Add("date.$", "$.date");
            parameters.Add("partitions.$", "$$.Map.Item.Value.partitions");

            var lqpPrepareMetaDataRunBaseMap = new Map(this, "lqpPrepareMetaDataRunBaseMap", new MapProps {
              InputPath = "$",
              ItemsPath = "$.runbaseConfig.batches",
              ResultPath = JsonPath.DISCARD,
              Parameters = parameters,
              
            });

            lqpPrepareMetaDataRunBaseMap.Iterator(Chain.Start(runLqpMetaDataRunBaseStateMachineTask));

            
            // +++++++++++++++++++++++++++++++++++++++++++++
            // ++++ LQP Prepare MetaData Step Function +++++
            // +++++++++++++++++++++++++++++++++++++++++++++
            var lqpPrepareMetaDataChain = Chain
                .Start(lqpPrepareMetaDataTask)
                .Next(lqpCreateRunBaseConfigTask)
                .Next(lqpPrepareMetaDataRunBaseMap)
                .Next(lqpTidyTask);


            var lqpPrepareMetaDataStateMachine = new StateMachine(this, "lqpPrepMetaDataStateMachine", new StateMachineProps{
                Definition = lqpPrepareMetaDataChain
            });


            // +++++++++++++++++++++++++++++++++++++++++++++
            // +++++++ Heron Pipeline State Machine ++++++++
            // +++++++++++++++++++++++++++++++++++++++++++++
            var pipelineFinishTask = new Succeed(this, "pipelineSucceedTask");
            var metaDataPresentCondition = Condition.BooleanEquals(JsonPath.StringAt("$.checkForMetaData.metaDataReady"), true);
            var metaDataNotPresentCondition = Condition.BooleanEquals(JsonPath.StringAt("$.checkForMetaData.metaDataReady"), false);

            var metaDataReadyChoiceTask = new Choice(this, "metaDataReadyChoiceTask", new ChoiceProps{
                Comment = "Is LQP metadata available?"
            });
            metaDataReadyChoiceTask.When(metaDataPresentCondition, next: addSequencesToQueueTask);
            metaDataReadyChoiceTask.When(metaDataNotPresentCondition, next: pipelineFinishTask);

            var pipelineChain = Chain
                    .Start(checkLqpMetaDataIsPresentTask)
                    .Next(metaDataReadyChoiceTask);

            var pipelineStateMachine = new StateMachine(this, "pipelineStateMachine", new StateMachineProps
            {
                Definition = pipelineChain
            });
        }
    }
}
