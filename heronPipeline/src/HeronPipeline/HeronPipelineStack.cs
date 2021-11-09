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
// using HeronPipeline.Go

namespace HeronPipeline
{
    public class HeronPipelineStack : Stack
    {
        public Role ecsExecutionRole;
        public Amazon.CDK.AWS.ECS.Volume volume;
        public Cluster cluster;
        public Bucket pipelineBucket;
        public Table sequencesTable;
        //Amazon.CDK.AWS.ECS.Volume volume, Cluster cluster, Bucket bucket, Table sequencesTable

        internal HeronPipelineStack(Construct scope, string id, IStackProps props = null) : base(scope, id, props)
        {

            var testObj = new TestClass(this, "testClass");
            //++++++++++++++++++++++++++++++++++++++++++
            // VPC
            //++++++++++++++++++++++++++++++++++++++++++
            var vpc = new Vpc(this, "vpc", new VpcProps{
                MaxAzs = 3, ///TODO: Increase this once EIP's are freed
                Cidr = "11.0.0.0/16",
                // NatGateways = 1,
                // SubnetConfiguration = new[]{
                //     new SubnetConfiguration {
                //         CidrMask = 24,
                //         Name = "ingress",
                //         SubnetType = SubnetType.PUBLIC
                //     },
                //     new SubnetConfiguration {
                //         CidrMask = 24,
                //         Name = "application",
                //         SubnetType = SubnetType.PRIVATE
                //     }},
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
                ProvisionedThroughputPerSecond = Size.Mebibytes(30),
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

            volume = new Amazon.CDK.AWS.ECS.Volume();
            volume.EfsVolumeConfiguration = new EfsVolumeConfiguration{
                FileSystemId = pipelineEFS.FileSystemId,
                AuthorizationConfig = new AuthorizationConfig{
                    AccessPointId = pipelineEFSAccessPoint.AccessPointId,
                    Iam = "ENABLED"
                },
                TransitEncryption = "ENABLED"
            };
            volume.Name = "efsVolume";


            //++++++++++++++++++++++++++++++++++++++++++
            //+++++++++++++++ Storage ++++++++++++++++++
            //++++++++++++++++++++++++++++++++++++++++++
            pipelineBucket = new Bucket(this, "dataBucket", new BucketProps{
                Versioned = true,
                RemovalPolicy = RemovalPolicy.DESTROY,
                AutoDeleteObjects = true
            });


            var samplesTable = new Table(this, "heronSamplesTable", new TableProps{
                BillingMode = BillingMode.PAY_PER_REQUEST,
                PartitionKey = new Attribute { Name = "cogUkId", Type = AttributeType.STRING},
                SortKey = new Attribute { Name = "runCompleteDate", Type = AttributeType.NUMBER},
                PointInTimeRecovery = true
            });

            samplesTable.AddGlobalSecondaryIndex(new GlobalSecondaryIndexProps {
                IndexName = "lastChangedDate",
                PartitionKey = new Attribute { Name = "cogUkId", Type = AttributeType.STRING},
                SortKey = new Attribute { Name = "lastChangedDate", Type = AttributeType.NUMBER},
                ProjectionType = ProjectionType.ALL
            });

            sequencesTable = new Table(this, "heronSequencesTable", new TableProps {
                BillingMode = BillingMode.PAY_PER_REQUEST,
                PartitionKey = new Attribute { Name = "seqHash", Type = AttributeType.STRING},
                PointInTimeRecovery = true
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
            ecsExecutionRole = new Role(this, "fargateExecutionRole", new RoleProps{
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

            cluster = new Cluster(this, "heronCluster", new ClusterProps{
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
              Interval = Duration.Seconds(2),
              MaxAttempts = 5,
              Errors = new string[] {"States.ALL"}
            };
            
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
                })
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
                                Value = reprocessingQueue.QueueUrl
                            },
                            new TaskEnvironmentVariable{
                              Name = "HERON_DAILY_PROCESSING_QUEUE",
                              Value = dailyProcessingQueue.QueueUrl
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

            var s3AccessPolicyStatement = new PolicyStatement(new PolicyStatementProps
            {
                Effect = Effect.ALLOW,
                Actions = new string[] { "s3:*" }
            });
            s3AccessPolicyStatement.AddResources(new string[] {
              pipelineBucket.BucketArn,
              pipelineBucket.BucketArn + "/*"
            });

            var sqsAccessPolicyStatement = new PolicyStatement( new PolicyStatementProps {
              Effect = Effect.ALLOW,
              Actions = new string[] { "sqs:*"},
            });
            sqsAccessPolicyStatement.AddResources(new string[] {
              dailyProcessingQueue.QueueArn,
              reprocessingQueue.QueueArn
            });

            var dynamoDBAccessPolicyStatement = new PolicyStatement(new PolicyStatementProps{
              Effect = Effect.ALLOW,
              Actions = new string[] {"dynamodb:*"}
            });
            dynamoDBAccessPolicyStatement.AddResources(new string[]{
              samplesTable.TableArn,
              sequencesTable.TableArn
            });
            

            var lambdaPipelineFileSystem = new Amazon.CDK.AWS.Lambda.FileSystem(fileSystemConfig);

            // Mark: getMessageCount
            var getMessageCountFunction = new PythonFunction(this, "getMessageCountFunction", new PythonFunctionProps{
                Entry = "src/functions/getMessageCount",
                Runtime = Runtime.PYTHON_3_7,
                Index = "app.py",
                Handler = "lambda_handler",
                Environment = new Dictionary<string, string> {
                    {"EXECUTION_MODE",JsonPath.StringAt("$.executionMode")},
                    {"HERON_SEQUENCES_TABLE",sequencesTable.TableName},
                    {"HERON_PROCESSING_QUEUE", reprocessingQueue.QueueUrl},
                    {"HERON_DAILY_PROCESSING_QUEUE",dailyProcessingQueue.QueueUrl}
                }
            });
            getMessageCountFunction.AddToRolePolicy(sqsAccessPolicyStatement);

            var getMessageCountTask = new LambdaInvoke(this, "getMessageCountTask", new LambdaInvokeProps{
                LambdaFunction = getMessageCountFunction,
                ResultPath = "$.messageCount",
                PayloadResponseOnly = true
            });
            // +++++++++++++++++++++++++++++++++++++++++++++
            // +++++++++++++++++++++++++++++++++++++++++++++
            // +++++++++++++ State Machines ++++++++++++++++
            // +++++++++++++++++++++++++++++++++++++++++++++
            // +++++++++++++++++++++++++++++++++++++++++++++

        
            // +++++++++++++++++++++++++++++++++++++++++++++
            // +++++++++++++++++++++++++++++++++++++++++++++
            // +++++++++++++++++++++++++++++++++++++++++++++
            // ++++ Process Sample Batch State Machine +++++
            // +++++++++++++++++++++++++++++++++++++++++++++
            // +++++++++++++++++++++++++++++++++++++++++++++
            // +++++++++++++++++++++++++++++++++++++++++++++
            var readSampleBatchFunction = new PythonFunction(this, "readSampleBatchFunction", new PythonFunctionProps{
              Entry = "src/functions/readSampleBatchFromQueue",
              Runtime = Runtime.PYTHON_3_7,
              Index = "app.py",
              Handler = "lambda_handler",
              Timeout = Duration.Seconds(900)
            });
            readSampleBatchFunction.AddToRolePolicy(s3AccessPolicyStatement);
            readSampleBatchFunction.AddToRolePolicy(sqsAccessPolicyStatement);
            readSampleBatchFunction.AddToRolePolicy(dynamoDBAccessPolicyStatement);

            var readSampleBatchCountTask = new LambdaInvoke(this, "readSampleBatchCountTask", new LambdaInvokeProps{
              LambdaFunction = readSampleBatchFunction,
              ResultPath = "$.sampleBatch",
              PayloadResponseOnly = true
            });
            readSampleBatchCountTask.AddRetry(retryItem);

            // +++++++++++++++++++++++++++++++++++++++++++
            // +++++++++++++++++++++++++++++++++++++++++++
            var alignFastaImage = ContainerImage.FromAsset("src/images/alignFasta");
            var alignFastaTaskDefinition = new TaskDefinition(this, "alignFastaTaskDefinition", new TaskDefinitionProps{
                Family = "alignFasta",
                Cpu = "1024",
                MemoryMiB = "4096",
                NetworkMode = NetworkMode.AWS_VPC,
                Compatibility = Compatibility.FARGATE,
                ExecutionRole = ecsExecutionRole,
                TaskRole = ecsExecutionRole,
                Volumes = new Amazon.CDK.AWS.ECS.Volume[] { volume }
            });
            alignFastaTaskDefinition.AddContainer("alignFastaContainer", new Amazon.CDK.AWS.ECS.ContainerDefinitionOptions
            {
                Image = alignFastaImage,
                Logging = new AwsLogDriver(new AwsLogDriverProps
                {
                    StreamPrefix = "alignFasta",
                    LogGroup = new LogGroup(this, "alignFastaLogGroup", new LogGroupProps
                    {
                        LogGroupName = "alignFastaLogGroup",
                        Retention = RetentionDays.ONE_WEEK,
                        RemovalPolicy = RemovalPolicy.DESTROY
                    })
                })
            });
            var alignFastaContainer = alignFastaTaskDefinition.FindContainer("alignFastaContainer");
            alignFastaContainer.AddMountPoints(new MountPoint[] {
                    new MountPoint {
                        SourceVolume = "efsVolume",
                        ContainerPath = "/mnt/efs0",
                        ReadOnly = false,
                    }
                });
            
            var alignFastaTask = new EcsRunTask(this, "alignFastaTask", new EcsRunTaskProps
            {
                IntegrationPattern = IntegrationPattern.RUN_JOB,
                Cluster = cluster,
                TaskDefinition = alignFastaTaskDefinition,
                AssignPublicIp = true,
                LaunchTarget = new EcsFargateLaunchTarget(),
                ContainerOverrides = new ContainerOverride[] {
                    new ContainerOverride {
                        ContainerDefinition = alignFastaContainer,
                        Environment = new TaskEnvironmentVariable[] {
                            new TaskEnvironmentVariable{
                              Name = "ITERATION_UUID",
                              Value = JsonPath.StringAt("$.sampleBatch.iterationUUID")
                            },
                            new TaskEnvironmentVariable{
                              Name = "SEQ_DATA_ROOT",
                              Value = "/mnt/efs0/seqData"
                            },
                            new TaskEnvironmentVariable{
                              Name = "DATE_PARTITION",
                              Value = JsonPath.StringAt("$.date")   
                            },
                            new TaskEnvironmentVariable{
                              Name = "HERON_SAMPLES_BUCKET",
                              Value = pipelineBucket.BucketName
                            },
                            new TaskEnvironmentVariable{
                                Name = "HERON_SEQUENCES_TABLE",
                                Value = sequencesTable.TableName
                            },
                            new TaskEnvironmentVariable{
                                Name = "MESSAGE_LIST_S3_KEY",
                                Value = JsonPath.StringAt("$.sampleBatch.messageListS3Key")
                            },
                            new TaskEnvironmentVariable{
                                Name = "REF_FASTA_KEY",
                                Value = "resources/MN908947.fa"
                            },
                            new TaskEnvironmentVariable{
                                Name = "TRIM_START",
                                Value = "265"
                            },
                            new TaskEnvironmentVariable{
                                Name = "TRIM_END",
                                Value = "29674"
                            }
                        }
                    }
                },
                ResultPath = JsonPath.DISCARD
            });
            alignFastaTask.AddRetry(retryItem);
            var alignFastaTestTask = new EcsRunTask(this, "alignFastaTestTask", new EcsRunTaskProps
            {
                IntegrationPattern = IntegrationPattern.RUN_JOB,
                Cluster = cluster,
                TaskDefinition = alignFastaTaskDefinition,
                AssignPublicIp = true,
                LaunchTarget = new EcsFargateLaunchTarget(),
                ContainerOverrides = new ContainerOverride[] {
                    new ContainerOverride {
                        ContainerDefinition = alignFastaContainer,
                        Environment = new TaskEnvironmentVariable[] {
                            new TaskEnvironmentVariable{
                              Name = "ITERATION_UUID",
                              Value = "13c7376f-825b-4952-93eb-8e02af37efd4"
                            },
                            new TaskEnvironmentVariable{
                              Name = "SEQ_DATA_ROOT",
                              Value = "/mnt/efs0/seqData"
                            },
                            new TaskEnvironmentVariable{
                              Name = "DATE_PARTITION",
                              Value = "2021-10-25"
                            },
                            new TaskEnvironmentVariable{
                              Name = "HERON_SAMPLES_BUCKET",
                              Value = pipelineBucket.BucketName
                            },
                            new TaskEnvironmentVariable{
                                Name = "HERON_SEQUENCES_TABLE",
                                Value = sequencesTable.TableName
                            },
                            new TaskEnvironmentVariable{
                                Name = "MESSAGE_LIST_S3_KEY",
                                Value = "messageLists/2021-10-25/messageList13c7376f-825b-4952-93eb-8e02af37efd4.json"
                            },
                            new TaskEnvironmentVariable{
                                Name = "REF_FASTA_KEY",
                                Value = "resources/MN908947.fa"
                            },
                            new TaskEnvironmentVariable{
                                Name = "TRIM_START",
                                Value = "266" //Value = "265"
                            },
                            new TaskEnvironmentVariable{
                                Name = "TRIM_END",
                                Value = "29674"
                            }
                        }
                    }
                },
                ResultPath = JsonPath.DISCARD
            });

            // +++++++++++++++++++++++++++++++++++++++++++
            // +++++++++++++++++++++++++++++++++++++++++++
            var goFastaAlignment = new GoFastaAlignment(this,
                                                        "goFastaAlignment",
                                                        this.ecsExecutionRole,
                                                        this.volume,
                                                        this.cluster,
                                                        this.pipelineBucket,
                                                        this.sequencesTable);
            goFastaAlignment.Create();
            goFastaAlignment.CreateTestTask();
            // +++++++++++++++++++++++++++++++++++++++++++
            // +++++++++++++++++++++++++++++++++++++++++++

            var alignFastaFunction = new DockerImageFunction(this, "alignFastaFunction", new DockerImageFunctionProps{
              Code = DockerImageCode.FromImageAsset("src/functions/alignFastaFunction"),
              Timeout = Duration.Seconds(900),
              Environment = new Dictionary<string, string> {
                {"HERON_SAMPLES_BUCKET", pipelineBucket.BucketName},
                {"HERON_SAMPLES_TABLE", samplesTable.TableName},
                {"HERON_SEQUENCES_TABLE",sequencesTable.TableName},
                {"REF_FASTA_KEY", "resources/MN908947.fa"},
                {"TRIM_START", "265"},
                {"TRIM_END", "29674"},
              }
            });
            alignFastaFunction.AddToRolePolicy(s3AccessPolicyStatement);
            alignFastaFunction.AddToRolePolicy(sqsAccessPolicyStatement);
            alignFastaFunction.AddToRolePolicy(dynamoDBAccessPolicyStatement);

            var genotypeVariantsImage = ContainerImage.FromAsset("src/images/genotypeVariants");
            var genotypeVariantsTaskDefinition = new TaskDefinition(this, "genotypeVariantsTaskDefinition", new TaskDefinitionProps{
                Family = "genotypeVariants",
                Cpu = "1024",
                MemoryMiB = "2048",
                NetworkMode = NetworkMode.AWS_VPC,
                Compatibility = Compatibility.FARGATE,
                ExecutionRole = ecsExecutionRole,
                TaskRole = ecsExecutionRole,
                Volumes = new Amazon.CDK.AWS.ECS.Volume[] { volume }
            });
            genotypeVariantsTaskDefinition.AddContainer("genotypeVariantsContainer", new Amazon.CDK.AWS.ECS.ContainerDefinitionOptions
            {
                Image = genotypeVariantsImage,
                Logging = new AwsLogDriver(new AwsLogDriverProps
                {
                    StreamPrefix = "genotypeVariants",
                    LogGroup = new LogGroup(this, "genotypeVariantsLogGroup", new LogGroupProps
                    {
                        LogGroupName = "genotypeVariantsLogGroup",
                        Retention = RetentionDays.ONE_WEEK,
                        RemovalPolicy = RemovalPolicy.DESTROY
                    })
                })
            });
            var genotypeVariantsContainer = genotypeVariantsTaskDefinition.FindContainer("genotypeVariantsContainer");
            genotypeVariantsContainer.AddMountPoints(new MountPoint[] {
                    new MountPoint {
                        SourceVolume = "efsVolume",
                        ContainerPath = "/mnt/efs0",
                        ReadOnly = false,
                    }
                });
            var genotypeVariantsTask = new EcsRunTask(this, "genotypeVariantsPlaceTask", new EcsRunTaskProps
            {
                IntegrationPattern = IntegrationPattern.RUN_JOB,
                Cluster = cluster,
                TaskDefinition = genotypeVariantsTaskDefinition,
                AssignPublicIp = true,
                LaunchTarget = new EcsFargateLaunchTarget(),
                ContainerOverrides = new ContainerOverride[] {
                    new ContainerOverride {
                        ContainerDefinition = genotypeVariantsContainer,
                        Environment = new TaskEnvironmentVariable[] {
                            new TaskEnvironmentVariable{
                              Name = "DATE_PARTITION",
                              Value = JsonPath.StringAt("$.date")
                            },
                            new TaskEnvironmentVariable{
                              Name = "MESSAGE_LIST_S3_KEY",
                              Value = JsonPath.StringAt("$.sampleBatch.messageListS3Key")
                            },
                            new TaskEnvironmentVariable{
                              Name = "HERON_SAMPLES_BUCKET",
                              Value = pipelineBucket.BucketName
                            },
                            new TaskEnvironmentVariable{
                              Name = "SEQ_DATA_ROOT",
                              Value = "/mnt/efs0/seqData"
                            },
                            new TaskEnvironmentVariable{
                              Name = "ITERATION_UUID",
                              Value = JsonPath.StringAt("$.sampleBatch.iterationUUID")
                            },
                            new TaskEnvironmentVariable{
                                Name = "RECIPE_FILE_PATH",
                                Value = JsonPath.StringAt("$.recipeFilePath")
                            },
                            new TaskEnvironmentVariable{
                                Name = "HERON_SEQUENCES_TABLE",
                                Value = sequencesTable.TableName
                            }
                        }
                    }
                },
                ResultPath = JsonPath.DISCARD
            });
            genotypeVariantsTask.AddRetry(retryItem);

            var prepareSequencesImage = ContainerImage.FromAsset("src/images/prepareSequences", new AssetImageProps
            { 
            });
            var prepareSequencesTaskDefinition = new TaskDefinition(this, "prepareSequencesTaskDefinition", new TaskDefinitionProps{
                Family = "prepareSequences",
                Cpu = "1024",
                MemoryMiB = "4096",
                NetworkMode = NetworkMode.AWS_VPC,
                Compatibility = Compatibility.FARGATE,
                ExecutionRole = ecsExecutionRole,
                TaskRole = ecsExecutionRole,
                Volumes = new Amazon.CDK.AWS.ECS.Volume[] { volume }
            });

            prepareSequencesTaskDefinition.AddContainer("prepareSequencesContainer", new Amazon.CDK.AWS.ECS.ContainerDefinitionOptions
            {
                Image = prepareSequencesImage,
                Logging = new AwsLogDriver(new AwsLogDriverProps
                {
                    StreamPrefix = "prepareSequences",
                    LogGroup = new LogGroup(this, "prepareSequencesLogGroup", new LogGroupProps
                    {
                        LogGroupName = "prepareSequencesLogGroup",
                        Retention = RetentionDays.ONE_WEEK,
                        RemovalPolicy = RemovalPolicy.DESTROY
                    })
                })
            });
            var prepareSequencesContainer = prepareSequencesTaskDefinition.FindContainer("prepareSequencesContainer");
            prepareSequencesContainer.AddMountPoints(new MountPoint[] {
                    new MountPoint {
                        SourceVolume = "efsVolume",
                        ContainerPath = "/mnt/efs0",
                        ReadOnly = false,
                    }
                });

            var prepareSequencesTask = new EcsRunTask(this, "prepareSequencesTask", new EcsRunTaskProps
            {
                IntegrationPattern = IntegrationPattern.RUN_JOB,
                Cluster = cluster,
                TaskDefinition = prepareSequencesTaskDefinition,
                AssignPublicIp = true,
                LaunchTarget = new EcsFargateLaunchTarget(),
                ContainerOverrides = new ContainerOverride[] {
                    new ContainerOverride {
                        ContainerDefinition = prepareSequencesContainer,
                        Environment = new TaskEnvironmentVariable[] {
                            new TaskEnvironmentVariable{
                              Name = "DATE_PARTITION",
                              Value = JsonPath.StringAt("$.date")
                            },
                            new TaskEnvironmentVariable{
                              Name = "MESSAGE_LIST_S3_KEY",
                              Value = JsonPath.StringAt("$.sampleBatch.messageListS3Key")
                            },
                            new TaskEnvironmentVariable{
                              Name = "HERON_SAMPLES_BUCKET",
                              Value = pipelineBucket.BucketName
                            },
                            new TaskEnvironmentVariable{
                              Name = "SEQ_DATA_ROOT",
                              Value = "/mnt/efs0/seqData"
                            },
                            new TaskEnvironmentVariable{
                              Name = "ITERATION_UUID",
                              Value = JsonPath.StringAt("$.sampleBatch.iterationUUID")
                            }
                        }
                    }
                },
                ResultPath = JsonPath.DISCARD
            });
            prepareSequencesTask.AddRetry(retryItem);
            
            var prepareSequencesTestTask = new EcsRunTask(this, "prepareSequencesTestTask", new EcsRunTaskProps
            {
                IntegrationPattern = IntegrationPattern.RUN_JOB,
                Cluster = cluster,
                TaskDefinition = prepareSequencesTaskDefinition,
                AssignPublicIp = true,
                LaunchTarget = new EcsFargateLaunchTarget(),
                ContainerOverrides = new ContainerOverride[] {
                    new ContainerOverride {
                        ContainerDefinition = prepareSequencesContainer,
                        Environment = new TaskEnvironmentVariable[] {
                            new TaskEnvironmentVariable{
                              Name = "DATE_PARTITION",
                              Value = JsonPath.StringAt("$.date")
                            },
                            new TaskEnvironmentVariable{
                              Name = "MESSAGE_LIST_S3_KEY",
                              Value = JsonPath.StringAt("$.sampleBatch.messageListS3Key")
                            },
                            new TaskEnvironmentVariable{
                              Name = "HERON_SAMPLES_BUCKET",
                              Value = pipelineBucket.BucketName
                            },
                            new TaskEnvironmentVariable{
                              Name = "SEQ_DATA_ROOT",
                              Value = "/mnt/efs0/seqData"
                            },
                            new TaskEnvironmentVariable{
                              Name = "ITERATION_UUID",
                              Value = JsonPath.StringAt("$.sampleBatch.iterationUUID")
                            }
                        }
                    }
                },
                ResultPath = JsonPath.DISCARD
            });



            var prepareConsensusSequencesImage = ContainerImage.FromAsset("src/images/prepareConsensusSequences", new AssetImageProps
            { 
            });
            var prepareConsensusSequencesTaskDefinition = new TaskDefinition(this, "prepareConsensusSequencesTaskDefinition", new TaskDefinitionProps{
                Family = "prepareConsensusSequences",
                Cpu = "1024",
                MemoryMiB = "4096",
                NetworkMode = NetworkMode.AWS_VPC,
                Compatibility = Compatibility.FARGATE,
                ExecutionRole = ecsExecutionRole,
                TaskRole = ecsExecutionRole,
                Volumes = new Amazon.CDK.AWS.ECS.Volume[] { volume }
            });

            prepareConsensusSequencesTaskDefinition.AddContainer("prepareConsensusSequencesContainer", new Amazon.CDK.AWS.ECS.ContainerDefinitionOptions
            {
                Image = prepareConsensusSequencesImage,
                Logging = new AwsLogDriver(new AwsLogDriverProps
                {
                    StreamPrefix = "prepareConsensusSequences",
                    LogGroup = new LogGroup(this, "prepareConsensusSequencesLogGroup", new LogGroupProps
                    {
                        LogGroupName = "prepareConsensusSequencesLogGroup",
                        Retention = RetentionDays.ONE_WEEK,
                        RemovalPolicy = RemovalPolicy.DESTROY
                    })
                })
            });
            var prepareConsensusSequencesContainer = prepareConsensusSequencesTaskDefinition.FindContainer("prepareConsensusSequencesContainer");
            prepareConsensusSequencesContainer.AddMountPoints(new MountPoint[] {
                    new MountPoint {
                        SourceVolume = "efsVolume",
                        ContainerPath = "/mnt/efs0",
                        ReadOnly = false,
                    }
                });

            var prepareConsensusSequencesTask = new EcsRunTask(this, "prepareConsensusSequencesTask", new EcsRunTaskProps
            {
                IntegrationPattern = IntegrationPattern.RUN_JOB,
                Cluster = cluster,
                TaskDefinition = prepareConsensusSequencesTaskDefinition,
                AssignPublicIp = true,
                LaunchTarget = new EcsFargateLaunchTarget(),
                ContainerOverrides = new ContainerOverride[] {
                    new ContainerOverride {
                        ContainerDefinition = prepareConsensusSequencesContainer,
                        Environment = new TaskEnvironmentVariable[] {
                            new TaskEnvironmentVariable{
                              Name = "DATE_PARTITION",
                              Value = JsonPath.StringAt("$.date")
                            },
                            new TaskEnvironmentVariable{
                              Name = "MESSAGE_LIST_S3_KEY",
                              Value = JsonPath.StringAt("$.sampleBatch.messageListS3Key")
                            },
                            new TaskEnvironmentVariable{
                              Name = "HERON_SAMPLES_BUCKET",
                              Value = pipelineBucket.BucketName
                            },
                            new TaskEnvironmentVariable{
                              Name = "SEQ_DATA_ROOT",
                              Value = "/mnt/efs0/seqData"
                            },
                            new TaskEnvironmentVariable{
                              Name = "ITERATION_UUID",
                              Value = JsonPath.StringAt("$.sampleBatch.iterationUUID")
                            }
                        }
                    }
                },
                ResultPath = JsonPath.DISCARD
            });
            prepareConsensusSequencesTask.AddRetry(retryItem);

            // +++++++++++++++++++++++++++++++++++++++++++++++++++
            // +++++++++++++++++++++++++++++++++++++++++++++++++++
            var pangolinImage = ContainerImage.FromAsset("src/images/pangolin", new AssetImageProps
            { 
            });
            var pangolinTaskDefinition = new TaskDefinition(this, "pangolinTaskDefinition", new TaskDefinitionProps{
                Family = "pangolin",
                Cpu = "1024",
                MemoryMiB = "4096",
                NetworkMode = NetworkMode.AWS_VPC,
                Compatibility = Compatibility.FARGATE,
                ExecutionRole = ecsExecutionRole,
                TaskRole = ecsExecutionRole,
                Volumes = new Amazon.CDK.AWS.ECS.Volume[] { volume }
            });
            pangolinTaskDefinition.AddContainer("pangolinContainer", new Amazon.CDK.AWS.ECS.ContainerDefinitionOptions
            {
                Image = pangolinImage,
                Logging = new AwsLogDriver(new AwsLogDriverProps
                {
                    StreamPrefix = "pangolin",
                    LogGroup = new LogGroup(this, "pangolinLogGroup", new LogGroupProps
                    {
                        LogGroupName = "pangolinLogGroup",
                        Retention = RetentionDays.ONE_WEEK,
                        RemovalPolicy = RemovalPolicy.DESTROY
                    })
                })
            });
            var pangolinContainer = pangolinTaskDefinition.FindContainer("pangolinContainer");
            pangolinContainer.AddMountPoints(new MountPoint[] {
                    new MountPoint {
                        SourceVolume = "efsVolume",
                        ContainerPath = "/mnt/efs0",
                        ReadOnly = false,
                    }
                });
            var pangolinTask = new EcsRunTask(this, "pangolinPlaceTask", new EcsRunTaskProps
            {
                IntegrationPattern = IntegrationPattern.RUN_JOB,
                Cluster = cluster,
                TaskDefinition = pangolinTaskDefinition,
                AssignPublicIp = true,
                LaunchTarget = new EcsFargateLaunchTarget(),
                ContainerOverrides = new ContainerOverride[] {
                    new ContainerOverride {
                        ContainerDefinition = pangolinContainer,
                        Environment = new TaskEnvironmentVariable[] {
                            new TaskEnvironmentVariable{
                              Name = "DATE_PARTITION",
                              Value = JsonPath.StringAt("$.date")
                            },
                            new TaskEnvironmentVariable{
                              Name = "MESSAGE_LIST_S3_KEY",
                              Value = JsonPath.StringAt("$.sampleBatch.messageListS3Key")
                            },
                            new TaskEnvironmentVariable{
                              Name = "HERON_SAMPLES_BUCKET",
                              Value = pipelineBucket.BucketName
                            },
                            new TaskEnvironmentVariable{
                              Name = "SEQ_DATA_ROOT",
                              Value = "/mnt/efs0/seqData"
                            },
                            new TaskEnvironmentVariable{
                              Name = "ITERATION_UUID",
                              Value = JsonPath.StringAt("$.sampleBatch.iterationUUID")
                            },
                            new TaskEnvironmentVariable{
                                Name = "HERON_SEQUENCES_TABLE",
                                Value = sequencesTable.TableName
                            }
                        }
                    }
                },
                ResultPath = JsonPath.DISCARD
            });
            pangolinTask.AddRetry(retryItem);


            var armadillinImage = ContainerImage.FromAsset("src/images/armadillin", new AssetImageProps
            { 
            });
            var armadillinTaskDefinition = new TaskDefinition(this, "armadillinTaskDefinition", new TaskDefinitionProps{
                Family = "armadillin",
                Cpu = "1024",
                MemoryMiB = "4096",
                NetworkMode = NetworkMode.AWS_VPC,
                Compatibility = Compatibility.FARGATE,
                ExecutionRole = ecsExecutionRole,
                TaskRole = ecsExecutionRole,
                Volumes = new Amazon.CDK.AWS.ECS.Volume[] { volume }
            });
            armadillinTaskDefinition.AddContainer("armadillinContainer", new Amazon.CDK.AWS.ECS.ContainerDefinitionOptions
            {
                Image = armadillinImage,
                Logging = new AwsLogDriver(new AwsLogDriverProps
                {
                    StreamPrefix = "armadillin",
                    LogGroup = new LogGroup(this, "armadillinLogGroup", new LogGroupProps
                    {
                        LogGroupName = "armadillinLogGroup",
                        Retention = RetentionDays.ONE_WEEK,
                        RemovalPolicy = RemovalPolicy.DESTROY
                    })
                })
            });
            var armadillinContainer = armadillinTaskDefinition.FindContainer("armadillinContainer");
            armadillinContainer.AddMountPoints(new MountPoint[] {
                    new MountPoint {
                        SourceVolume = "efsVolume",
                        ContainerPath = "/mnt/efs0",
                        ReadOnly = false,
                    }
                });
            var armadillinTask = new EcsRunTask(this, "armadillinPlaceTask", new EcsRunTaskProps
            {
                IntegrationPattern = IntegrationPattern.RUN_JOB,
                Cluster = cluster,
                TaskDefinition = armadillinTaskDefinition,
                AssignPublicIp = true,
                LaunchTarget = new EcsFargateLaunchTarget(),
                ContainerOverrides = new ContainerOverride[] {
                    new ContainerOverride {
                        ContainerDefinition = armadillinContainer,
                        Environment = new TaskEnvironmentVariable[] {
                            new TaskEnvironmentVariable{
                              Name = "DATE_PARTITION",
                              Value = JsonPath.StringAt("$.date")
                            },
                            new TaskEnvironmentVariable{
                              Name = "MESSAGE_LIST_S3_KEY",
                              Value = JsonPath.StringAt("$.sampleBatch.messageListS3Key")
                            },
                            new TaskEnvironmentVariable{
                              Name = "HERON_SAMPLES_BUCKET",
                              Value = pipelineBucket.BucketName
                            },
                            new TaskEnvironmentVariable{
                              Name = "SEQ_DATA_ROOT",
                              Value = "/mnt/efs0/seqData"
                            },
                            new TaskEnvironmentVariable{
                              Name = "ITERATION_UUID",
                              Value = JsonPath.StringAt("$.sampleBatch.iterationUUID")
                            },
                            new TaskEnvironmentVariable{
                                Name = "HERON_SEQUENCES_TABLE",
                                Value = sequencesTable.TableName
                            }
                        }
                    }
                },
                ResultPath = JsonPath.DISCARD
            });

            var armadillinTestTask = new EcsRunTask(this, "armadillinPlaceTestTask", new EcsRunTaskProps
            {
                IntegrationPattern = IntegrationPattern.RUN_JOB,
                Cluster = cluster,
                TaskDefinition = armadillinTaskDefinition,
                AssignPublicIp = true,
                LaunchTarget = new EcsFargateLaunchTarget(),
                ContainerOverrides = new ContainerOverride[] {
                    new ContainerOverride {
                        ContainerDefinition = armadillinContainer,
                        Environment = new TaskEnvironmentVariable[] {
                            new TaskEnvironmentVariable{
                              Name = "DATE_PARTITION",
                              Value = JsonPath.StringAt("$.date")
                            },
                            new TaskEnvironmentVariable{
                              Name = "HERON_SAMPLES_BUCKET",
                              Value = pipelineBucket.BucketName
                            },
                            new TaskEnvironmentVariable{
                              Name = "SEQ_DATA_ROOT",
                              Value = "/mnt/efs0/seqData"
                            },
                            new TaskEnvironmentVariable{
                              Name = "ITERATION_UUID",
                              Value = JsonPath.StringAt("$.sampleBatch.iterationUUID")
                            },
                            new TaskEnvironmentVariable{
                                Name = "HERON_SEQUENCES_TABLE",
                                Value = sequencesTable.TableName
                            }
                        }
                    }
                },
                ResultPath = JsonPath.DISCARD
            });
            armadillinTask.AddRetry(retryItem);
            
            
            var processSamplesFinishTask = new Succeed(this, "processSamplesSucceedTask");
            var messagesAvailableChoiceTask = new Choice(this, "messagesAvailableChoiceTask", new ChoiceProps{
                Comment = "are there any messages in the sample batch"
            });
            
            var messagesAvailableCondition = Condition.NumberGreaterThan(JsonPath.StringAt("$.sampleBatch.messageCount"), 0);
            var messagesNotAvailableCondition = Condition.NumberEquals(JsonPath.StringAt("$.sampleBatch.messageCount"), 0);

            var placeSequencesParallel = new Parallel(this, "placeSequencesParallel", new ParallelProps{
              OutputPath = JsonPath.DISCARD
            });

            var pangolinChain = Chain
                .Start(pangolinTask);

            var armadillinChain = Chain
                .Start(armadillinTask);

            var armadillinTestChain = Chain
                .Start(alignFastaTestTask)
                .Next(prepareSequencesTestTask)
                .Next(armadillinTestTask);

            var genotypeVariantsChain = Chain
                .Start(genotypeVariantsTask);

            placeSequencesParallel.Branch(new Chain[] { armadillinChain, pangolinChain, genotypeVariantsChain });

            var processSamplesChain = Chain
              .Start(prepareConsensusSequencesTask)
            //   .Next(alignFastaTask)
              .Next(goFastaAlignment.goFastaAlignTask)
              .Next(prepareSequencesTask)
              .Next(placeSequencesParallel);

            messagesAvailableChoiceTask.When(messagesAvailableCondition, processSamplesChain);
            messagesAvailableChoiceTask.When(messagesNotAvailableCondition, processSamplesFinishTask);

            var processSampleBatchChain = Chain
              .Start(readSampleBatchCountTask)
              .Next(messagesAvailableChoiceTask);
            
            var processSampleBatchStateMachine = new StateMachine(this, "processSampleBatchStateMachine", new StateMachineProps{
              Definition = processSampleBatchChain
            //   Definition = processSamplesChain
            });


            // +++++++++++++++++++++++++++++++++++++++++++++
            // +++++++++++++++++++++++++++++++++++++++++++++
            // +++++++++++++++++++++++++++++++++++++++++++++
            // ++++ Start Nested Process Sample Batch ++++++
            // +++++++++++++++++++++++++++++++++++++++++++++
            // +++++++++++++++++++++++++++++++++++++++++++++
            // +++++++++++++++++++++++++++++++++++++++++++++
            var startSampleProcessingMapParameters = new Dictionary<string, object>();
            startSampleProcessingMapParameters.Add("date.$", "$.date");
            startSampleProcessingMapParameters.Add("queueName.$", "$.queueName");
            startSampleProcessingMapParameters.Add("recipeFilePath.$", "$.recipeFilePath");

            var startSampleProcessingMap = new Map(this, "startSampleProcessingMap", new MapProps {
              InputPath = "$",
              ItemsPath = "$.mapIterations",
              ResultPath = JsonPath.DISCARD,
              Parameters = startSampleProcessingMapParameters
            });

            var stateMachineInputObject2 = new Dictionary<string, object> {
                {"queueName", JsonPath.StringAt("$.queueName")},
                {"date", JsonPath.StringAt("$.date")},
                {"recipeFilePath", JsonPath.StringAt("$.recipeFilePath")},
                {"bucketName", pipelineBucket.BucketName}
            };

            var stateMachineInput2 = TaskInput.FromObject(stateMachineInputObject2);

            var startNestedProcessSamplesStateMachine = new StepFunctionsStartExecution(this, "startNestedProcessSamplesStateMachine", new StepFunctionsStartExecutionProps{
              StateMachine = processSampleBatchStateMachine,
              IntegrationPattern = IntegrationPattern.RUN_JOB,
              ResultPath = JsonPath.DISCARD,
              Input = stateMachineInput2
            });

            startSampleProcessingMap.Iterator(Chain.Start(startNestedProcessSamplesStateMachine));
            var startNestedSampleProcessingDefinition = Chain.Start(startSampleProcessingMap);

            var startNestedSampleProcessingStateMachine = new StateMachine(this, "startNestedSampleProcessingStateMachine", new StateMachineProps{
              Definition = startNestedSampleProcessingDefinition
            });

            // +++++++++++++++++++++++++++++++++++++++++++++
            // +++++++++++++++++++++++++++++++++++++++++++++
            // +++++++++++++++++++++++++++++++++++++++++++++
            // +++++++ Heron Pipeline State Machine ++++++++
            // +++++++++++++++++++++++++++++++++++++++++++++
            // +++++++++++++++++++++++++++++++++++++++++++++
            // +++++++++++++++++++++++++++++++++++++++++++++
            var pipelineFinishTask = new Succeed(this, "pipelineSucceedTask");

            // Input parameters to the map iteration state
            var launchSampleProcessingMapParameters = new Dictionary<string, object>();
            launchSampleProcessingMapParameters.Add("date.$", "$.date");
            launchSampleProcessingMapParameters.Add("queueName.$", "$.messageCount.queueName");
            launchSampleProcessingMapParameters.Add("recipeFilePath.$", "$.recipeFilePath");
            launchSampleProcessingMapParameters.Add("mapIterations.$", "$$.Map.Item.Value.process");

            var launchSampleProcessingMap = new Map(this, "launchSampleProcessingMap", new MapProps {
              InputPath = "$",
              ItemsPath = "$.messageCount.manageProcessSequencesBatchMapConfig",
              ResultPath = JsonPath.DISCARD,
              Parameters = launchSampleProcessingMapParameters
            //   MaxConcurrency = 10
            });

            var stateMachineInputObject = new Dictionary<string, object> {
                {"queueName", JsonPath.StringAt("$.queueName")},
                {"mapIterations", JsonPath.StringAt("$.mapIterations")},
                {"date", JsonPath.StringAt("$.date")},
                {"recipeFilePath", JsonPath.StringAt("$.recipeFilePath")}
            };
            var stateMachineInput = TaskInput.FromObject(stateMachineInputObject);
              

            var startNestedStateMachine = new StepFunctionsStartExecution(this, "startNestedStateMachine", new StepFunctionsStartExecutionProps{
              StateMachine = startNestedSampleProcessingStateMachine,
              IntegrationPattern = IntegrationPattern.RUN_JOB,
              ResultPath = JsonPath.DISCARD,
              Input = stateMachineInput
            });

            launchSampleProcessingMap.Iterator(Chain.Start(startNestedStateMachine));

            // Export results task
            var exportResultsImage = ContainerImage.FromAsset("src/images/exportResults");
            var exportResultsTaskDefinition = new TaskDefinition(this, "exportResultsTaskDefinition", new TaskDefinitionProps{
                Family = "exportResults",
                Cpu = "1024",
                MemoryMiB = "4096",
                NetworkMode = NetworkMode.AWS_VPC,
                Compatibility = Compatibility.FARGATE,
                ExecutionRole = ecsExecutionRole,
                TaskRole = ecsExecutionRole
            });
            exportResultsTaskDefinition.AddContainer("exportResultsContainer", new Amazon.CDK.AWS.ECS.ContainerDefinitionOptions
            {
                Image = exportResultsImage,
                Logging = new AwsLogDriver(new AwsLogDriverProps
                {
                    StreamPrefix = "exportResults",
                    LogGroup = new LogGroup(this, "exportResultsLogGroup", new LogGroupProps
                    {
                        LogGroupName = "exportResultsLogGroup",
                        Retention = RetentionDays.ONE_WEEK,
                        RemovalPolicy = RemovalPolicy.DESTROY
                    })
                })
            });
            var exportResultsContainer = exportResultsTaskDefinition.FindContainer("exportResultsContainer");
            var exportResultsTask = new EcsRunTask(this, "exportResultsTask", new EcsRunTaskProps
            {
                IntegrationPattern = IntegrationPattern.RUN_JOB,
                Cluster = cluster,
                TaskDefinition = exportResultsTaskDefinition,
                AssignPublicIp = true,
                LaunchTarget = new EcsFargateLaunchTarget(),
                ContainerOverrides = new ContainerOverride[] {
                    new ContainerOverride {
                        ContainerDefinition = exportResultsContainer,
                        Environment = new TaskEnvironmentVariable[] {
                            new TaskEnvironmentVariable{
                              Name = "DATE_PARTITION",
                              Value = JsonPath.StringAt("$.date")
                            },
                            new TaskEnvironmentVariable{
                              Name = "HERON_SAMPLES_BUCKET",
                              Value = pipelineBucket.BucketName
                            },
                            new TaskEnvironmentVariable{
                                Name = "HERON_SEQUENCES_TABLE",
                                Value = sequencesTable.TableName
                            },
                            new TaskEnvironmentVariable{
                                Name = "HERON_SAMPLES_TABLE",
                                Value = samplesTable.TableName
                            },
                            new TaskEnvironmentVariable{
                              Name = "EXECUTION_ID",
                              Value = JsonPath.StringAt("$$.Execution.Id")
                            }
                        }
                    }
                },
                ResultPath = "$.result"
            });

            var processMessagesChain = Chain
              .Start(addSequencesToQueueTask)
              .Next(getMessageCountTask)
              .Next(launchSampleProcessingMap)
              .Next(exportResultsTask)
              .Next(pipelineFinishTask);

            var pipelineChain = Chain
                    .Start(processMessagesChain);

            var pipelineStateMachine = new StateMachine(this, "pipelineStateMachine", new StateMachineProps
            {
                Definition = pipelineChain
            });


            var testArmadillianStateMachine = new StateMachine(this, "testArmadillianStateMachine", new StateMachineProps
            {
                Definition = armadillinTestChain
            });
        }
    }

    internal sealed class TestClass: Construct 
    {
        public TestClass(Construct scope, string id): base(scope, id)
        {

        }
    }
}