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
        // public Amazon.CDK.AWS.ECS.Volume volume;
        public Cluster cluster;
        // public Bucket pipelineBucket;
        // public Table sequencesTable;
        //Amazon.CDK.AWS.ECS.Volume volume, Cluster cluster, Bucket bucket, Table sequencesTable

        internal HeronPipelineStack(Construct scope, string id, IStackProps props = null) : base(scope, id, props)
        {

            var testObj = new TestClass(this, "testClass");
            var infrastructure = new Infrastructure(this, "infrastructure");
            infrastructure.Create();
            //++++++++++++++++++++++++++++++++++++++++++
            // VPC
            //++++++++++++++++++++++++++++++++++++++++++
            // var vpc = new Vpc(this, "vpc", new VpcProps{
            //     MaxAzs = 3, ///TODO: Increase this once EIP's are freed
            //     Cidr = "11.0.0.0/16",
            // });

            // var secGroup = new SecurityGroup(this, "vpcSecurityGroup", new SecurityGroupProps{
            //     Vpc = vpc,
            //     AllowAllOutbound = true
            // });
            // secGroup.AddIngressRule(Peer.AnyIpv4(), Port.AllIcmp(), "All Incoming");
            // secGroup.AddIngressRule(Peer.AnyIpv4(), Port.Tcp(2049), "EFS Port");
            // secGroup.AddIngressRule(Peer.AnyIpv4(), Port.AllTraffic(), "All Traffic");
            // secGroup.Node.AddDependency(vpc);

            //++++++++++++++++++++++++++++++++++++++++++
            // File System (EFS)
            //++++++++++++++++++++++++++++++++++++++++++
            // var pipelineEFS = new Amazon.CDK.AWS.EFS.FileSystem(this, "pipelineEFS", new FileSystemProps{
            //     Vpc = vpc,
            //     ThroughputMode = ThroughputMode.PROVISIONED,
            //     ProvisionedThroughputPerSecond = Size.Mebibytes(30),
            //     PerformanceMode = PerformanceMode.GENERAL_PURPOSE,
            //     RemovalPolicy = RemovalPolicy.DESTROY,
            //     Encrypted = false,
            //     SecurityGroup = secGroup
            // });

            // var pipelineEFSAccessPoint = new AccessPoint(this, "pipelineEFSAccessPoint", new AccessPointProps{
            //     FileSystem = pipelineEFS,
            //     PosixUser = new PosixUser { Gid = "1000", Uid = "1000" },
            //     CreateAcl = new Acl { OwnerUid = "1000", OwnerGid = "1000", Permissions = "0777" },
            //     Path = "/efs"
            // });
            // pipelineEFSAccessPoint.Node.AddDependency(pipelineEFS);

            // volume = new Amazon.CDK.AWS.ECS.Volume();
            // volume.EfsVolumeConfiguration = new EfsVolumeConfiguration{
            //     FileSystemId = pipelineEFS.FileSystemId,
            //     AuthorizationConfig = new AuthorizationConfig{
            //         AccessPointId = pipelineEFSAccessPoint.AccessPointId,
            //         Iam = "ENABLED"
            //     },
            //     TransitEncryption = "ENABLED"
            // };
            // volume.Name = "efsVolume";


            //++++++++++++++++++++++++++++++++++++++++++
            //+++++++++++++++ Storage ++++++++++++++++++
            //++++++++++++++++++++++++++++++++++++++++++
            // pipelineBucket = new Bucket(this, "dataBucket", new BucketProps{
            //     Versioned = true,
            //     RemovalPolicy = RemovalPolicy.DESTROY,
            //     AutoDeleteObjects = true
            // });


            // var samplesTable = new Table(this, "heronSamplesTable", new TableProps{
            //     BillingMode = BillingMode.PAY_PER_REQUEST,
            //     PartitionKey = new Attribute { Name = "cogUkId", Type = AttributeType.STRING},
            //     SortKey = new Attribute { Name = "runCompleteDate", Type = AttributeType.NUMBER},
            //     PointInTimeRecovery = true
            // });

            // samplesTable.AddGlobalSecondaryIndex(new GlobalSecondaryIndexProps {
            //     IndexName = "lastChangedDate",
            //     PartitionKey = new Attribute { Name = "cogUkId", Type = AttributeType.STRING},
            //     SortKey = new Attribute { Name = "lastChangedDate", Type = AttributeType.NUMBER},
            //     ProjectionType = ProjectionType.ALL
            // });

            // sequencesTable = new Table(this, "heronSequencesTable", new TableProps {
            //     BillingMode = BillingMode.PAY_PER_REQUEST,
            //     PartitionKey = new Attribute { Name = "seqHash", Type = AttributeType.STRING},
            //     PointInTimeRecovery = true
            // });

            //++++++++++++++++++++++++++++++++++++++++++
            //SQS Queues
            //++++++++++++++++++++++++++++++++++++++++++
            // var dailyProcessingQueue = new Queue(this, "dailyProcessingQueue", new QueueProps {
            //     ContentBasedDeduplication = true,
            //     Fifo = true,
            //     FifoThroughputLimit = FifoThroughputLimit.PER_MESSAGE_GROUP_ID,
            //     DeduplicationScope = DeduplicationScope.MESSAGE_GROUP
            // });

            // var reprocessingQueue = new Queue(this, "reprocessingQueue", new QueueProps {
            //     ContentBasedDeduplication = true,
            //     Fifo = true,
            //     FifoThroughputLimit = FifoThroughputLimit.PER_MESSAGE_GROUP_ID,
            //     DeduplicationScope = DeduplicationScope.MESSAGE_GROUP
            // });


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
                Vpc = infrastructure.vpc,
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
            
            var fileSystemConfig = new FileSystemConfig();
            fileSystemConfig.Arn = infrastructure.pipelineEFSAccessPoint.AccessPointArn;
            fileSystemConfig.LocalMountPath = "/mnt/efs0";

            var s3AccessPolicyStatement = new PolicyStatement(new PolicyStatementProps
            {
                Effect = Effect.ALLOW,
                Actions = new string[] { "s3:*" }
            });
            s3AccessPolicyStatement.AddResources(new string[] {
              infrastructure.bucket.BucketArn,
              infrastructure.bucket.BucketArn + "/*"
            });

            var sqsAccessPolicyStatement = new PolicyStatement( new PolicyStatementProps {
              Effect = Effect.ALLOW,
              Actions = new string[] { "sqs:*"},
            });
            sqsAccessPolicyStatement.AddResources(new string[] {
              infrastructure.dailyProcessingQueue.QueueArn,
              infrastructure.reprocessingQueue.QueueArn
            });

            var dynamoDBAccessPolicyStatement = new PolicyStatement(new PolicyStatementProps{
              Effect = Effect.ALLOW,
              Actions = new string[] {"dynamodb:*"}
            });
            dynamoDBAccessPolicyStatement.AddResources(new string[]{
              infrastructure.samplesTable.TableArn,
              infrastructure.sequencesTable.TableArn
            });
            

            var lambdaPipelineFileSystem = new Amazon.CDK.AWS.Lambda.FileSystem(fileSystemConfig);

            // +++++++++++++++++++++++++++++++++++++++++++++
            // +++++++++++++++++++++++++++++++++++++++++++++
            // +++++++++++++ State Machines ++++++++++++++++
            // +++++++++++++++++++++++++++++++++++++++++++++
            // +++++++++++++++++++++++++++++++++++++++++++++

        
            

            // +++++++++++++++++++++++++++++++++++++++++++
            // ++ Classes to create pipeline components ++
            // +++++++++++++++++++++++++++++++++++++++++++

            var helperFunctions = new HelperFunctions(this,
                                                        "helperFunctions",
                                                        this.ecsExecutionRole,
                                                        infrastructure.volume,
                                                        this.cluster,
                                                        infrastructure.bucket,
                                                        infrastructure.sequencesTable,
                                                        infrastructure.reprocessingQueue,
                                                        infrastructure.dailyProcessingQueue,
                                                        sqsAccessPolicyStatement,
                                                        s3AccessPolicyStatement,
                                                        dynamoDBAccessPolicyStatement);
            helperFunctions.Create();

            var prepareSequences = new PrepareSequences(this,
                                                        "prepareSequences",
                                                        this.ecsExecutionRole,
                                                        infrastructure.volume,
                                                        this.cluster,
                                                        infrastructure.bucket,
                                                        infrastructure.sequencesTable,
                                                        infrastructure.reprocessingQueue,
                                                        infrastructure.dailyProcessingQueue);
            prepareSequences.CreateAddSequencesToQueue();
            prepareSequences.CreatePrepareSequences();
            prepareSequences.CreatePrepareConsenusSequences();

            var goFastaAlignment = new GoFastaAlignment(this,
                                                        "goFastaAlignment",
                                                        this.ecsExecutionRole,
                                                        infrastructure.volume,
                                                        this.cluster,
                                                        infrastructure.bucket,
                                                        infrastructure.sequencesTable);
            goFastaAlignment.Create();
            goFastaAlignment.CreateTestTask();

            var pangolinModel = new PangolinModel(  this,
                                                    "pangolinTaskDefinition",
                                                    this.ecsExecutionRole,
                                                    infrastructure.volume,
                                                    this.cluster,
                                                    infrastructure.bucket,
                                                    infrastructure.sequencesTable);
            pangolinModel.Create();

            var armadillinModel = new ArmadillinModel(  this,
                                                        "armadillinTaskDefinition",
                                                        this.ecsExecutionRole,
                                                        infrastructure.volume,
                                                        this.cluster,
                                                        infrastructure.bucket,
                                                        infrastructure.sequencesTable);
            armadillinModel.Create();
            armadillinModel.CreateTestTask();

            var genotypeVariantsModel = new GenotypeVariantsModel(  this,
                                                                    "genotypeVariantsTaskDefinition",
                                                                    this.ecsExecutionRole,
                                                                    infrastructure.volume,
                                                                    this.cluster,
                                                                    infrastructure.bucket,
                                                                    infrastructure.sequencesTable);
            genotypeVariantsModel.Create();
          
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
                .Start(pangolinModel.pangolinTask);

            var armadillinChain = Chain
                .Start(armadillinModel.armadillinTask);

            var genotypeVariantsChain = Chain
                .Start(genotypeVariantsModel.genotypeVariantsTask);

            placeSequencesParallel.Branch(new Chain[] { armadillinChain, pangolinChain, genotypeVariantsChain });

            var processSamplesChain = Chain
              .Start(prepareSequences.prepareConsensusSequencesTask)
            //   .Next(alignFastaTask)
              .Next(goFastaAlignment.goFastaAlignTask)
              .Next(prepareSequences.prepareSequencesTask)
              .Next(placeSequencesParallel);

            messagesAvailableChoiceTask.When(messagesAvailableCondition, processSamplesChain);
            messagesAvailableChoiceTask.When(messagesNotAvailableCondition, processSamplesFinishTask);

            var processSampleBatchChain = Chain
              .Start(helperFunctions.readSampleBatchCountTask)
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
                {"bucketName", infrastructure.bucket.BucketName}
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
                              Value = infrastructure.bucket.BucketName
                            },
                            new TaskEnvironmentVariable{
                                Name = "HERON_SEQUENCES_TABLE",
                                Value = infrastructure.sequencesTable.TableName
                            },
                            new TaskEnvironmentVariable{
                                Name = "HERON_SAMPLES_TABLE",
                                Value = infrastructure.samplesTable.TableName
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
              .Start(prepareSequences.addSequencesToQueueTask)
              .Next(helperFunctions.getMessageCountTask)
              .Next(launchSampleProcessingMap)
              .Next(exportResultsTask)
              .Next(pipelineFinishTask);

            var pipelineChain = Chain
                    .Start(processMessagesChain);

            var pipelineStateMachine = new StateMachine(this, "pipelineStateMachine", new StateMachineProps
            {
                Definition = pipelineChain
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