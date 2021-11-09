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
        internal HeronPipelineStack(Construct scope, string id, IStackProps props = null) : base(scope, id, props)
        {

            var testObj = new TestClass(this, "testClass");
            var infrastructure = new Infrastructure(this, "infrastructure");
            infrastructure.Create();
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
            // +++++++++++++++++++++++++++++++++++++++++++++
            // +++++++++++++ State Machines ++++++++++++++++
            // +++++++++++++++++++++++++++++++++++++++++++++
            // +++++++++++++++++++++++++++++++++++++++++++++

            // +++++++++++++++++++++++++++++++++++++++++++
            // ++ Classes to create pipeline components ++
            // +++++++++++++++++++++++++++++++++++++++++++

            var helperFunctions = new HelperFunctions(this,
                                                        "helperFunctions",
                                                        infrastructure.ecsExecutionRole,
                                                        infrastructure.volume,
                                                        infrastructure.cluster,
                                                        infrastructure.bucket,
                                                        infrastructure.sequencesTable,
                                                        infrastructure.reprocessingQueue,
                                                        infrastructure.dailyProcessingQueue,
                                                        infrastructure.sqsAccessPolicyStatement,
                                                        infrastructure.s3AccessPolicyStatement,
                                                        infrastructure.dynamoDBAccessPolicyStatement);
            helperFunctions.Create();

            var prepareSequences = new PrepareSequences(this,
                                                        "prepareSequences",
                                                        infrastructure.ecsExecutionRole,
                                                        infrastructure.volume,
                                                        infrastructure.cluster,
                                                        infrastructure.bucket,
                                                        infrastructure.sequencesTable,
                                                        infrastructure.reprocessingQueue,
                                                        infrastructure.dailyProcessingQueue);
            prepareSequences.CreateAddSequencesToQueue();
            prepareSequences.CreatePrepareSequences();
            prepareSequences.CreatePrepareConsenusSequences();

            var goFastaAlignment = new GoFastaAlignment(this,
                                                        "goFastaAlignment",
                                                        infrastructure.ecsExecutionRole,
                                                        infrastructure.volume,
                                                        infrastructure.cluster,
                                                        infrastructure.bucket,
                                                        infrastructure.sequencesTable);
            goFastaAlignment.Create();
            goFastaAlignment.CreateTestTask();

            var pangolinModel = new PangolinModel(  this,
                                                    "pangolinTaskDefinition",
                                                    infrastructure.ecsExecutionRole,
                                                    infrastructure.volume,
                                                    infrastructure.cluster,
                                                    infrastructure.bucket,
                                                    infrastructure.sequencesTable);
            pangolinModel.Create();

            var armadillinModel = new ArmadillinModel(  this,
                                                        "armadillinTaskDefinition",
                                                        infrastructure.ecsExecutionRole,
                                                        infrastructure.volume,
                                                        infrastructure.cluster,
                                                        infrastructure.bucket,
                                                        infrastructure.sequencesTable);
            armadillinModel.Create();
            armadillinModel.CreateTestTask();

            var genotypeVariantsModel = new GenotypeVariantsModel(  this,
                                                                    "genotypeVariantsTaskDefinition",
                                                                    infrastructure.ecsExecutionRole,
                                                                    infrastructure.volume,
                                                                    infrastructure.cluster,
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
                ExecutionRole = infrastructure.ecsExecutionRole,
                TaskRole = infrastructure.ecsExecutionRole
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
                Cluster = infrastructure.cluster,
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