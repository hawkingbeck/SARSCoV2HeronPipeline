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

namespace HeronPipeline
{
  internal sealed class StateMachines: Construct 
  {
    private Infrastructure infrastructure;
    private PangolinModel pangolinModel;
    private ArmadillinModel armadillinModel;
    private GenotypeVariantsModel genotypeVariantsModel;
    private PrepareSequences prepareSequences;
    private GoFastaAlignment goFastaAlignment;
    private HelperFunctions helperFunctions;
    private ExportResults exportResults;


    private StateMachine processSampleBatchStateMachine;
    private StateMachine startNestedSampleProcessingStateMachine;

    public StateMachines( Construct scope, 
                          string id, 
                          Infrastructure infrastructure,
                          PangolinModel pangolinModel,
                          ArmadillinModel armadillinModel,
                          GenotypeVariantsModel genotypeVariantsModel,
                          PrepareSequences prepareSequences,
                          GoFastaAlignment goFastaAlignment,
                          HelperFunctions helperFunctions,
                          ExportResults exportResults
                          ): base(scope, id)
    {
      this.infrastructure = infrastructure;
      this.pangolinModel = pangolinModel;
      this.armadillinModel = armadillinModel;
      this.genotypeVariantsModel = genotypeVariantsModel;
      this.prepareSequences = prepareSequences;
      this.goFastaAlignment = goFastaAlignment;
      this.helperFunctions = helperFunctions;
      this.exportResults = exportResults;
    }

    public void Create()
    {
      CreateProcessSampleBatchStateMachine();
      CreateStartNestedSequenceProcessingStateMachine();
      CreatePipelineStateMachine();
    }

    private void CreateProcessSampleBatchStateMachine()
    {
      var processSamplesFinishTask = new Succeed(this, "processSamplesSucceedTask");

      var shouldRunFastaAlignmentChoiceTask = new Choice(this, "shouldRunFastaAlignmentTask", new ChoiceProps{
        Comment = "Depending on the processing mode we can skip fasta the alignment step"
      });
      var performAlignmentStep = Condition.StringEquals(JsonPath.StringAt("$.executionMode"), "DAILY");
      var skipAlignmentStepCondition1 = Condition.StringEquals(JsonPath.StringAt("$.executionMode"), "REPROCESSING");
      var skipAlignmentStepCondition2 = Condition.StringEquals(JsonPath.StringAt("$.executionMode"), "ARMADILLIN-RERUN");

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
        .Start(prepareSequences.prepareSequencesTask)
        .Next(placeSequencesParallel);

      var alignFastaChain = Chain
        .Start(prepareSequences.prepareConsensusSequencesTask)
        .Next(goFastaAlignment.goFastaAlignTask)
        .Next(processSamplesChain);

      shouldRunFastaAlignmentChoiceTask.When(performAlignmentStep, alignFastaChain);
      shouldRunFastaAlignmentChoiceTask.When(skipAlignmentStepCondition1, processSamplesChain);
      shouldRunFastaAlignmentChoiceTask.When(skipAlignmentStepCondition2, processSamplesChain);

      messagesAvailableChoiceTask.When(messagesAvailableCondition, processSamplesChain);
      messagesAvailableChoiceTask.When(messagesNotAvailableCondition, processSamplesFinishTask);

      var processSampleBatchChain = Chain
        .Start(helperFunctions.readSampleBatchCountTask)
        .Next(messagesAvailableChoiceTask);
      
      processSampleBatchStateMachine = new StateMachine(this, "processSampleBatchStateMachine", new StateMachineProps{
        Definition = processSampleBatchChain
      });
    }
    private void CreateStartNestedSequenceProcessingStateMachine()
    {
      var startSampleProcessingMapParameters = new Dictionary<string, object>();
      startSampleProcessingMapParameters.Add("date.$", "$.date");
      startSampleProcessingMapParameters.Add("queueName.$", "$.queueName");
      startSampleProcessingMapParameters.Add("recipeFilePath.$", "$.recipeFilePath");
      startSampleProcessingMapParameters.Add("executionMode.$", "$.executionMode");

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
          {"bucketName", infrastructure.bucket.BucketName},
          {"executionMode", JsonPath.StringAt("$.executionMode")}
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

      startNestedSampleProcessingStateMachine = new StateMachine(this, "startNestedSampleProcessingStateMachine", new StateMachineProps{
        Definition = startNestedSampleProcessingDefinition
      });
    }

    private void CreatePipelineStateMachine()
    {
      var pipelineFinishTask = new Succeed(this, "pipelineSucceedTask");

      // Input parameters to the map iteration state
      var launchSampleProcessingMapParameters = new Dictionary<string, object>();
      launchSampleProcessingMapParameters.Add("date.$", "$.date");
      launchSampleProcessingMapParameters.Add("queueName.$", "$.messageCount.queueName");
      launchSampleProcessingMapParameters.Add("recipeFilePath.$", "$.recipeFilePath");
      launchSampleProcessingMapParameters.Add("mapIterations.$", "$$.Map.Item.Value.process");
      launchSampleProcessingMapParameters.Add("executionMode.$", "$.executionMode");

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
          {"recipeFilePath", JsonPath.StringAt("$.recipeFilePath")},
          {"executionMode", JsonPath.StringAt("$.executionMode")}
      };
      var stateMachineInput = TaskInput.FromObject(stateMachineInputObject);
              

      var startNestedStateMachine = new StepFunctionsStartExecution(this, "startNestedStateMachine", new StepFunctionsStartExecutionProps{
        StateMachine = startNestedSampleProcessingStateMachine,
        IntegrationPattern = IntegrationPattern.RUN_JOB,
        ResultPath = JsonPath.DISCARD,
        Input = stateMachineInput
      });

      launchSampleProcessingMap.Iterator(Chain.Start(startNestedStateMachine));            

      var processMessagesChain = Chain
        .Start(prepareSequences.addSequencesToQueueTask)
        .Next(helperFunctions.getMessageCountTask)
        .Next(launchSampleProcessingMap)
        .Next(exportResults.exportResultsTask)
        .Next(pipelineFinishTask);

      var pipelineChain = Chain
              .Start(processMessagesChain);

      var pipelineStateMachine = new StateMachine(this, "pipelineStateMachine", new StateMachineProps
      {
          Definition = pipelineChain
      });
    }
  }
}