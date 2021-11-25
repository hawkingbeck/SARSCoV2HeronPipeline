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
            var infrastructure = new Infrastructure(this, id);
            infrastructure.Create();

            var helperFunctions = new HelperFunctions(this, id, infrastructure.ecsExecutionRole, infrastructure.volume, infrastructure.cluster, infrastructure.bucket, infrastructure.sequencesTable, infrastructure.reprocessingQueue, infrastructure.dailyProcessingQueue, infrastructure.sqsAccessPolicyStatement, infrastructure.s3AccessPolicyStatement, infrastructure.dynamoDBAccessPolicyStatement);
            helperFunctions.Create();

            var prepareSequences = new PrepareSequences(this, id, infrastructure.ecsExecutionRole, infrastructure.volume, infrastructure.cluster, infrastructure.bucket, infrastructure.sequencesTable, infrastructure.reprocessingQueue, infrastructure.dailyProcessingQueue);
            prepareSequences.CreateAddSequencesToQueue();
            prepareSequences.CreatePrepareSequences();
            prepareSequences.CreatePrepareConsenusSequences();

            var goFastaAlignment = new GoFastaAlignment(this, id, infrastructure.ecsExecutionRole, infrastructure.volume, infrastructure.cluster, infrastructure.bucket, infrastructure.sequencesTable);
            goFastaAlignment.Create();
            goFastaAlignment.CreateTestTask();

            var pangolinModel = new PangolinModel(this, id, infrastructure.ecsExecutionRole, infrastructure.volume, infrastructure.cluster, infrastructure.bucket, infrastructure.sequencesTable);
            pangolinModel.Create();

            var armadillinModel = new ArmadillinModel(this, id, infrastructure.ecsExecutionRole, infrastructure.volume, infrastructure.cluster, infrastructure.bucket, infrastructure.sequencesTable);
            armadillinModel.Create();
            armadillinModel.CreateTestTask();

            var genotypeVariantsModel = new GenotypeVariantsModel(this, id, infrastructure.ecsExecutionRole, infrastructure.volume, infrastructure.cluster, infrastructure.bucket, infrastructure.sequencesTable);
            genotypeVariantsModel.Create();

            var exportResults = new ExportResults(this, id, infrastructure);
            exportResults.Create();

            var stateMachines = new StateMachines(this, id, infrastructure, pangolinModel, armadillinModel, genotypeVariantsModel, prepareSequences, goFastaAlignment, helperFunctions, exportResults);
            stateMachines.Create();
        }
    }

    internal sealed class TestClass: Construct 
    {
        public TestClass(Construct scope, string id): base(scope, id)
        {

        }
    }
}