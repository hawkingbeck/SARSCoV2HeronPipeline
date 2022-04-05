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

            var idToSupply = id + "_";

            var testObj = new TestClass(this, "testClass");
            var infrastructure = new Infrastructure(this, idToSupply+"infra_");
            infrastructure.Create();
            

            var helperFunctions = new HelperFunctions(this, idToSupply+"helper_", infrastructure.ecsExecutionRole, infrastructure.volume, infrastructure.cluster, infrastructure.bucket, infrastructure.sequencesTable, infrastructure.reprocessingQueue, infrastructure.dailyProcessingQueue, infrastructure.sqsAccessPolicyStatement, infrastructure.s3AccessPolicyStatement, infrastructure.dynamoDBAccessPolicyStatement);
            helperFunctions.Create();

            var cleanEfs = new CleanEFS(this, idToSupply+"cleanEfs_", infrastructure.ecsExecutionRole, infrastructure.volume, infrastructure.cluster);
            cleanEfs.Create();

            var prepareSequences = new PrepareSequences(this, idToSupply+"prep_", infrastructure.ecsExecutionRole, infrastructure.volume, infrastructure.cluster, infrastructure.bucket, infrastructure.sequencesTable, infrastructure.reprocessingQueue, infrastructure.dailyProcessingQueue);
            prepareSequences.CreateAddSequencesToQueue();
            prepareSequences.CreatePrepareSequences();
            prepareSequences.CreatePrepareConsenusSequences();

            var goFastaAlignment = new GoFastaAlignment(this, idToSupply+"align_", infrastructure.ecsExecutionRole, infrastructure.volume, infrastructure.cluster, infrastructure.bucket, infrastructure.sequencesTable);
            goFastaAlignment.Create();

            var mutationsModel = new MutationsModel(this, idToSupply+"mutations_", infrastructure.ecsExecutionRole, infrastructure.volume, infrastructure.cluster, infrastructure.bucket, infrastructure.sequencesTable, infrastructure.mutationsTable);
            mutationsModel.Create();

            var pangolinModel = new PangolinModel(this, idToSupply+"pango_", infrastructure.ecsExecutionRole, infrastructure.volume, infrastructure.cluster, infrastructure.bucket, infrastructure.sequencesTable);
            pangolinModel.Create();

            var armadillinModel = new ArmadillinModel(this, idToSupply+"armadillin_", infrastructure.ecsExecutionRole, infrastructure.volume, infrastructure.cluster, infrastructure.bucket, infrastructure.sequencesTable);
            armadillinModel.Create();

            var genotypeVariantsModel = new GenotypeVariantsModel(this, idToSupply+"genotype_", infrastructure.ecsExecutionRole, infrastructure.volume, infrastructure.cluster, infrastructure.bucket, infrastructure.sequencesTable);
            genotypeVariantsModel.Create();

            var exportResults = new ExportResults(this, idToSupply+"export_", infrastructure);
            exportResults.Create();

            var mergeExportFiles = new MergeExportFiles(this, idToSupply+"merge_", infrastructure);
            mergeExportFiles.Create();

            var exportDynamoDBTable = new ExportDynamoDBTable(this, idToSupply+"exportDynamo_", infrastructure);
            exportDynamoDBTable.Create();

            var stateMachines = new StateMachines(this, idToSupply+"stateMachine_", infrastructure, pangolinModel, armadillinModel, genotypeVariantsModel, mutationsModel, prepareSequences, goFastaAlignment, helperFunctions, exportResults, mergeExportFiles, exportDynamoDBTable, cleanEfs);
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