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
  internal sealed class HelperFunctions : Construct
  {
    public LambdaInvoke readSampleBatchCountTask;
    public LambdaInvoke getMessageCountTask;
    private Construct scope;
    private Role ecsExecutionRole;
    private Amazon.CDK.AWS.ECS.Volume volume;
    private Cluster cluster;
    private Bucket bucket;
    private Table sequencesTable;
    private RetryProps retryItem;
    private TaskDefinition alignFastaTaskDefinition;
    private Amazon.CDK.AWS.ECS.ContainerDefinition alignFastaContainer;
    private Queue reprocessingQueue;
    private Queue dailyProcessingQueue;
    private PolicyStatement sqsAccessPolicyStatement;
    private PolicyStatement s3AccessPolicyStatement;
    private PolicyStatement dynamoDBAccessPolicyStatement;

    public HelperFunctions(
                            Construct scope, 
                            string id, 
                            Role executionRole, 
                            Amazon.CDK.AWS.ECS.Volume volume, 
                            Cluster cluster, 
                            Bucket bucket, 
                            Table sequencesTable,
                            Queue reprocessingQueue,
                            Queue dailyProcessingQueue,
                            PolicyStatement sqsAccessPolicyStatement,
                            PolicyStatement s3AccessPolicyStatement,
                            PolicyStatement dynamoDBAccessPolicyStatement
                            ): base(scope, id)
    {
      this.scope = scope;
      this.ecsExecutionRole = executionRole;
      this.volume = volume;
      this.cluster = cluster;
      this.bucket = bucket;
      this.sequencesTable = sequencesTable;
      this.reprocessingQueue = reprocessingQueue;
      this.dailyProcessingQueue = dailyProcessingQueue;
      this.sqsAccessPolicyStatement = sqsAccessPolicyStatement;
      this.s3AccessPolicyStatement = s3AccessPolicyStatement;
      this.dynamoDBAccessPolicyStatement = dynamoDBAccessPolicyStatement;

      this.retryItem = new RetryProps {
              BackoffRate = 5,
              Interval = Duration.Seconds(2),
              MaxAttempts = 5,
              Errors = new string[] {"States.ALL"}
            };
    }

    public void Create()
    {
      this.CreateReadSampleBatch();
      this.CreateGetMessageCount();
    }
    private void CreateGetMessageCount()
    {
      var getMessageCountFunction = new PythonFunction(this, "getMessageCountFunction", new PythonFunctionProps{
          Entry = "src/functions/getMessageCount",
          Runtime = Runtime.PYTHON_3_7,
          Index = "app.py",
          Handler = "lambda_handler",
          Environment = new Dictionary<string, string> {
              {"SAMPLE_BATCH_SIZE", JsonPath.StringAt("$.sampleBatchSize")},
              {"EXECUTION_MODE", JsonPath.StringAt("$.executionMode")},
              {"HERON_SEQUENCES_TABLE",sequencesTable.TableName},
              {"HERON_PROCESSING_QUEUE", reprocessingQueue.QueueUrl},
              {"HERON_DAILY_PROCESSING_QUEUE",dailyProcessingQueue.QueueUrl}
          }
      });
      getMessageCountFunction.AddToRolePolicy(sqsAccessPolicyStatement);

      this.getMessageCountTask = new LambdaInvoke(this, "getMessageCountTask", new LambdaInvokeProps{
          LambdaFunction = getMessageCountFunction,
          ResultPath = "$.messageCount",
          PayloadResponseOnly = true
      });



    }

    private void CreateReadSampleBatch()
    {
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

            this.readSampleBatchCountTask = new LambdaInvoke(this, "readSampleBatchCountTask", new LambdaInvokeProps{
              LambdaFunction = readSampleBatchFunction,
              ResultPath = "$.sampleBatch",
              PayloadResponseOnly = true
            });
            readSampleBatchCountTask.AddRetry(retryItem);
    }
  }
}