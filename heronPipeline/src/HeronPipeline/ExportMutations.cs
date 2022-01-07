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


namespace HeronPipeline {
  internal sealed class ExportMutations: Construct {

    public EcsRunTask exportMutationsTask;
    public LambdaInvoke startTableExportTask;
    public LambdaInvoke getExportStatusTask;

    private Construct scope;
    private string id;
    
    private Infrastructure infrastructure;
    
    public ExportMutations(Construct scope, string id, Infrastructure infrastructure): base(scope, id)
    {
      this.scope = scope;
      this.id = id;
      this.infrastructure = infrastructure;
    }

    public void Create()
    {
      CreateStartExportFunction();
      CreateGetExportStatus();
    }

    public void CreateStartExportFunction()
    {
      var startTableExportFunction = new PythonFunction(this, this.id + "_startTableExportFunction", new PythonFunctionProps{
          Entry = "src/functions/startTableExport",
          Runtime = Runtime.PYTHON_3_7,
          Index = "app.py",
          Handler = "lambda_handler",
          Environment = new Dictionary<string, string> {
              {"HERON_MUTATIONS_TABLE",infrastructure.mutationsTable.TableArn},
              {"HERON_BUCKET", infrastructure.bucket.BucketName}
          }
      });
      startTableExportFunction.AddToRolePolicy(this.infrastructure.dynamoDBAccessPolicyStatement);
      startTableExportFunction.AddToRolePolicy(this.infrastructure.s3AccessPolicyStatement);

      this.startTableExportTask = new LambdaInvoke(this, this.id + "_startTableExportTask", new LambdaInvokeProps{
          LambdaFunction = startTableExportFunction,
          ResultPath = "$.exportJob",
          PayloadResponseOnly = true
      });
    }

    public void CreateGetExportStatus(){
      var getExportStatusFunction = new PythonFunction(this, this.id + "_getExportStatusFunction", new PythonFunctionProps{
        Entry = "src/functions/getExportStatusFunction",
          Runtime = Runtime.PYTHON_3_7,
          Index = "app.py",
          Handler = "lambda_handler",
          Environment = new Dictionary<string, string> {
              {"HERON_MUTATIONS_TABLE",infrastructure.mutationsTable.TableArn},
              {"HERON_BUCKET", infrastructure.bucket.BucketName}
          }
      });
      getExportStatusFunction.AddToRolePolicy(this.infrastructure.dynamoDBAccessPolicyStatement);
      getExportStatusFunction.AddToRolePolicy(this.infrastructure.s3AccessPolicyStatement);

      this.getExportStatusTask = new LambdaInvoke(this, this.id + "_getExportStatusTask", new LambdaInvokeProps{
          LambdaFunction = getExportStatusFunction,
          ResultPath = "$.exportStatus",
          PayloadResponseOnly = true
      });
    }
  }
}