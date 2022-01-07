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

      this.startTableExportTask = new LambdaInvoke(this, this.id + "_startTableExportTask", new LambdaInvokeProps{
          LambdaFunction = startTableExportFunction,
          ResultPath = "$.exportTable",
          PayloadResponseOnly = true
      });
    }
    public void CreatePrev()
    {
      var exportMutationsImage = ContainerImage.FromAsset("src/images/exportMutations");
      var exportMutationsTaskDefinition = new TaskDefinition(this, this.id + "_exportMutationsTaskDefinition", new TaskDefinitionProps{
          Family = this.id + "_exportMutations",
          Cpu = "4096",
          MemoryMiB = "16384",
          NetworkMode = NetworkMode.AWS_VPC,
          Compatibility = Compatibility.FARGATE,
          ExecutionRole = infrastructure.ecsExecutionRole,
          TaskRole = infrastructure.ecsExecutionRole
      });
      exportMutationsTaskDefinition.AddContainer("exportMutationsContainer", new Amazon.CDK.AWS.ECS.ContainerDefinitionOptions
      {
          Image = exportMutationsImage,
          Logging = new AwsLogDriver(new AwsLogDriverProps
          {
              StreamPrefix = "exportMutations",
              LogGroup = new LogGroup(this, "exportMutationsLogGroup", new LogGroupProps
              {
                  LogGroupName = this.id + "exportMutationsLogGroup",
                  Retention = RetentionDays.ONE_WEEK,
                  RemovalPolicy = RemovalPolicy.DESTROY
              })
          })
      });
      var exportMutationsContainer = exportMutationsTaskDefinition.FindContainer("exportMutationsContainer");
      exportMutationsTask = new EcsRunTask(this, this.id + "_exportMutationsTask", new EcsRunTaskProps
      {
          IntegrationPattern = IntegrationPattern.RUN_JOB,
          Cluster = infrastructure.cluster,
          TaskDefinition = exportMutationsTaskDefinition,
          AssignPublicIp = true,
          LaunchTarget = new EcsFargateLaunchTarget(),
          ContainerOverrides = new ContainerOverride[] {
              new ContainerOverride {
                  ContainerDefinition = exportMutationsContainer,
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
                          Name = "HERON_MUTATIONS_TABLE",
                          Value = infrastructure.mutationsTable.TableName
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
    }
  }
}