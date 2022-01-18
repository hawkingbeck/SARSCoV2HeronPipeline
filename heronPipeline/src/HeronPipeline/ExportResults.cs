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
  internal sealed class ExportResults: Construct {

    public EcsRunTask exportResultsTask;

    private Construct scope;
    private string id;
    
    private Infrastructure infrastructure;
    
    public ExportResults(Construct scope, string id, Infrastructure infrastructure): base(scope, id)
    {
      this.scope = scope;
      this.id = id;
      this.infrastructure = infrastructure;
    }
    public void Create()
    {
      var exportResultsImage = ContainerImage.FromAsset("src/images/exportResults");
      
      var exportResultsTaskDefinition = new TaskDefinition(this, this.id + "_exportResultsTaskDefinition", new TaskDefinitionProps{
          Family = this.id + "_exportResults",
          Cpu = "4096",
          MemoryMiB = "30720",
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
                  LogGroupName = this.id + "exportResultsLogGroup",
                  Retention = RetentionDays.ONE_WEEK,
                  RemovalPolicy = RemovalPolicy.DESTROY
              })
          })
      });
      var exportResultsContainer = exportResultsTaskDefinition.FindContainer("exportResultsContainer");
      exportResultsTask = new EcsRunTask(this, this.id + "_exportResultsTask", new EcsRunTaskProps
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
                        Name = "BRANCH_ZERO",
                        Value = JsonPath.StringAt("$.export[0].exportMutations.Output.exportJob.resultS3Prefix")
                      },
                      new TaskEnvironmentVariable{
                        Name = "BRANCH_ONE",
                        Value = JsonPath.StringAt("$.export[1].exportSequences.Output.exportJob.resultS3Prefix")
                      },
                      new TaskEnvironmentVariable{
                        Name = "BRANCH_TWO",
                        Value = JsonPath.StringAt("$.export[2].exportSamples.Output.exportJob.resultS3Prefix")
                      },
                      new TaskEnvironmentVariable{
                        Name = "DATE_PARTITION",
                        Value = JsonPath.StringAt("$.date")
                      },
                      new TaskEnvironmentVariable{
                        Name = "HERON_SAMPLES_BUCKET",
                        Value = infrastructure.bucket.BucketName
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