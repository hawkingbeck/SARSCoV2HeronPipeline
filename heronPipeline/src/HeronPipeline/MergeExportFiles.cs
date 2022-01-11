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
  internal sealed class MergeExportFiles: Construct {
    public EcsRunTask mergeMutationExportFilesTask;

    private Construct scope;
    private string id;
    
    private Infrastructure infrastructure;
    
    public MergeExportFiles(Construct scope, string id, Infrastructure infrastructure): base(scope, id)
    {
      this.scope = scope;
      this.id = id;
      this.infrastructure = infrastructure;
    }

    public void Create()
    {
      CreateMergeMutationExportFilesTask();
    }
    public void CreateMergeMutationExportFilesTask(){
      
      var mergeMutationExportFilesImage = ContainerImage.FromAsset("src/images/mergeMutationExportFiles");
      var mergeMutationExportFilesTaskDefinition = new TaskDefinition(this, this.id + "_mergeMutationExportFilesTaskDefinition", new TaskDefinitionProps{
          Family = this.id + "_mergeMutationExportFiles",
          Cpu = "4096",
          MemoryMiB = "30720",
          NetworkMode = NetworkMode.AWS_VPC,
          Compatibility = Compatibility.FARGATE,
          ExecutionRole = this.infrastructure.ecsExecutionRole,
          TaskRole = this.infrastructure.ecsExecutionRole,
          Volumes = new Amazon.CDK.AWS.ECS.Volume[] { this.infrastructure.volume }
      });
      mergeMutationExportFilesTaskDefinition.AddContainer("mergeMutationExportFilesContainer", new Amazon.CDK.AWS.ECS.ContainerDefinitionOptions
      {
          Image = mergeMutationExportFilesImage,
          Logging = new AwsLogDriver(new AwsLogDriverProps
          {
              StreamPrefix = "mergeMutationExportFiles",
              LogGroup = new LogGroup(this, "mergeMutationExportFilesLogGroup", new LogGroupProps
              {
                  LogGroupName = this.id + "mergeMutationExportFilesLogGroup",
                  Retention = RetentionDays.ONE_WEEK,
                  RemovalPolicy = RemovalPolicy.DESTROY
              })
          })
      });

      var mergeMutationExportFilesContainer = mergeMutationExportFilesTaskDefinition.FindContainer("mergeMutationExportFilesContainer");

      this.mergeMutationExportFilesTask = new EcsRunTask(this, this.id + "_mergeMutationExportFilesTask", new EcsRunTaskProps
      {
          IntegrationPattern = IntegrationPattern.RUN_JOB,
          Cluster = infrastructure.cluster,
          TaskDefinition = mergeMutationExportFilesTaskDefinition,
          AssignPublicIp = true,
          LaunchTarget = new EcsFargateLaunchTarget(),
          ContainerOverrides = new ContainerOverride[] {
              new ContainerOverride {
                  ContainerDefinition = mergeMutationExportFilesContainer,
                  Environment = new TaskEnvironmentVariable[] {
                      new TaskEnvironmentVariable{
                        Name = "EXPORT_ARN",
                        Value = JsonPath.StringAt("$.exportJob.exportArn")
                      },
                      new TaskEnvironmentVariable{
                        Name = "S3_PREFIX",
                        Value = JsonPath.StringAt("$.exportJob.s3Prefix")
                      },
                      new TaskEnvironmentVariable{
                        Name = "HERON_BUCKET",
                        Value = infrastructure.bucket.BucketName
                      }
                  }
              }
          }
      });
    }
  }
}