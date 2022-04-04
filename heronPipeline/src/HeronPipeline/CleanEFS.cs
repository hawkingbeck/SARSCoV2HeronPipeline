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
  internal sealed class CleanEFS: Construct {
    public EcsRunTask cleanEfsTask;
    private Construct scope;
    private string id;
    private Role ecsExecutionRole;
    private Amazon.CDK.AWS.ECS.Volume volume;
    private Cluster cluster;
    private RetryProps retryItem;
    private TaskDefinition cleanEfsTaskDefinition;
    private Amazon.CDK.AWS.ECS.ContainerDefinition cleanEfsContainer;
    public CleanEFS(Construct scope, string id, Role executionRole, Amazon.CDK.AWS.ECS.Volume volume, Cluster cluster): base(scope, id)
    {
      this.scope = scope;
      this.id = id;
      this.ecsExecutionRole = executionRole;
      this.volume = volume;
      this.cluster = cluster;
      this.retryItem = new RetryProps{
        BackoffRate = 5,
        Interval = Duration.Seconds(2),
        MaxAttempts = 3,
        Errors = new string[] {"States.ALL"}
      };
    }

    public void Create(){
      var cleanEfsImage = ContainerImage.FromAsset("src/images/cleanEfs", new AssetImageProps
      { 
      });
      this.cleanEfsTaskDefinition = new TaskDefinition(this, this.id + "_cleanEfs", new TaskDefinitionProps{
          Family = this.id + "_cleanEfs",
          Cpu = "1024",
          MemoryMiB = "4096",
          NetworkMode = NetworkMode.AWS_VPC,
          Compatibility = Compatibility.FARGATE,
          ExecutionRole = ecsExecutionRole,
          TaskRole = ecsExecutionRole,
          Volumes = new Amazon.CDK.AWS.ECS.Volume[] { volume }
      });
      this.cleanEfsTaskDefinition.AddContainer("cleanEfsContainer", new Amazon.CDK.AWS.ECS.ContainerDefinitionOptions
      {
          Image = cleanEfsImage,
          Logging = new AwsLogDriver(new AwsLogDriverProps
          {
              StreamPrefix = "cleanEfs",
              LogGroup = new LogGroup(this, "cleanEfsLogGroup", new LogGroupProps
              {
                  LogGroupName = this.id + "cleanEfsLogGroup",
                  Retention = RetentionDays.ONE_WEEK,
                  RemovalPolicy = RemovalPolicy.DESTROY
              })
          })
      });
      this.cleanEfsContainer = this.cleanEfsTaskDefinition.FindContainer("cleanEfsContainer");
      cleanEfsContainer.AddMountPoints(new MountPoint[] {
              new MountPoint {
                  SourceVolume = "efsVolume",
                  ContainerPath = "/mnt/efs0",
                  ReadOnly = false,
              }
          });
      this.cleanEfsTask = new EcsRunTask(this, this.id + "_cleanEfsPlaceTask", new EcsRunTaskProps
      {
          IntegrationPattern = IntegrationPattern.RUN_JOB,
          Cluster = cluster,
          TaskDefinition = this.cleanEfsTaskDefinition,
          AssignPublicIp = true,
          LaunchTarget = new EcsFargateLaunchTarget(),
          ContainerOverrides = new ContainerOverride[] {
              new ContainerOverride {
                  ContainerDefinition = this.cleanEfsContainer,
                  Environment = new TaskEnvironmentVariable[] {
                      new TaskEnvironmentVariable{
                        Name = "DATE_PARTITION",
                        Value = JsonPath.StringAt("$.date")
                      },
                      new TaskEnvironmentVariable{
                        Name = "SEQ_DATA_ROOT",
                        Value = "/mnt/efs0/seqData"
                      }
                  }
              }
          },
          ResultPath = JsonPath.DISCARD
      });
      
      this.cleanEfsTask.AddRetry(this.retryItem);
    }
  }
}