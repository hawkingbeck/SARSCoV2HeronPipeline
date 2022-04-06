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
  internal sealed class PangolinModel: Construct 
  {
    public EcsRunTask pangolinTask;
    public Succeed skipPangolinTask;
    private Construct scope;
    private string id;
    private Role ecsExecutionRole;
    private Amazon.CDK.AWS.ECS.Volume volume;
    private Cluster cluster;
    private Bucket bucket;
    private Table sequencesTable;
    private RetryProps retryItem;

    public PangolinModel(Construct scope, string id, Role executionRole, Amazon.CDK.AWS.ECS.Volume volume, Cluster cluster, Bucket bucket, Table sequencesTable): base(scope, id)
    {
      this.scope = scope;
      this.id = id;
      this.ecsExecutionRole = executionRole;
      this.volume = volume;
      this.cluster = cluster;
      this.bucket = bucket;
      this.sequencesTable = sequencesTable;
      this.retryItem = new RetryProps{
        BackoffRate = 5,
        Interval = Duration.Seconds(2),
        MaxAttempts = 3,
        Errors = new string[] {"States.ALL"}
      };
    }

    public void Create()
    {
      var pangolinImage = ContainerImage.FromAsset("src/images/pangolin", new AssetImageProps
      { 
      });
      var pangolinTaskDefinition = new TaskDefinition(this, this.id + "_pangolinTaskDefinition", new TaskDefinitionProps{
          Family = this.id + "_pangolin",
          Cpu = "4096",
          MemoryMiB = "16384",
          NetworkMode = NetworkMode.AWS_VPC,
          Compatibility = Compatibility.FARGATE,
          ExecutionRole = ecsExecutionRole,
          TaskRole = ecsExecutionRole,
          Volumes = new Amazon.CDK.AWS.ECS.Volume[] { this.volume }
      });
      pangolinTaskDefinition.AddContainer("pangolinContainer", new Amazon.CDK.AWS.ECS.ContainerDefinitionOptions
      {
          Image = pangolinImage,
          Logging = new AwsLogDriver(new AwsLogDriverProps
          {
              StreamPrefix = "pangolin",
              LogGroup = new LogGroup(this, "pangolinLogGroup", new LogGroupProps
              {
                  LogGroupName = this.id + "pangolinLogGroup",
                  Retention = RetentionDays.ONE_WEEK,
                  RemovalPolicy = RemovalPolicy.DESTROY
              })
          })
      });
      
      var pangolinContainer = pangolinTaskDefinition.FindContainer("pangolinContainer");
      pangolinContainer.AddMountPoints(new MountPoint[] {
              new MountPoint {
                  SourceVolume = "efsVolume",
                  ContainerPath = "/mnt/efs0",
                  ReadOnly = false,
              }
          });
      this.pangolinTask = new EcsRunTask(this, this.id + "_pangolinPlaceTask", new EcsRunTaskProps
      {
          IntegrationPattern = IntegrationPattern.RUN_JOB,
          Cluster = cluster,
          TaskDefinition = pangolinTaskDefinition,
          AssignPublicIp = true,
          LaunchTarget = new EcsFargateLaunchTarget(),
          ContainerOverrides = new ContainerOverride[] {
              new ContainerOverride {
                  ContainerDefinition = pangolinContainer,
                  Environment = new TaskEnvironmentVariable[] {
                      new TaskEnvironmentVariable{
                        Name = "DATE_PARTITION",
                        Value = JsonPath.StringAt("$.date")
                      },
                      new TaskEnvironmentVariable{
                        Name = "MESSAGE_LIST_S3_KEY",
                        Value = JsonPath.StringAt("$.sampleBatch.messageListS3Key")
                      },
                      new TaskEnvironmentVariable{
                        Name = "HERON_SAMPLES_BUCKET",
                        Value = bucket.BucketName
                      },
                      new TaskEnvironmentVariable{
                        Name = "SEQ_DATA_ROOT",
                        Value = "/mnt/efs0/seqData"
                      },
                      new TaskEnvironmentVariable{
                        Name = "ITERATION_UUID",
                        Value = JsonPath.StringAt("$.sampleBatch.iterationUUID")
                      },
                      new TaskEnvironmentVariable{
                          Name = "HERON_SEQUENCES_TABLE",
                          Value = sequencesTable.TableName
                      }
                  }
              }
          },
          ResultPath = JsonPath.DISCARD
      });
      pangolinTask.AddRetry(retryItem);

      skipPangolinTask = new Succeed(this, "skipPangolinTask");
    }
  }
}