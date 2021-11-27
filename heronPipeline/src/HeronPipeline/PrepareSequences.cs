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
  internal sealed class PrepareSequences: Construct 
  {
    public EcsRunTask addSequencesToQueueTask;
    public EcsRunTask prepareSequencesTask;
    public EcsRunTask prepareConsensusSequencesTask;
    private Construct scope;
    private string id;
    private Role ecsExecutionRole;
    private Amazon.CDK.AWS.ECS.Volume volume;
    private Cluster cluster;
    private Bucket bucket;
    private Table sequencesTable;
    private RetryProps retryItem;
    private Queue reprocessingQueue;
    private Queue dailyProcessingQueue;

    public PrepareSequences(  Construct scope, 
                              string id, 
                              Role executionRole, 
                              Amazon.CDK.AWS.ECS.Volume volume, 
                              Cluster cluster, 
                              Bucket bucket, 
                              Table sequencesTable,
                              Queue reprocessingQueue,
                              Queue dailyProcessingQueue): base(scope, id)
    {
      this.scope = scope;
      this.id = id;
      this.ecsExecutionRole = executionRole;
      this.volume = volume;
      this.cluster = cluster;
      this.bucket = bucket;
      this.sequencesTable = sequencesTable;
      this.reprocessingQueue = reprocessingQueue;
      this.dailyProcessingQueue = dailyProcessingQueue;
      this.retryItem = new RetryProps{
        BackoffRate = 5,
        Interval = Duration.Seconds(2),
        MaxAttempts = 3,
        Errors = new string[] {"States.ALL"}
      };
    }

    public void CreateAddSequencesToQueue()
    {
      var addSequencesToQueueImage = ContainerImage.FromAsset("src/images/addSequencesToQueue");
      var addSequencesToQueueTaskDefinition = new TaskDefinition(this, this.id + "_addSequencesToQueueTaskDefinition", new TaskDefinitionProps{
          Family = this.id + "_addSequencesToQueue",
          Cpu = "256",
          MemoryMiB = "512",
          NetworkMode = NetworkMode.AWS_VPC,
          Compatibility = Compatibility.FARGATE,
          ExecutionRole = ecsExecutionRole,
          TaskRole = ecsExecutionRole
      });
      addSequencesToQueueTaskDefinition.AddContainer("addSequencesToQueueContainer", new Amazon.CDK.AWS.ECS.ContainerDefinitionOptions
      {
          Image = addSequencesToQueueImage,
          Logging = new AwsLogDriver(new AwsLogDriverProps
          {
              StreamPrefix = "addSequencesToQueue",
              LogGroup = new LogGroup(this, "addSequencesToQueueLogGroup", new LogGroupProps
              {
                  LogGroupName = this.id + "_addSequencesToQueueLogGroup",
                  Retention = RetentionDays.ONE_WEEK,
                  RemovalPolicy = RemovalPolicy.DESTROY
              })
          })
      });

      var addSequencesToQueueContainer = addSequencesToQueueTaskDefinition.FindContainer("addSequencesToQueueContainer");

      this.addSequencesToQueueTask = new EcsRunTask(this, this.id + "_addSequencesToQueueTask", new EcsRunTaskProps
      {
          IntegrationPattern = IntegrationPattern.RUN_JOB,
          Cluster = cluster,
          TaskDefinition = addSequencesToQueueTaskDefinition,
          AssignPublicIp = true,
          LaunchTarget = new EcsFargateLaunchTarget(),
          ContainerOverrides = new ContainerOverride[] {
              new ContainerOverride {
                  ContainerDefinition = addSequencesToQueueContainer,
                  Environment = new TaskEnvironmentVariable[] {
                      new TaskEnvironmentVariable{
                          Name = "EXECUTION_MODE",
                          Value = JsonPath.StringAt("$.executionMode")
                        },
                      new TaskEnvironmentVariable{
                          Name = "HERON_SEQUENCES_TABLE",
                          Value = sequencesTable.TableName
                      },
                      new TaskEnvironmentVariable{
                          Name = "HERON_PROCESSING_QUEUE",
                          Value = reprocessingQueue.QueueUrl
                      },
                      new TaskEnvironmentVariable{
                        Name = "HERON_DAILY_PROCESSING_QUEUE",
                        Value = dailyProcessingQueue.QueueUrl
                      }
                  }
              }
          },
          ResultPath = JsonPath.DISCARD
      });
    }

    public void CreatePrepareSequences()
    {
      var prepareSequencesImage = ContainerImage.FromAsset("src/images/prepareSequences", new AssetImageProps
      { 
      });
      var prepareSequencesTaskDefinition = new TaskDefinition(this, this.id + "_prepareSequencesTaskDefinition", new TaskDefinitionProps{
          Family = this.id + "_prepareSequences",
          Cpu = "1024",
          MemoryMiB = "4096",
          NetworkMode = NetworkMode.AWS_VPC,
          Compatibility = Compatibility.FARGATE,
          ExecutionRole = ecsExecutionRole,
          TaskRole = ecsExecutionRole,
          Volumes = new Amazon.CDK.AWS.ECS.Volume[] { volume }
      });

      prepareSequencesTaskDefinition.AddContainer("prepareSequencesContainer", new Amazon.CDK.AWS.ECS.ContainerDefinitionOptions
      {
          Image = prepareSequencesImage,
          Logging = new AwsLogDriver(new AwsLogDriverProps
          {
              StreamPrefix = "prepareSequences",
              LogGroup = new LogGroup(this, "prepareSequencesLogGroup", new LogGroupProps
              {
                  LogGroupName = this.id + "prepareSequencesLogGroup",
                  Retention = RetentionDays.ONE_WEEK,
                  RemovalPolicy = RemovalPolicy.DESTROY
              })
          })
      });
      var prepareSequencesContainer = prepareSequencesTaskDefinition.FindContainer("prepareSequencesContainer");
      prepareSequencesContainer.AddMountPoints(new MountPoint[] {
              new MountPoint {
                  SourceVolume = "efsVolume",
                  ContainerPath = "/mnt/efs0",
                  ReadOnly = false,
              }
          });

      this.prepareSequencesTask = new EcsRunTask(this, this.id + "_prepareSequencesTask", new EcsRunTaskProps
      {
          IntegrationPattern = IntegrationPattern.RUN_JOB,
          Cluster = cluster,
          TaskDefinition = prepareSequencesTaskDefinition,
          AssignPublicIp = true,
          LaunchTarget = new EcsFargateLaunchTarget(),
          ContainerOverrides = new ContainerOverride[] {
              new ContainerOverride {
                  ContainerDefinition = prepareSequencesContainer,
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
                      }
                  }
              }
          },
          ResultPath = JsonPath.DISCARD
      });

      prepareSequencesTask.AddRetry(retryItem);
    }

    public void CreatePrepareConsenusSequences()
    {
      var prepareConsensusSequencesImage = ContainerImage.FromAsset("src/images/prepareConsensusSequences", new AssetImageProps
      { 
      });

      var prepareConsensusSequencesTaskDefinition = new TaskDefinition(this, this.id + "_prepareConsensusSequencesTaskDefinition", new TaskDefinitionProps{
          Family = this.id + "_prepareConsensusSequences",
          Cpu = "1024",
          MemoryMiB = "4096",
          NetworkMode = NetworkMode.AWS_VPC,
          Compatibility = Compatibility.FARGATE,
          ExecutionRole = ecsExecutionRole,
          TaskRole = ecsExecutionRole,
          Volumes = new Amazon.CDK.AWS.ECS.Volume[] { volume }
      });

      prepareConsensusSequencesTaskDefinition.AddContainer("prepareConsensusSequencesContainer", new Amazon.CDK.AWS.ECS.ContainerDefinitionOptions
      {
          Image = prepareConsensusSequencesImage,
          Logging = new AwsLogDriver(new AwsLogDriverProps
          {
              StreamPrefix = "prepareConsensusSequences",
              LogGroup = new LogGroup(this, "prepareConsensusSequencesLogGroup", new LogGroupProps
              {
                  LogGroupName = this.id + "prepareConsensusSequencesLogGroup",
                  Retention = RetentionDays.ONE_WEEK,
                  RemovalPolicy = RemovalPolicy.DESTROY
              })
          })
      });

      var prepareConsensusSequencesContainer = prepareConsensusSequencesTaskDefinition.FindContainer("prepareConsensusSequencesContainer");
      prepareConsensusSequencesContainer.AddMountPoints(new MountPoint[] {
              new MountPoint {
                  SourceVolume = "efsVolume",
                  ContainerPath = "/mnt/efs0",
                  ReadOnly = false,
              }
          });

      this.prepareConsensusSequencesTask = new EcsRunTask(this, this.id + "_prepareConsensusSequencesTask", new EcsRunTaskProps
      {
          IntegrationPattern = IntegrationPattern.RUN_JOB,
          Cluster = cluster,
          TaskDefinition = prepareConsensusSequencesTaskDefinition,
          AssignPublicIp = true,
          LaunchTarget = new EcsFargateLaunchTarget(),
          ContainerOverrides = new ContainerOverride[] {
              new ContainerOverride {
                  ContainerDefinition = prepareConsensusSequencesContainer,
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
                        Name = "SAMPLE_BATCH_SIZE",
                        Value = JsonPath.StringAt("$.sampleBatchSize")
                      },
                  }
              }
          },
          ResultPath = JsonPath.DISCARD
      });

      prepareConsensusSequencesTask.AddRetry(retryItem);
    }

  }
}