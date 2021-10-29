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
  internal sealed class GoFastaAlignment : Construct
  {
    public EcsRunTask goFastaAlignTask;
    public EcsRunTask goFastaAlignTestTask;

    private Construct scope;
    private Role ecsExecutionRole;
    private Amazon.CDK.AWS.ECS.Volume volume;
    private Cluster cluster;
    private Bucket bucket;
    private Table sequencesTable;
    private RetryProps retryItem;
    private TaskDefinition alignFastaTaskDefinition;
    private Amazon.CDK.AWS.ECS.ContainerDefinition alignFastaContainer;

    public GoFastaAlignment(Construct scope, string id, Role executionRole, Amazon.CDK.AWS.ECS.Volume volume, Cluster cluster, Bucket bucket, Table sequencesTable): base(scope, id)
    {
      this.scope = scope;
      this.ecsExecutionRole = executionRole;
      this.volume = volume;
      this.cluster = cluster;
      this.bucket = bucket;
      this.sequencesTable = sequencesTable;

      this.retryItem = new RetryProps {
              BackoffRate = 5,
              Interval = Duration.Seconds(2),
              MaxAttempts = 5,
              Errors = new string[] {"States.ALL"}
            };
    }

    public void Create(){
      var alignFastaImage = ContainerImage.FromAsset("src/images/goFastaAlignment");
      alignFastaTaskDefinition = new TaskDefinition(scope, "goFastaTaskDefinition", new TaskDefinitionProps{
          Family = "goFasta",
          Cpu = "1024",
          MemoryMiB = "4096",
          NetworkMode = NetworkMode.AWS_VPC,
          Compatibility = Compatibility.FARGATE,
          ExecutionRole = ecsExecutionRole,
          TaskRole = ecsExecutionRole,
          Volumes = new Amazon.CDK.AWS.ECS.Volume[] { volume }
      });

      alignFastaTaskDefinition.AddContainer("goFastaAlignContainer", new Amazon.CDK.AWS.ECS.ContainerDefinitionOptions
      {
          Image = alignFastaImage,
          Logging = new AwsLogDriver(new AwsLogDriverProps
          {
              StreamPrefix = "goFastaAlign",
              LogGroup = new LogGroup(scope, "goFastaAlignLogGroup", new LogGroupProps
              {
                  LogGroupName = "goFastaAlignLogGroup",
                  Retention = RetentionDays.ONE_WEEK,
                  RemovalPolicy = RemovalPolicy.DESTROY
              })
          })
      });
      
      alignFastaContainer = alignFastaTaskDefinition.FindContainer("goFastaAlignContainer");
      alignFastaContainer.AddMountPoints(new MountPoint[] {
              new MountPoint {
                  SourceVolume = "efsVolume",
                  ContainerPath = "/mnt/efs0",
                  ReadOnly = false,
              }
          });
            
      goFastaAlignTask = new EcsRunTask(scope, "goFastaAlignTask", new EcsRunTaskProps
      {
          IntegrationPattern = IntegrationPattern.RUN_JOB,
          Cluster = cluster,
          TaskDefinition = alignFastaTaskDefinition,
          AssignPublicIp = true,
          LaunchTarget = new EcsFargateLaunchTarget(),
          ContainerOverrides = new ContainerOverride[] {
              new ContainerOverride {
                  ContainerDefinition = alignFastaContainer,
                  Environment = new TaskEnvironmentVariable[] {
                      new TaskEnvironmentVariable{
                        Name = "ITERATION_UUID",
                        Value = JsonPath.StringAt("$.sampleBatch.iterationUUID")
                      },
                      new TaskEnvironmentVariable{
                        Name = "SEQ_DATA_ROOT",
                        Value = "/mnt/efs0/seqData"
                      },
                      new TaskEnvironmentVariable{
                        Name = "DATE_PARTITION",
                        Value = JsonPath.StringAt("$.date")   
                      },
                      new TaskEnvironmentVariable{
                        Name = "HERON_SAMPLES_BUCKET",
                        Value = bucket.BucketName
                      },
                      new TaskEnvironmentVariable{
                          Name = "HERON_SEQUENCES_TABLE",
                          Value = sequencesTable.TableName
                      },
                      new TaskEnvironmentVariable{
                          Name = "MESSAGE_LIST_S3_KEY",
                          Value = JsonPath.StringAt("$.sampleBatch.messageListS3Key")
                      },
                      new TaskEnvironmentVariable{
                          Name = "REF_FASTA_KEY",
                          Value = "resources/MN908947.fa"
                      },
                      new TaskEnvironmentVariable{
                          Name = "TRIM_START",
                          Value = "265"
                      },
                      new TaskEnvironmentVariable{
                          Name = "TRIM_END",
                          Value = "29674"
                      }
                  }
              }
          },
          ResultPath = JsonPath.DISCARD
      });

      goFastaAlignTask.AddRetry(retryItem);
    }

    public void CreateTestTask(){
      goFastaAlignTestTask = new EcsRunTask(scope, "goFastaAlignTestTask", new EcsRunTaskProps
      {
          IntegrationPattern = IntegrationPattern.RUN_JOB,
          Cluster = cluster,
          TaskDefinition = alignFastaTaskDefinition,
          AssignPublicIp = true,
          LaunchTarget = new EcsFargateLaunchTarget(),
          ContainerOverrides = new ContainerOverride[] {
              new ContainerOverride {
                  ContainerDefinition = alignFastaContainer,
                  Environment = new TaskEnvironmentVariable[] {
                      new TaskEnvironmentVariable{
                        Name = "ITERATION_UUID",
                        Value = JsonPath.StringAt("$.sampleBatch.iterationUUID")
                      },
                      new TaskEnvironmentVariable{
                        Name = "SEQ_DATA_ROOT",
                        Value = "/mnt/efs0/seqData"
                      },
                      new TaskEnvironmentVariable{
                        Name = "DATE_PARTITION",
                        Value = JsonPath.StringAt("$.date")   
                      },
                      new TaskEnvironmentVariable{
                        Name = "HERON_SAMPLES_BUCKET",
                        Value = bucket.BucketName
                      },
                      new TaskEnvironmentVariable{
                          Name = "HERON_SEQUENCES_TABLE",
                          Value = sequencesTable.TableName
                      },
                      new TaskEnvironmentVariable{
                          Name = "MESSAGE_LIST_S3_KEY",
                          Value = JsonPath.StringAt("$.sampleBatch.messageListS3Key")
                      },
                      new TaskEnvironmentVariable{
                          Name = "REF_FASTA_KEY",
                          Value = "resources/MN908947.fa"
                      },
                      new TaskEnvironmentVariable{
                          Name = "TRIM_START",
                          Value = "265"
                      },
                      new TaskEnvironmentVariable{
                          Name = "TRIM_END",
                          Value = "29674"
                      }
                  }
              }
          },
          ResultPath = JsonPath.DISCARD
      });


    }
  }

  
}