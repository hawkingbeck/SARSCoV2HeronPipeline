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
  internal sealed class MutationsModel: Construct 
  {
    public EcsRunTask mutationsTask;
    public EcsRunTask mutationsTestTask;
    public Succeed skipMutationsTask;
    private Construct scope;
    private string id;
    private Role ecsExecutionRole;
    private Amazon.CDK.AWS.ECS.Volume volume;
    private Cluster cluster;
    private Bucket bucket;
    private Table sequencesTable;
    private RetryProps retryItem;

    public MutationsModel(Construct scope, string id, Role executionRole, Amazon.CDK.AWS.ECS.Volume volume, Cluster cluster, Bucket bucket, Table sequencesTable): base(scope, id)
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

    public void Create(){
      CreateTask();
      CreateTest();
    }
    public void CreateTask()
    {
      var mutationsImage = ContainerImage.FromAsset("src/images/mutations");
      var mutationsTaskDefinition = new TaskDefinition(this, this.id + "_mutationsTaskDefinition", new TaskDefinitionProps{
          Family = this.id + "_mutations",
          Cpu = "1024",
          MemoryMiB = "2048",
          NetworkMode = NetworkMode.AWS_VPC,
          Compatibility = Compatibility.FARGATE,
          ExecutionRole = ecsExecutionRole,
          TaskRole = ecsExecutionRole,
          Volumes = new Amazon.CDK.AWS.ECS.Volume[] { volume }
      });
      mutationsTaskDefinition.AddContainer("mutationsContainer", new Amazon.CDK.AWS.ECS.ContainerDefinitionOptions
      {
          Image = mutationsImage,
          Logging = new AwsLogDriver(new AwsLogDriverProps
          {
              StreamPrefix = "mutationsVariants",
              LogGroup = new LogGroup(this, "mutationsLogGroup", new LogGroupProps
              {
                  LogGroupName = this.id + "mutationsLogGroup",
                  Retention = RetentionDays.ONE_WEEK,
                  RemovalPolicy = RemovalPolicy.DESTROY
              })
          })
      });
      var mutationsContainer = mutationsTaskDefinition.FindContainer("mutationsContainer");
      mutationsContainer.AddMountPoints(new MountPoint[] {
              new MountPoint {
                  SourceVolume = "efsVolume",
                  ContainerPath = "/mnt/efs0",
                  ReadOnly = false,
              }
          });
      this.mutationsTask = new EcsRunTask(this, this.id + "_mutationsPlaceTask", new EcsRunTaskProps
      {
          IntegrationPattern = IntegrationPattern.RUN_JOB,
          Cluster = cluster,
          TaskDefinition = mutationsTaskDefinition,
          AssignPublicIp = true,
          LaunchTarget = new EcsFargateLaunchTarget(),
          ContainerOverrides = new ContainerOverride[] {
              new ContainerOverride {
                  ContainerDefinition = mutationsContainer,
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
                      },
                      new TaskEnvironmentVariable{
                          Name = "REF_FASTA_KEY",
                          Value = "resources/MN908947.fa"
                      },
                      new TaskEnvironmentVariable{
                          Name = "REF_GB_KEY",
                          Value = "resources/MN908947.gb"
                      },
                      new TaskEnvironmentVariable{
                          Name = "GENES_TSV_KEY",
                          Value = "resources/genes.tsv"
                      },
                      new TaskEnvironmentVariable{
                          Name = "GENES_OVERLAP_TSV_KEY",
                          Value = "resources/gene_overlaps.tsv"
                      },
                      new TaskEnvironmentVariable{
                        Name = "GO_FASTA_THREADS",
                        Value = JsonPath.StringAt("$.goFastaThreads")
                      }
                  }
              }
          },
          ResultPath = JsonPath.DISCARD
      });
      this.mutationsTask.AddRetry(retryItem);

      skipMutationsTask = new Succeed(this, "skipMutationsTask");



    }

    private void CreateTest(){
      var mutationsImage = ContainerImage.FromAsset("src/images/mutations");
      var mutationsTaskDefinition = new TaskDefinition(this, this.id + "_mutationsTestTaskDefinition", new TaskDefinitionProps{
          Family = this.id + "_mutations",
          Cpu = "1024",
          MemoryMiB = "2048",
          NetworkMode = NetworkMode.AWS_VPC,
          Compatibility = Compatibility.FARGATE,
          ExecutionRole = ecsExecutionRole,
          TaskRole = ecsExecutionRole,
          Volumes = new Amazon.CDK.AWS.ECS.Volume[] { volume }
      });
      mutationsTaskDefinition.AddContainer("mutationsContainer", new Amazon.CDK.AWS.ECS.ContainerDefinitionOptions
      {
          Image = mutationsImage,
          Logging = new AwsLogDriver(new AwsLogDriverProps
          {
              StreamPrefix = "mutationsTestVariants",
              LogGroup = new LogGroup(this, "mutationsTestLogGroup", new LogGroupProps
              {
                  LogGroupName = this.id + "mutationsTestLogGroup",
                  Retention = RetentionDays.ONE_WEEK,
                  RemovalPolicy = RemovalPolicy.DESTROY
              })
          })
      });
      var mutationsContainer = mutationsTaskDefinition.FindContainer("mutationsContainer");
      mutationsContainer.AddMountPoints(new MountPoint[] {
              new MountPoint {
                  SourceVolume = "efsVolume",
                  ContainerPath = "/mnt/efs0",
                  ReadOnly = false,
              }
          });
      this.mutationsTestTask = new EcsRunTask(this, this.id + "_mutationsTestPlaceTask", new EcsRunTaskProps
      {
          IntegrationPattern = IntegrationPattern.RUN_JOB,
          Cluster = cluster,
          TaskDefinition = mutationsTaskDefinition,
          AssignPublicIp = true,
          LaunchTarget = new EcsFargateLaunchTarget(),
          ContainerOverrides = new ContainerOverride[] {
              new ContainerOverride {
                  ContainerDefinition = mutationsContainer,
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
                      },
                      new TaskEnvironmentVariable{
                          Name = "REF_FASTA_KEY",
                          Value = "resources/MN908947.fa"
                      },
                      new TaskEnvironmentVariable{
                          Name = "REF_GB_KEY",
                          Value = "resources/MN908947.gb"
                      },
                      new TaskEnvironmentVariable{
                          Name = "GENES_TSV_KEY",
                          Value = "resources/genes.tsv"
                      },
                      new TaskEnvironmentVariable{
                          Name = "GENES_OVERLAP_TSV_KEY",
                          Value = "resources/gene_overlaps.tsv"
                      },
                      new TaskEnvironmentVariable{
                        Name = "GO_FASTA_THREADS",
                        Value = JsonPath.StringAt("$.goFastaThreads")
                      }
                  }
              }
          },
          ResultPath = JsonPath.DISCARD
      });
    }
  }
      
}