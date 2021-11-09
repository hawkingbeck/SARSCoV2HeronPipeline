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
  internal sealed class Infrastructure : Construct
  {
    public Amazon.CDK.AWS.ECS.Volume volume;
    public AccessPoint pipelineEFSAccessPoint;
    public Vpc vpc;
    public Bucket bucket;
    public Table samplesTable;
    public Table sequencesTable;
    public Queue dailyProcessingQueue;
    public Queue reprocessingQueue;
    
    private Construct scope;
    private SecurityGroup secGroup;

    
    public Infrastructure(Construct scope, string id): base(scope, id)
    {
      this.scope = scope;
    }

    public void Create()
    {
      CreateVPC();
      CreateStorage();
      CreateEFS();
      CreateQueues();
    }
    private void CreateVPC()
    {
      vpc = new Vpc(this, "vpc", new VpcProps{
                MaxAzs = 3, ///TODO: Increase this once EIP's are freed
                Cidr = "11.0.0.0/16",
            });

      secGroup = new SecurityGroup(this, "vpcSecurityGroup", new SecurityGroupProps{
          Vpc = vpc,
          AllowAllOutbound = true
      });
      secGroup.AddIngressRule(Peer.AnyIpv4(), Port.AllIcmp(), "All Incoming");
      secGroup.AddIngressRule(Peer.AnyIpv4(), Port.Tcp(2049), "EFS Port");
      secGroup.AddIngressRule(Peer.AnyIpv4(), Port.AllTraffic(), "All Traffic");
      secGroup.Node.AddDependency(vpc);
    }

    private void CreateEFS()
    {
      //++++++++++++++++++++++++++++++++++++++++++
      // File System (EFS)
      //++++++++++++++++++++++++++++++++++++++++++
      var pipelineEFS = new Amazon.CDK.AWS.EFS.FileSystem(this, "pipelineEFS", new FileSystemProps{
          Vpc = vpc,
          ThroughputMode = ThroughputMode.PROVISIONED,
          ProvisionedThroughputPerSecond = Size.Mebibytes(30),
          PerformanceMode = PerformanceMode.GENERAL_PURPOSE,
          RemovalPolicy = RemovalPolicy.DESTROY,
          Encrypted = false,
          SecurityGroup = secGroup
      });

      pipelineEFSAccessPoint = new AccessPoint(this, "pipelineEFSAccessPoint", new AccessPointProps{
          FileSystem = pipelineEFS,
          PosixUser = new PosixUser { Gid = "1000", Uid = "1000" },
          CreateAcl = new Acl { OwnerUid = "1000", OwnerGid = "1000", Permissions = "0777" },
          Path = "/efs"
      });
      pipelineEFSAccessPoint.Node.AddDependency(pipelineEFS);

      volume = new Amazon.CDK.AWS.ECS.Volume();
      volume.EfsVolumeConfiguration = new EfsVolumeConfiguration{
          FileSystemId = pipelineEFS.FileSystemId,
          AuthorizationConfig = new AuthorizationConfig{
              AccessPointId = pipelineEFSAccessPoint.AccessPointId,
              Iam = "ENABLED"
          },
          TransitEncryption = "ENABLED"
      };
      volume.Name = "efsVolume";
    }

    private void CreateStorage()
    {
      bucket = new Bucket(this, "dataBucket", new BucketProps{
          Versioned = true,
          RemovalPolicy = RemovalPolicy.DESTROY,
          AutoDeleteObjects = true
      });


      samplesTable = new Table(this, "heronSamplesTable", new TableProps{
          BillingMode = BillingMode.PAY_PER_REQUEST,
          PartitionKey = new Attribute { Name = "cogUkId", Type = AttributeType.STRING},
          SortKey = new Attribute { Name = "runCompleteDate", Type = AttributeType.NUMBER},
          PointInTimeRecovery = true
      });

      samplesTable.AddGlobalSecondaryIndex(new GlobalSecondaryIndexProps {
          IndexName = "lastChangedDate",
          PartitionKey = new Attribute { Name = "cogUkId", Type = AttributeType.STRING},
          SortKey = new Attribute { Name = "lastChangedDate", Type = AttributeType.NUMBER},
          ProjectionType = ProjectionType.ALL
      });

      sequencesTable = new Table(this, "heronSequencesTable", new TableProps {
          BillingMode = BillingMode.PAY_PER_REQUEST,
          PartitionKey = new Attribute { Name = "seqHash", Type = AttributeType.STRING},
          PointInTimeRecovery = true
      });
    }

    private void CreateQueues()
    {
      dailyProcessingQueue = new Queue(this, "dailyProcessingQueue", new QueueProps {
          ContentBasedDeduplication = true,
          Fifo = true,
          FifoThroughputLimit = FifoThroughputLimit.PER_MESSAGE_GROUP_ID,
          DeduplicationScope = DeduplicationScope.MESSAGE_GROUP
      });

      reprocessingQueue = new Queue(this, "reprocessingQueue", new QueueProps {
          ContentBasedDeduplication = true,
          Fifo = true,
          FifoThroughputLimit = FifoThroughputLimit.PER_MESSAGE_GROUP_ID,
          DeduplicationScope = DeduplicationScope.MESSAGE_GROUP
      });
    }
  }
}