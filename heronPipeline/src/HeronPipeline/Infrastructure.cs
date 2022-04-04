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
    public Table mutationsTable;
    public Queue dailyProcessingQueue;
    public Queue reprocessingQueue;
    public Role ecsExecutionRole;
    public Cluster cluster;
    public PolicyStatement s3AccessPolicyStatement;
    public PolicyStatement sqsAccessPolicyStatement;
    public PolicyStatement dynamoDBAccessPolicyStatement;
    public PolicyStatement dynamoDBExportPolicyStatement;
    public Amazon.CDK.AWS.Lambda.FileSystem lambdaPipelineFileSystem;
    private Construct scope;
    private string id;
    private int provisionedThroughput;
    private SecurityGroup secGroup;

    
    public Infrastructure(Construct scope, string id): base(scope, id)
    {
      this.scope = scope;
      this.id = id;
    }

    public void Create()
    {
      CreateVPC();
      CreateStorage();
      CreateEFS();
      CreateQueues();
      CreateExecutionRole();
      CreateCluster();
      CreateAccessPolicies();
    }
    private void CreateVPC()
    {
      var numberOfAzs = 1;
      this.provisionedThroughput = 0;
      var CidrString = "";
      if (this.id == "HeronProdStack_infra_"){
        numberOfAzs = 3;
        CidrString = "12.0.0.0/16";
        this.provisionedThroughput = 30;
      }else if (this.id == "HeronTestStack_infra_"){
        numberOfAzs = 1;
        CidrString = "13.0.0.0/16";
        this.provisionedThroughput = 10;
      } else if (this.id == "HeronDevStack_infra_"){
        numberOfAzs = 1;
        CidrString = "14.0.0.0/16";
        this.provisionedThroughput = 10;
      }
      vpc = new Vpc(this, "vpc", new VpcProps{
                MaxAzs = numberOfAzs, ///TODO: Increase this once EIP's are freed
                Cidr = CidrString,
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
          ProvisionedThroughputPerSecond = Size.Mebibytes(this.provisionedThroughput),
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
      var fileSystemConfig = new FileSystemConfig();
      fileSystemConfig.Arn = pipelineEFSAccessPoint.AccessPointArn;
      fileSystemConfig.LocalMountPath = "/mnt/efs0";
      lambdaPipelineFileSystem = new Amazon.CDK.AWS.Lambda.FileSystem(fileSystemConfig);
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
          SortKey = new Attribute { Name = "runMetaData", Type = AttributeType.STRING},
          PointInTimeRecovery = true
      });

      sequencesTable = new Table(this, "heronSequencesTable", new TableProps {
          BillingMode = BillingMode.PAY_PER_REQUEST,
          PartitionKey = new Attribute { Name = "seqHash", Type = AttributeType.STRING},
          PointInTimeRecovery = true
      });

      mutationsTable = new Table(this, "heronMutationsTable", new TableProps {
          BillingMode = BillingMode.PAY_PER_REQUEST,
          PartitionKey = new Attribute { Name = "mutationId", Type = AttributeType.STRING},
          PointInTimeRecovery = true
      });
    }
    private void CreateQueues()
    {
      dailyProcessingQueue = new Queue(this, "daily", new QueueProps {
          ContentBasedDeduplication = true,
          Fifo = true,
          FifoThroughputLimit = FifoThroughputLimit.PER_MESSAGE_GROUP_ID,
          DeduplicationScope = DeduplicationScope.MESSAGE_GROUP
      });

      reprocessingQueue = new Queue(this, "reprocessing", new QueueProps {
          ContentBasedDeduplication = true,
          Fifo = true,
          FifoThroughputLimit = FifoThroughputLimit.PER_MESSAGE_GROUP_ID,
          DeduplicationScope = DeduplicationScope.MESSAGE_GROUP
      });
    }
    private void CreateExecutionRole()
    {
      ecsExecutionRole = new Role(this, this.id + "_fargateExecutionRole", new RoleProps{
          Description = "Role for fargate execution",
          AssumedBy = new ServicePrincipal("ec2.amazonaws.com"), //The service that needs to use this role
      });

      ecsExecutionRole.AddManagedPolicy(ManagedPolicy.FromAwsManagedPolicyName("AmazonEC2FullAccess"));
      ecsExecutionRole.AddManagedPolicy(ManagedPolicy.FromAwsManagedPolicyName("AmazonSQSFullAccess"));
      ecsExecutionRole.AddManagedPolicy(ManagedPolicy.FromAwsManagedPolicyName("AmazonS3FullAccess"));
      ecsExecutionRole.AddManagedPolicy(ManagedPolicy.FromAwsManagedPolicyName("AmazonDynamoDBFullAccess"));
      ecsExecutionRole.AddManagedPolicy(ManagedPolicy.FromManagedPolicyArn(this, this.id + "ecsExecutionRolePolicy", "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"));
      ecsExecutionRole.AddManagedPolicy(ManagedPolicy.FromAwsManagedPolicyName("CloudWatchEventsFullAccess"));


      var policyStatement = new PolicyStatement(new PolicyStatementProps{
          Effect = Effect.ALLOW,
          Actions = new string[] { "sts:AssumeRole" },
          Principals = new ServicePrincipal[] { new ServicePrincipal("ecs-tasks.amazonaws.com") }
      });

      ecsExecutionRole.AssumeRolePolicy.AddStatements(policyStatement);
    }

    private void CreateCluster()
    {
      cluster = new Cluster(this, "heronCluster", new ClusterProps{
          Vpc = vpc,
          EnableFargateCapacityProviders = true
      });
    }
    private void CreateAccessPolicies()
    {
      s3AccessPolicyStatement = new PolicyStatement(new PolicyStatementProps
      {
          Effect = Effect.ALLOW,
          Actions = new string[] { "s3:*" }
      });
      s3AccessPolicyStatement.AddResources(new string[] {
        bucket.BucketArn,
        bucket.BucketArn + "/*"
      });

      sqsAccessPolicyStatement = new PolicyStatement( new PolicyStatementProps {
        Effect = Effect.ALLOW,
        Actions = new string[] { "sqs:*"},
      });
      sqsAccessPolicyStatement.AddResources(new string[] {
        dailyProcessingQueue.QueueArn,
        reprocessingQueue.QueueArn
      });

      dynamoDBAccessPolicyStatement = new PolicyStatement(new PolicyStatementProps{
        Effect = Effect.ALLOW,
        Actions = new string[] {"dynamodb:*"}
      });
      dynamoDBAccessPolicyStatement.AddResources(new string[]{
        samplesTable.TableArn,
        sequencesTable.TableArn,
        mutationsTable.TableArn
      });

      dynamoDBExportPolicyStatement = new PolicyStatement(new PolicyStatementProps{
        Effect = Effect.ALLOW,
        Actions = new string[] {"dynamodb:*"},
        Resources = new string[] {"arn:aws:dynamodb:eu-west-1:889562587392:*"}
      });
    }
  }
}