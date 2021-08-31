using Amazon.CDK;
using Amazon.CDK.AWS.S3;

namespace HeronPipeline
{
    public class HeronPipelineStack : Stack
    {
        internal HeronPipelineStack(Construct scope, string id, IStackProps props = null) : base(scope, id, props)
        {
            // The code that defines your stack goes here
            new Bucket(this, "dataBucket", new BucketProps{
              Versioned = true,
              RemovalPolicy = RemovalPolicy.DESTROY,
              AutoDeleteObjects = true
            });
        }
    }
}
