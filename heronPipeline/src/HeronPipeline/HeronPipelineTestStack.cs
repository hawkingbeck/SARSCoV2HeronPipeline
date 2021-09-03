
using System;
using Amazon.CDK;
using Amazon.CDK.AWS.S3;

namespace HeronPipeline
{
    public class HeronPipelineTestStack: Stack
    {
        internal HeronPipelineTestStack(Construct scope, string id, IStackProps props = null) : base(scope, id, props)
        {
            new Bucket(this, "testDataBucket", new BucketProps{
                Versioned = false,
                RemovalPolicy = RemovalPolicy.DESTROY,
                AutoDeleteObjects = true
            });

            
        }
    }
}
