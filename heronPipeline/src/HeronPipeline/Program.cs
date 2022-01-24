using Amazon.CDK;
using System;
using System.Collections.Generic;
using System.Linq;

namespace HeronPipeline
{
    sealed class Program
    {
        public static void Main(string[] args)
        {
            var app = new App();
            var pipeline = new HeronPipelineStack(app, "HeronProdStack", new StackProps
            {
                
            });

            Tags.Of(pipeline).Add("service-class", "prod");
            Tags.Of(pipeline).Add("pi", "sp31@sanger.ac.uk");

            var testPipeline = new HeronPipelineStack(app, "HeronTestStack", new StackProps
            {
                
            });

            Tags.Of(testPipeline).Add("service-class", "test");
            Tags.Of(testPipeline).Add("pi", "sp31@sanger.ac.uk");

            var devPipeline = new HeronPipelineStack(app, "HeronDevStack", new StackProps
            {
                
            });

            Tags.Of(devPipeline).Add("service-class", "dev");
            Tags.Of(devPipeline).Add("pi", "sp31@sanger.ac.uk");

            app.Synth();
        }
    }
}
