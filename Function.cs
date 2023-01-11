using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Util;
using System.Net;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace S3TriggerExample
{
    public class Function
    {
        IAmazonS3 S3Client { get; set; }

        /// <summary>
        /// Default constructor. This constructor is used by Lambda to construct the instance. When invoked in a Lambda environment
        /// the AWS credentials will come from the IAM role associated with the function and the AWS region will be set to the
        /// region the Lambda function is executed in.
        /// </summary>
        public Function()
        {
            S3Client = new AmazonS3Client();
        }

        /// <summary>
        /// Constructs an instance with a preconfigured S3 client. This can be used for testing the outside of the Lambda environment.
        /// </summary>
        /// <param name="s3Client"></param>
        public Function(IAmazonS3 s3Client)
        {
            this.S3Client = s3Client;
        }
        
        /// <summary>
        /// This method is called for every Lambda invocation. This method takes in an S3 event object and can be used 
        /// to respond to S3 notifications.
        /// </summary>
        /// <param name="evnt"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task<string> FunctionHandler(S3Event evnt, ILambdaContext context)
        {
            var s3Event = evnt.Records?[0].S3;
            MemoryStream ms = null;
            if(s3Event == null)
            {
                return null;
            }

            try
            {
                context.Logger.LogLine(evnt.ToString());
                context.Logger.LogLine("Lambda Entered");
                context.Logger.LogLine("Bucket Name "+ s3Event.Bucket.Name +" Key "+ s3Event.Object.Key);

                //string filepath = "tmp";
                //this.S3Client.DownloadToFilePathAsync(s3Event.Bucket.Name,
                //    s3Event.Object.Key,
                //    filepath+s3Event.Object.Key);
                string input = "";
                GetObjectRequest getObjectRequest = new GetObjectRequest
                {
                    BucketName = s3Event.Bucket.Name,
                    Key = s3Event.Object.Key
                };

                string line = "";
                using (var res = await this.S3Client.GetObjectAsync(getObjectRequest))
                {
                    if (res.HttpStatusCode == HttpStatusCode.OK)
                    {
                        context.Logger.LogLine("Status OK");
                        //using (ms = new MemoryStream())
                        //{
                        //    await res.ResponseStream.CopyToAsync(ms);
                        //}
                        StreamReader reader = new StreamReader(res.ResponseStream);
                        while ((line = reader.ReadLine()) != null)
                        {
                            Console.WriteLine(line);
                           // csv.Add(line.Split(','));
                        }
                    }
                    //StreamReader reader = new StreamReader(res.ResponseStream);
                    //input=reader.ReadLine();
                    //context.Logger.LogLine(input);
                }
                
                //if (ms is null || ms.ToArray().Length < 1)
                //    throw new FileNotFoundException(string.Format("The document is not found"));
                //byte[] b = ms.ToArray();

                var response = await this.S3Client.GetObjectMetadataAsync(s3Event.Bucket.Name, s3Event.Object.Key);
                return response.Headers.ContentType;
            }
            catch(Exception e)
            {
                context.Logger.LogLine($"Error getting object {s3Event.Object.Key} from bucket {s3Event.Bucket.Name}. Make sure they exist and your bucket is in the same region as this function.");
                context.Logger.LogLine(e.Message);
                context.Logger.LogLine(e.StackTrace);
                throw;
            }
        }
    }
}
