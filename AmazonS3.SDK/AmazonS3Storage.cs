using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using Amazon.Runtime;
using Amazon.S3.IO;
using System.Configuration;

namespace AmazonS3.SDK
{
    public static class AmazonS3Storage
    {
        static IAmazonS3 s3Client = new AmazonS3Client(new BasicAWSCredentials("AKIAI5ZEH25NWLMRLIOQ", "ZtZOiFpo9wRHAgHbKA406yAiIlUPvKLAy1CNFPXq"), Amazon.RegionEndpoint.USEast1);

        /* 1. Create A Folder*/
              public static void CreateFolder(string bucketName, string folderName)
        {
            try
            {
                PutObjectRequest putRequest1 = new PutObjectRequest
                {
                    BucketName = bucketName,
                    Key = folderName + "/"
                };

                PutObjectResponse response1 = s3Client.PutObject(putRequest1);
            }
            catch (AmazonS3Exception amazonS3Exception)
            {
                if (amazonS3Exception.ErrorCode != null &&
                    (amazonS3Exception.ErrorCode.Equals("InvalidAccessKeyId")
                    ||
                    amazonS3Exception.ErrorCode.Equals("InvalidSecurity")))
                {
                    Console.WriteLine("Check the provided AWS Credentials.");
                    Console.WriteLine(
                        "For service sign up go to http://aws.amazon.com/s3");
                }
                else
                {
                    Console.WriteLine(
                        "Error occurred. Message:'{0}' when writing an object"
                        , amazonS3Exception.Message);
                }
            }
        }

        /* 2. Upload an object*/
        public static void UploadFileToS3(string folderName, string FileName, string FilePath)
        {
            try
            {
                string bucketName = ConfigurationManager.AppSettings.Get("TAKATANET_AMAZON_S3_BUCKETNAME").ToString();
                TransferUtility utility = new TransferUtility(s3Client);
                utility.Upload(FilePath, bucketName, folderName + "/" + FileName);
            }
            catch (AmazonS3Exception amazonS3Exception)
            {
                if (amazonS3Exception.ErrorCode != null &&
                    (amazonS3Exception.ErrorCode.Equals("InvalidAccessKeyId")
                    ||
                    amazonS3Exception.ErrorCode.Equals("InvalidSecurity")))
                {
                    Console.WriteLine("Check the provided AWS Credentials.");
                    Console.WriteLine(
                        "For service sign up go to http://aws.amazon.com/s3");
                }
                else
                {
                    Console.WriteLine(
                        "Error occurred. Message:'{0}' when writing an object"
                        , amazonS3Exception.Message);
                }
            }
        }

        /* 3. Delete an Object */

        public static void DeleteFileFromS3(string folderName, string FileName)
        {
            try
            {
                ListObjectsRequest request = new ListObjectsRequest();
                request.BucketName = "takatanet-test-bucket";
                request.Prefix = string.IsNullOrEmpty(FileName) ? folderName + "/" : folderName + "/" + FileName;
                ListObjectsResponse response = s3Client.ListObjects(request);
                List<KeyVersion> keys = new List<KeyVersion>();
                foreach (S3Object obj in response.S3Objects)
                {
                    KeyVersion keyVersion = new KeyVersion()
                    {
                        Key = obj.Key
                    };
                    keys.Add(keyVersion);
                    //S3FileInfo s3FileInfo = new Amazon.S3.IO.S3FileInfo(s3Client, request.BucketName, obj.Key);
                    //s3FileInfo.Delete();
                }
                MultiObjectDelete(keys);


                //DeleteObjectRequest objRequest = new DeleteObjectRequest()
                //{
                //    BucketName = "takatanet-test-bucket",
                //    Key = string.IsNullOrEmpty(FileName)? folderName + "/" : folderName + "/" + FileName
                //};
                //DeleteObjectResponse reponse = s3Client.DeleteObject(objRequest);
                //S3FileInfo s3FileInfo = new Amazon.S3.IO.S3FileInfo(s3Client, objRequest.BucketName, objRequest.Key);
                //s3FileInfo.Delete();
            }
            catch (AmazonS3Exception amazonS3Exception)
            {
                if (amazonS3Exception.ErrorCode != null &&
                    (amazonS3Exception.ErrorCode.Equals("InvalidAccessKeyId")
                    ||
                    amazonS3Exception.ErrorCode.Equals("InvalidSecurity")))
                {
                    Console.WriteLine("Check the provided AWS Credentials.");
                    Console.WriteLine(
                        "For service sign up go to http://aws.amazon.com/s3");
                }
                else
                {
                    Console.WriteLine(
                        "Error occurred. Message:'{0}' when writing an object"
                        , amazonS3Exception.Message);
                }
            }
        }

        private static void MultiObjectDelete(List<KeyVersion> keys)
        {
            // a. multi-object delete by specifying the key names and version IDs.
            DeleteObjectsRequest multiObjectDeleteRequest = new DeleteObjectsRequest
            {
                BucketName = "takatanet-test-bucket",
                Objects = keys // This includes the object keys and null version IDs.
            };
            multiObjectDeleteRequest.AddKey("AWSSDKcopy2.dll", null);
            try
            {
                DeleteObjectsResponse response = s3Client.DeleteObjects(multiObjectDeleteRequest);
                //Console.WriteLine("Successfully deleted all the {0} items", response.DeletedObjects.Count);
            }
            catch (DeleteObjectsException e)
            {
                //PrintDeletionReport(e);
            }
        }

        /* Download or Get an Object */

        public static MemoryStream DownloadFromS3(string folderName, string fileName1, string fileName2)
        {
            MemoryStream file = new MemoryStream();
            try
            {
                    GetObjectRequest request = new GetObjectRequest()
                    {
                        BucketName = "takatanet-test-bucket",
                        Key = folderName + "/" + fileName1 + fileName2
                    };
                    GetObjectResponse response = s3Client.GetObject(request);

                    BufferedStream stream2 = new BufferedStream(response.ResponseStream);
                    byte[] buffer = new byte[0x2000];
                    int count = 0;
                    while ((count = stream2.Read(buffer, 0, buffer.Length)) > 0)
                    {
                        file.Write(buffer, 0, count);
                    }


                    //byte[] byteArray = file.ToArray();
                    ////Clean up the memory stream
                    //file.Flush();
                    //file.Close();
                    //// Clear all content output from the buffer stream
                    //httpResponse.Clear();
                    //// Add a HTTP header to the output stream that specifies the default filename
                    //// for the browser's download dialog
                    //httpResponse.AddHeader("Content-Disposition", "attachment; filename=" + request.Key);
                    //// Add a HTTP header to the output stream that contains the 
                    //// content length(File Size). This lets the browser know how much data is being transfered
                    //httpResponse.AddHeader("Content-Length", byteArray.Length.ToString());
                    //// Set the HTTP MIME type of the output stream
                    //httpResponse.ContentType = "application/octet-stream";
                    //// Write the data out to the client.
                    //httpResponse.BinaryWrite(byteArray);
                    //httpResponse.OutputStream.Write(buffer, 0, buffer.Length);
                    return file;
            }
            catch (AmazonS3Exception)
            {
                //Show exception
            }
            return file;
        }

        public static bool DoesObjectExists(string bucketName, string FileName)
        {
            try
            {
                S3FileInfo s3FileInfo = new Amazon.S3.IO.S3FileInfo(s3Client, bucketName, FileName);
                return s3FileInfo.Exists;                
            }
            catch (Amazon.S3.AmazonS3Exception ex)
            {
                if (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
                    return false;

                //status wasn't not found, so throw the exception
                throw;
            }
        }
    }
}
