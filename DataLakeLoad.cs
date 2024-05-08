using System;
using System.Text;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;
using Newtonsoft.Json;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.EventHubs;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using Azure;
using Azure.Storage;
using Microsoft.Extensions.Logging;
using ADLSGen2Helper;
using Microsoft.Azure.WebJobs.Hosting;
using ParamBinding;
using Newtonsoft.Json.Linq;
using Polly;

namespace LoadFunctions
{
    public class Startup : IWebJobsStartup
    {
        public void Configure(IWebJobsBuilder builder)
        {
            builder.AddExtension<ParamExtension>();
        }
    }

    public class DataLakeCatalogTrigger
    {
        public string dataLakeStoreFileGuid;
        public string fileSystemName;
        public string dataLakeStoreFilePath;
        public string sender;
        public string application;
        public string sourceType;
        public string delimiter;
        public string ingestiontype;
        public string dataLakeStoreFolderPath;
    }

    public static partial class JsonExtensions
    {
        public static void ToNewlineDelimitedJson<T>(Stream stream, IEnumerable<T> items)
        {
            // Let caller dispose the underlying stream
            using (var textWriter = new StreamWriter(stream, new UTF8Encoding(false, true), 1024, true))
            {
                ToNewlineDelimitedJson(textWriter, items);
            }
        }

        public static void ToNewlineDelimitedJson<T>(TextWriter textWriter, IEnumerable<T> items)
        {
            var serializer = JsonSerializer.CreateDefault();

            foreach (var item in items)
            {
                // Formatting.None is the default; I set it here for clarity.
                using (var writer = new JsonTextWriter(textWriter) { Formatting = Formatting.None, CloseOutput = false })
                {
                    serializer.Serialize(writer, item);
                }
                // https://web.archive.org/web/20180513150745/http://specs.okfnlabs.org/ndjson/
                // Each JSON text MUST conform to the [RFC7159] standard and MUST be written to the stream followed by the newline character \n (0x0A).
                // The newline charater MAY be preceeded by a carriage return \r (0x0D). The JSON texts MUST NOT contain newlines or carriage returns.
                textWriter.Write("\n");
            }
        }
    }

    public static class DataLakeLoad
    {
        public static DataLakeServiceClient GetDataLakeServiceClient(string accountName, string accountKey)
        {
            StorageSharedKeyCredential sharedKeyCredential =
                new StorageSharedKeyCredential(accountName, accountKey);

            string dfsUri = "https://" + accountName + ".dfs.core.windows.net";

            return new DataLakeServiceClient
                (new Uri(dfsUri), sharedKeyCredential);
        }

        public static async Task UploadFile(DataLakeFileSystemClient fileSystemClient, string directory, string name, string content)
        {
            DataLakeDirectoryClient directoryClient =
                fileSystemClient.GetDirectoryClient(directory);

            DataLakeFileClient fileClient = await directoryClient.CreateFileAsync(name);

            long fileSize = content.Length;
            var stream = new MemoryStream();
            var writer = new StreamWriter(stream);
            writer.Write(content);
            writer.Flush();
            stream.Position = 0;
            fileClient.Upload(stream, new DataLakeFileUploadOptions(), new  System.Threading.CancellationToken());
        }

        /// <summary>
        /// Writes an event hub message to the datalake.
        /// </summary>
        /// <param name="eventHubMessage">Event hub object triggering the function.</param>
        /// <param name="ILogger">Object used for azure functions logging.</param>
        /// <returns>
        /// None.
        /// </returns>
        [FunctionName("data-lake-load")]
        public static async Task Run(
            [EventHubTrigger(
                "test-endpoint-feelslike",
                Connection = "EventHubConnectionAppSetting",
                ConsumerGroup = "test"
            )] EventData[] eventHubMessageList,
            [Queue(
                "data-lake-catalog-trigger",
                Connection = "DataLakeAccountConnectionString"
            )] ICollector<DataLakeCatalogTrigger> queueCollector,
            [Param("n1")]string sourceName,
            [Param("global")]string sourceRegion,
            [Param("internal")]string sourceSecurityLevel,
            [Param("property")]string sourceSubjectArea,
            [Param("delimiter")]string delimiter,
            [Param("ingestiontype")]string ingestiontype,
            ILogger log
        )
        {
            var policy = Policy.Handle<Exception>().WaitAndRetry(5,
                attempt => TimeSpan.FromSeconds(0.1 * Math.Pow(2, attempt)),
                (exception, calculatedWaitDuration) =>
                {
                    log.LogWarning("Failed to write to queue.");
                    log.LogWarning($"Exception: {exception.Message}");
                });
            var policyAsync = Policy.Handle<Exception>().WaitAndRetryAsync(5,
                attempt => TimeSpan.FromSeconds(0.1 * Math.Pow(2, attempt)),
                (exception, calculatedWaitDuration) =>
                {
                    log.LogWarning("Failed to write to lake.");
                    log.LogWarning($"Exception: {exception.Message}");
                });

            // Construct file path for data lake write
            var storageAccountName = System.Environment.GetEnvironmentVariable("StorageAccountName");
            var fileSystemName = "raw";
            var storageAccountKey = System.Environment.GetEnvironmentVariable("StorageAccountKey");
            var applicationName = System.Environment.GetEnvironmentVariable("Application");
            var dt = DateTime.Now;
            string timeSlice = $"udp_year={dt:yyyy}/udp_month={dt:MM}/udp_day={dt:dd}/udp_hour={dt:HH}/udp_minute={dt:mm}";
            string dataLakeStoreFolderPath = $"/{sourceRegion}/{sourceSecurityLevel}/{sourceSubjectArea}/{sourceName}/{timeSlice}/";
            string dataLakeStoreFileGuid = Guid.NewGuid().ToString();
            string dataLakeStoreFilePath = Path.Combine(dataLakeStoreFolderPath, dataLakeStoreFileGuid);

            try {
                await policyAsync.ExecuteAsync(async () => {
                    List<JObject> messageBodyList = eventHubMessageList.Select(a => JObject.Parse(Encoding.UTF8.GetString(a.Body))).ToList();
                    // string messageBody = $"[{String.Join(",", messageBodyList)}]";
                    var sb = new StringBuilder();
                    using (var textWriter = new StringWriter(sb))
                    {
                        JsonExtensions.ToNewlineDelimitedJson(textWriter, messageBodyList);
                    }
                    var messageBody = sb.ToString();

                    //Connect to Data Lake Store using API client library
                    log.LogInformation("Creating ADLS client.");
                    var client = GetDataLakeServiceClient(storageAccountName, storageAccountKey);
                    var fsClient = client.GetFileSystemClient(fileSystemName);

                    // Perform asynchronous write to data lake store and await completion
                    log.LogInformation($"Writing message to data lake at path {dataLakeStoreFilePath}");

                    await UploadFile(fsClient, dataLakeStoreFolderPath, dataLakeStoreFileGuid, messageBody);
                    log.LogInformation($"Message written to data lake at path {dataLakeStoreFilePath}");
                });

                policy.Execute(() => {
                    log.LogInformation("Sending message to queue.");
                    DataLakeCatalogTrigger triggerMessage = new DataLakeCatalogTrigger();
                    string application = System.Environment.GetEnvironmentVariable("Application");
                    triggerMessage.dataLakeStoreFileGuid = dataLakeStoreFileGuid;
                    triggerMessage.fileSystemName = fileSystemName;
                    triggerMessage.dataLakeStoreFilePath = dataLakeStoreFilePath;
                    triggerMessage.sender = application + '-' + sourceName;
                    triggerMessage.application = application;
                    triggerMessage.sourceType = "Event";
                    triggerMessage.delimiter = delimiter;
                    triggerMessage.ingestiontype = ingestiontype;
                    triggerMessage.dataLakeStoreFolderPath = dataLakeStoreFolderPath;
                    queueCollector.Add(triggerMessage);
                });
            }
            catch (Exception e) {
                log.LogError("Failed after retry. Error message incoming.");
                log.LogError(e.Message);
                throw e;
            }
        }
    }
}
