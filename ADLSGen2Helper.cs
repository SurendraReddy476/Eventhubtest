using StorageRestApiAuth;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace ADLSGen2Helper
{
    public interface IADLSClient
    {
        Task<string> CreateFileSystem(string fileSystemName);
        Task CreateFile(string fileSystemName, string dataLakeStoreFilePath, string myBlob, string contentType = "text/plain; charset=UTF-8");
        Task<Stream> ReadFileContent(string filesystem, string filepath, string range = null);

    }

    public class ADLSClient : IADLSClient
    {
        private string StorageAccountName;
        private string StorageAccountKey;
        private string BaseUri;
        private string ApiVersion = "2018-11-09";
        public ADLSClient(string storageAccountName, string storageAccountKey)
        {
            StorageAccountName = storageAccountName;
            StorageAccountKey = storageAccountKey;
            BaseUri = $"https://{StorageAccountName}.dfs.core.windows.net/";
        }

        public async Task<String> CreateFileSystem(string filesystem)
        {
            filesystem = filesystem.Trim('/');
            using (var httpRequestMessage = new HttpRequestMessage(HttpMethod.Put, $"{BaseUri}{filesystem}?resource=filesystem"))
            {
                DateTime now = DateTime.UtcNow;
                var dateString = now.ToString("R", CultureInfo.InvariantCulture);
                httpRequestMessage.Headers.Add("x-ms-date", dateString);
                httpRequestMessage.Headers.Add("x-ms-version", ApiVersion);

                var header = AzureStorageAuthenticationHelper.GetAuthorizationHeader(StorageAccountName, StorageAccountKey, now, httpRequestMessage);

                httpRequestMessage.Headers.Authorization = header;

                HttpClient client = new HttpClient();

                var response = await client.SendAsync(httpRequestMessage);

                string responseString = await response.Content.ReadAsStringAsync();

                client.Dispose();

                return responseString;

                
            }
        }

        public async Task<String> ListFileSystemsAsync()
        {

            using (var httpRequestMessage = new HttpRequestMessage(HttpMethod.Get, $"{BaseUri}?resource=account"))
            {
                DateTime now = DateTime.UtcNow;
                var dateString = now.ToString("R", CultureInfo.InvariantCulture);
                httpRequestMessage.Headers.Add("x-ms-date", dateString);
                httpRequestMessage.Headers.Add("x-ms-version", ApiVersion);

                var header = AzureStorageAuthenticationHelper.GetAuthorizationHeader(StorageAccountName, StorageAccountKey, now, httpRequestMessage);

                httpRequestMessage.Headers.Authorization = header;

                HttpClient client = new HttpClient();

                var response = await client.SendAsync(httpRequestMessage);

                string responseString = await response.Content.ReadAsStringAsync();

                client.Dispose();

                return responseString;

                
            }
        }

        public async Task CreatePath(string filesystem, string path)
        {
            using (var httpRequestMessage = new HttpRequestMessage(HttpMethod.Put, $"{BaseUri}{filesystem}/{path}?resource=directory"))
            {
                DateTime now = DateTime.UtcNow;
                var dateString = now.ToString("R", CultureInfo.InvariantCulture);
                httpRequestMessage.Headers.Add("x-ms-date", dateString);
                httpRequestMessage.Headers.Add("x-ms-version", ApiVersion);

                var header = AzureStorageAuthenticationHelper.GetAuthorizationHeader(StorageAccountName, StorageAccountKey, now, httpRequestMessage);

                httpRequestMessage.Headers.Authorization = header;

                HttpClient client = new HttpClient();

                var response = await client.SendAsync(httpRequestMessage);

                string responseString = await response.Content.ReadAsStringAsync();

                client.Dispose();

                
            }
        }

        public async Task CreateFile(string filesystem, string path, string fileContent, string contentType = "text/plain; charset=UTF-8")
        {
            // Create file
            var concatPath = path.Trim('/');
            using (var httpRequestMessage = new HttpRequestMessage(HttpMethod.Put, $"{BaseUri}{filesystem}/{concatPath}?resource=file"))
            {
                DateTime now = DateTime.UtcNow;
                var dateString = now.ToString("R", CultureInfo.InvariantCulture);
                httpRequestMessage.Headers.Add("x-ms-date", dateString);
                httpRequestMessage.Headers.Add("x-ms-version", ApiVersion);

                var header = AzureStorageAuthenticationHelper.GetAuthorizationHeader(StorageAccountName, StorageAccountKey, now, httpRequestMessage);

                httpRequestMessage.Headers.Authorization = header;

                HttpClient client = new HttpClient();

                var response = await client.SendAsync(httpRequestMessage);

                string responseString = await response.Content.ReadAsStringAsync();

                client.Dispose();
            }

            // Upload data to file 
            string contentLength;
            using (var httpRequestMessage = new HttpRequestMessage(HttpMethod.Patch, $"{BaseUri}{filesystem}/{concatPath}?action=append&position=0"))
            {
                DateTime now = DateTime.UtcNow;
                var dateString = now.ToString("R", CultureInfo.InvariantCulture);
                httpRequestMessage.Headers.Add("x-ms-date", dateString);
                httpRequestMessage.Headers.Add("x-ms-version", ApiVersion);
                var content = new StringContent(fileContent);
                httpRequestMessage.Content = content;
                contentLength = content.Headers.ContentLength.ToString();
                var header = AzureStorageAuthenticationHelper.GetAuthorizationHeader(StorageAccountName, StorageAccountKey, now, httpRequestMessage, contentType: content.Headers.ContentType.ToString());

                httpRequestMessage.Headers.Authorization = header;

                HttpClient client = new HttpClient();

                var response = await client.SendAsync(httpRequestMessage);
                
                string responseString = await response.Content.ReadAsStringAsync();
                
                client.Dispose();

            }

            // Flush file
            using (var httpRequestMessage = new HttpRequestMessage(HttpMethod.Patch, $"{BaseUri}{filesystem}/{concatPath}?action=flush&position={contentLength}"))
            {
                DateTime now = DateTime.UtcNow;
                var dateString = now.ToString("R", CultureInfo.InvariantCulture);
                httpRequestMessage.Headers.Add("x-ms-date", dateString);
                httpRequestMessage.Headers.Add("x-ms-version", ApiVersion);
                var header = AzureStorageAuthenticationHelper.GetAuthorizationHeader(StorageAccountName, StorageAccountKey, now, httpRequestMessage);

                httpRequestMessage.Headers.Authorization = header;

                HttpClient client = new HttpClient();

                var response = await client.SendAsync(httpRequestMessage);

                string responseString = await response.Content.ReadAsStringAsync();

                client.Dispose();
            }
        }

        public async Task<Stream> ReadFileContent(string filesystem, string filepath, string range = null)
        {
            string trim_filepath = filepath.TrimStart('/');
            var getUrl = $"{BaseUri}{filesystem}/{trim_filepath}";
            using (var httpRequestMessage = new HttpRequestMessage(HttpMethod.Get, getUrl))
            {
                DateTime now = DateTime.UtcNow;
                var dateString = now.ToString("R", CultureInfo.InvariantCulture);
                httpRequestMessage.Headers.Add("x-ms-date", dateString);
                httpRequestMessage.Headers.Add("x-ms-version", ApiVersion);
                if (!String.IsNullOrWhiteSpace(range))
                {
                    httpRequestMessage.Headers.Add("Range", range);
                }

                var header = AzureStorageAuthenticationHelper.GetAuthorizationHeader(StorageAccountName, StorageAccountKey, now, httpRequestMessage);

                httpRequestMessage.Headers.Authorization = header;

                HttpClient client = new HttpClient();

                var response = await client.SendAsync(httpRequestMessage);
                response.EnsureSuccessStatusCode();
                Stream responseStream = await response.Content.ReadAsStreamAsync();

                client.Dispose();

                return responseStream;

                
            }
        }
    }
}
