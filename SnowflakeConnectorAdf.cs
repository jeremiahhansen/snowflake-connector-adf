using System;
using System.IO;
using System.Data;
using System.Threading.Tasks;
using System.Text.RegularExpressions;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Snowflake.Data.Client;
using Microsoft.WindowsAzure.Storage;

namespace Snowflake.Connector
{
    public static class SnowflakeConnectorAdf
    {
        [FunctionName("SnowflakeConnectorAdf")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation($"Started at: {DateTime.Now.ToString()} (UTC).");

            // Read the POST body and convert to a JSON object
            string requestBodyString = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic requestBody = JsonConvert.DeserializeObject(requestBodyString,
                new JsonSerializerSettings() { DateParseHandling = DateParseHandling.None });

            // Get the required inputs and validate them
            #region Collect and validate inputs
            string snowflakeConnectionString = System.Environment.GetEnvironmentVariable("snowflakeConnectionString");
            if (String.IsNullOrEmpty(snowflakeConnectionString))
            {
                throw new Exception("snowflakeConnectionString must be provided");
            }
            string storageAccountConnectionString = System.Environment.GetEnvironmentVariable("storageAccountConnectionString");
            if (String.IsNullOrEmpty(storageAccountConnectionString))
            {
                throw new Exception("storageAccountConnectionString must be provided");
            }
            string storageAccountContainerName = System.Environment.GetEnvironmentVariable("storageAccountContainerName");
            if (String.IsNullOrEmpty(storageAccountContainerName))
            {
                throw new Exception("storageAccountContainerName must be provided");
            }
            string databaseName = Convert.ToString(requestBody.databaseName);
            if (String.IsNullOrEmpty(databaseName))
            {
                throw new Exception("databaseName must be provided");
            }
            string schemaName = Convert.ToString(requestBody.schemaName);
            if (String.IsNullOrEmpty(schemaName))
            {
                throw new Exception("schemaName must be provided");
            }
            string storedProcedureName = Convert.ToString(requestBody.storedProcedureName);
            if (String.IsNullOrEmpty(storedProcedureName))
            {
                throw new Exception("storedProcedureName must be provided");
            }
            #endregion Collect and validate inputs

            // Generate the blob file path to the stored procedure
            string storageAccountBlobFilePath = generateStoredProcedureBlobFilePath(databaseName, schemaName, storedProcedureName);

            // Get the SQL query to execute
            Task<string> blobReadTask = readContentFromBlobAsync(log, storageAccountConnectionString, storageAccountContainerName, storageAccountBlobFilePath);
            string sqlText = blobReadTask.GetAwaiter().GetResult();
            sqlText = sqlText.Trim();

            // Replace all parameter placeholders, if they've been defined
            if (requestBody.ContainsKey("parameters"))
            {
                sqlText = replaceSqlParameterPlaceholders(sqlText, requestBody.parameters);
            }

            // At this point, there should be no more /*Parameter*/ or /*Parameter_With_Quotes*/ values.
            if (sqlText.Contains("/*Parameter*/") || sqlText.Contains("/*Parameter_With_Quotes*/"))
            {
                throw new Exception("There are placeholders left over in sqlText after replacement: " + sqlText);
            }

            // Split sqlText into individual queries since we can only run one query at a time against Snowflake
            string[] sqlCommands = splitSqlCommands(sqlText);
            log.LogInformation($"Found {sqlCommands.Length} queries to execute");

            // Run the Snowflake SQL commands
            dynamic output = new JObject();
            output.customOutput = runSnowflakeSqlCommands(log, snowflakeConnectionString, sqlCommands);

            // Return the result JSON object
            log.LogInformation($"Completed successfully at: {DateTime.Now.ToString()} (UTC).");
            return new JsonResult(output);
        }

        /// <summary>
        /// Generate a stored procedure blob file path
        /// </summary>
        /// <param name="databaseName">The database that the stored procedure belongs to</param>
        /// <param name="schemaName">The schema that the stored procedure belongs to</param>
        /// <param name="storedProcedureName">The stored procedure's name</param>
        /// <returns>The blob file path to the stored procedure</returns>
        private static string generateStoredProcedureBlobFilePath(string databaseName, string schemaName, string storedProcedureName)
        {
            return String.Format("{0}/{1}/{2}.sql", databaseName, schemaName, storedProcedureName);
        }

        /// <summary>
        /// Read the contents of a file from Azure Blob Storage
        /// </summary>
        /// <param name="log">ILogger object</param>
        /// <param name="storageAccountConnectionString">The storage account connection string</param>
        /// <param name="storageAccountContainerName">The storage account container name</param>
        /// <param name="storageAccountBlobFilePath">The blob file path to the stored procedure</param>
        /// <returns>The contents of the file as a string</returns>
        private static async Task<string> readContentFromBlobAsync(ILogger log, string storageAccountConnectionString, string storageAccountContainerName, string storageAccountBlobFilePath)
        {
            log.LogInformation($"Getting content from blob file: {storageAccountBlobFilePath}");

            var storageAccount = CloudStorageAccount.Parse(storageAccountConnectionString);
            var myClient = storageAccount.CreateCloudBlobClient();
            var container = myClient.GetContainerReference(storageAccountContainerName);
            var blockBlob = container.GetBlockBlobReference(storageAccountBlobFilePath);
            return await blockBlob.DownloadTextAsync();
        }

        /// <summary>
        /// Replace all parameter placeholders in the SQL text
        /// </summary>
        /// <param name="sqlText">The SQL text to be updated</param>
        /// <param name="parameters">JSON object containing the parameters</param>
        /// <returns>The SQL text with all parameters replaced</returns>
        private static string replaceSqlParameterPlaceholders(string sqlText, dynamic parameters)
        {
            foreach (var param in parameters)
            {
                // Guard aginst SQL Injection attacks
                if (param.Value.ToString().Contains(";"))
                {
                    throw new Exception($"Found a semicolon in the value for parameter {param.Name}");
                }

                var paramStringWithQuotes = @"\/\*Parameter_With_Quotes\*\/.*\/\*" + param.Name + @"\*\/";
                var paramString = @"\/\*Parameter\*\/.*\/\*" + param.Name + @"\*\/";
                sqlText = Regex.Replace(sqlText, paramStringWithQuotes, "'" + param.Value.ToString() + "'");
                sqlText = Regex.Replace(sqlText, paramString, param.Value.ToString());
            }

            return sqlText;
        }

        /// <summary>
        /// Split the SQL text into individual commands
        /// </summary>
        /// <param name="sqlText">The SQL text to be split</param>
        /// <returns>An array of SQL commands</returns>
        private static string[] splitSqlCommands(string sqlText)
        {
            // Split sqlText on the query separator
            // If semicolon (;) doesn't work then try using the /*Sql_Query_Separator*/ comment
            string[] sqlCommands = sqlText.Split(new[] { ";" }, StringSplitOptions.RemoveEmptyEntries);

            return sqlCommands;
        }

        /// <summary>
        /// Run SQL commands in Snowflake and return a JSON object with column/value pairs from first row of the result.
        /// See https://github.com/snowflakedb/snowflake-connector-net for more details.
        /// </summary>
        /// <param name="log">ILogger object</param>
        /// <param name="snowflakeConnectionString">Snowflake connection string</param>
        /// <param name="sqlCommands">The SQL commands to execute</param>
        /// <returns>JSON object with column/value pairs from first row of the result</returns>
        private static JObject runSnowflakeSqlCommands(ILogger log, string snowflakeConnectionString, string[] sqlCommands)
        {
            var output = new JObject();

            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                // Connect to Snowflake
                conn.ConnectionString = snowflakeConnectionString;
                conn.Open();

                using (IDbCommand cmd = conn.CreateCommand())
                {
                    // Run every query except the last one using ExecuteNonQuery()
                    for (int i = 0; i < sqlCommands.Length - 1; i++)
                    {
                        cmd.CommandText = sqlCommands[i].Trim();
                        log.LogInformation($"Running SQL command #{i}: {cmd.CommandText}");
                        cmd.ExecuteNonQuery();
                    }

                    // Finally run the last query using ExecuteReader() so we can collect the output
                    cmd.CommandText = sqlCommands[sqlCommands.Length - 1].Trim();
                    log.LogInformation($"Running final SQL command: {cmd.CommandText}");
                    IDataReader reader = cmd.ExecuteReader();

                    // The result should be a table with one row and n columns, format the column/value pairs in JSON
                    while (reader.Read())
                    {
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            var columnName = reader.GetName(i);
                            var value = reader[i].ToString();
                            output.Add(columnName, value);
                        }
                    }
                }

                conn.Close();
            }

            return output;
        }
    }
}
