/*
Copyright (c) 2019 Snowflake Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at:

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
 */
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
        private static string _validBlobFolderNameRegex = @"^[A-Za-z0-9_-]+$";
        // The parameter name corresponds to a restricted Snowflake unquoted identifier
        // https://docs.snowflake.net/manuals/sql-reference/identifiers-syntax.html
        private static string _validParameterNameRegex = @"^[A-Za-z_]{1}[A-Za-z0-9_-]*$";
        // This is pretty restrictive
        private static string _validParameterTypeRegex = @"^VARCHAR|NUMBER$";
        private static string _validParameterValueRegex = @"^[A-Za-z0-9./\\ ]+$";

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

            // Get the SQL text to execute
            Task<string> blobReadTask = readContentFromBlobAsync(log, storageAccountConnectionString, storageAccountContainerName, storageAccountBlobFilePath);
            string sqlText = blobReadTask.GetAwaiter().GetResult();
            sqlText = sqlText.Trim();
            if (sqlText.Length == 0)
            {
                throw new Exception($"Blob script {storageAccountBlobFilePath} is empty");
            }

            // Split the SQL text into individual queries since we can only run one query at a time against Snowflake
            string[] sqlCommands = splitSqlCommands(sqlText);

            // Convert any parameters to SQL variables
            string setVariableCommand = "";
            if (requestBody.ContainsKey("parameters"))
            {
                setVariableCommand = generateSetVariableCommand(requestBody.parameters);
            }

            // Run the Snowflake SQL commands
            JObject output = runSnowflakeSqlCommands(log, snowflakeConnectionString, setVariableCommand, sqlCommands);

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
            // Validate the blob path elements
            #region Validate inputs
            if (!Regex.IsMatch(databaseName, _validBlobFolderNameRegex))
            {
                throw new Exception($"Found invalid databaseName value: {databaseName}");
            }
            if (!Regex.IsMatch(schemaName, _validBlobFolderNameRegex))
            {
                throw new Exception($"Found invalid schemaName value: {schemaName}");
            }
            if (!Regex.IsMatch(storedProcedureName, _validBlobFolderNameRegex))
            {
                throw new Exception($"Found invalid storedProcedureName value: {storedProcedureName}");
            }
            #endregion Validate inputs

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
        /// Generate the Snowflake SQL command to set all variables for the script
        /// </summary>
        /// <param name="parameters">JSON object containing the parameters</param>
        /// <returns>The SQL command to set all variables</returns>
        private static string generateSetVariableCommand(dynamic parameters)
        {
            string snowflakeVariableNames = "";
            string snowflakeVariableValues = "";

            foreach (var param in parameters)
            {
                string parameterName = param.name.ToString();
                string parameterType = param.type.ToString();
                string parameterValue = param.value.ToString();

                // Validate the parameter data
                #region Validate inputs
                if (!Regex.IsMatch(parameterName, _validParameterNameRegex))
                {
                    throw new Exception($"Found invalid parameter name: {parameterName}");
                }
                if (!Regex.IsMatch(parameterType, _validParameterTypeRegex, RegexOptions.IgnoreCase))
                {
                    throw new Exception($"Found invalid parameter type for {parameterName}: {parameterType}");
                }
                if (!Regex.IsMatch(parameterValue, _validParameterValueRegex))
                {
                    throw new Exception($"Found invalid parameter value for {parameterName}: {parameterValue}");
                }
                #endregion Validate inputs

                // Add a new variable for this parameter
                snowflakeVariableNames += $"{parameterName},";
                switch (parameterType.ToUpper())
                {
                    case "VARCHAR":
                        snowflakeVariableValues += $"'{parameterValue}',";
                        break;
                    case "NUMBER":
                        snowflakeVariableValues += $"{parameterValue},";
                        break;
                    default:
                        throw new Exception($"Found invalid parameter type: {parameterType}");
                }
            }

            // Remove the trailing comma from each string and return the final SQL command
            char[] charactersToRemove = {','};
            snowflakeVariableNames = snowflakeVariableNames.TrimEnd(charactersToRemove);
            snowflakeVariableValues = snowflakeVariableValues.TrimEnd(charactersToRemove);
            return $"SET ({snowflakeVariableNames}) = ({snowflakeVariableValues})";
        }

        /// <summary>
        /// Split the SQL text into individual commands
        /// </summary>
        /// <param name="sqlText">The SQL text to be split</param>
        /// <returns>An array of SQL commands</returns>
        private static string[] splitSqlCommands(string sqlText)
        {
            // Split sqlText on the query separator
            string[] sqlCommands = sqlText.Split(new[] { ";" }, StringSplitOptions.RemoveEmptyEntries);

            return sqlCommands;
        }

        /// <summary>
        /// Run SQL commands in Snowflake and return a JSON object with column/value pairs from first row of the result.
        /// See https://github.com/snowflakedb/snowflake-connector-net for more details.
        /// </summary>
        /// <param name="log">ILogger object</param>
        /// <param name="snowflakeConnectionString">Snowflake connection string</param>
        /// <param name="setVariableCommand">The SQL set variable command to execute</param>
        /// <param name="sqlCommands">The SQL commands to execute</param>
        /// <returns>JSON object with column/value pairs from first row of the result</returns>
        private static JObject runSnowflakeSqlCommands(ILogger log, string snowflakeConnectionString, string setVariableCommand, string[] sqlCommands)
        {
            var output = new JObject();
            log.LogInformation($"Found {sqlCommands.Length} queries to execute");

            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                // Connect to Snowflake
                conn.ConnectionString = snowflakeConnectionString;
                conn.Open();

                using (IDbCommand cmd = conn.CreateCommand())
                {
                    // First run the set variable command, if we have one
                    if (!String.IsNullOrEmpty(setVariableCommand))
                    {
                        cmd.CommandText = setVariableCommand;
                        log.LogInformation($"Running SQL set variable command: {cmd.CommandText}");
                        cmd.ExecuteNonQuery();
                    }

                    // Run every query except the last one using ExecuteNonQuery()
                    for (int i = 0; i < sqlCommands.Length - 1; i++)
                    {
                        cmd.CommandText = sqlCommands[i].Trim();
                        log.LogInformation($"Running SQL command #{i+1}: {cmd.CommandText}");
                        cmd.ExecuteNonQuery();
                    }

                    // Finally run the last query using ExecuteReader() so we can collect the output
                    cmd.CommandText = sqlCommands[sqlCommands.Length - 1].Trim();
                    log.LogInformation($"Running SQL command #{sqlCommands.Length} (final): {cmd.CommandText}");
                    IDataReader reader = cmd.ExecuteReader();

                    // The final result should be a table with one row and n columns, format the column/value pairs in JSON.
                    // Warning: If more than one row is returned in the final result this will return the following error:
                    // "Property with the same name already exists on object."
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
