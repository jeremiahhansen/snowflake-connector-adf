# Snowflake Connector for Azure Data Factory (ADF)
This connector is an Azure Function which allows Azure Data Factory (ADF) to connect to Snowflake in a flexible way. It provides SQL-based stored procedure functionality with dyamic parameters and return values. Used with ADF you can build complete end-to-end data warehouse solutions for Snowflake while following Microsoft and Azure best practices around portability and security.

To get started please follow the steps outlined in the [Prerequisites](#Prerequisites) and [Deployment](#deployment) sections below.

_**Note**_: As of November 2019 ADF does not provide a native Snowflake connector and Snowflake does not provide native SQL-based stored procedures. The goal of this connector is to enable SQL-based stored procedures against Snowflake from ADF so that when both native connectors are available the migration will be as painless as possible.

## Table of Contents
1. [Connector Overview](#connector-overview)
   1. [High Level Overview](#high-level-overview)
   1. [Parameters](#parameters)
   1. [Multiple Queries](#multiple-queries)
   1. [Return Values](#return-values)
   1. [Script Storage](#script-storage)
1. [Integration with ADF](#integration-with-adf)
   1. [ADF Overview](#adf-overview)
   1. [ADF Expressions](#adf-expressions)
1. [Prerequisites](#prerequisites)
1. [Deployment](#deployment)
1. [Legal](#legal)

## Connector Overview
### High Level Overview
At a really high level stored procedures provide the ability to pass parameters, run multiple SQL statments, and return values. I'll explain how it works below, but to begin with here is a high level overview of the connector:

![Connector Overview](/Docs/Screenshots/connector-overview.png?raw=true "Connector Overview")

And a more detailed sequence diagram to help explain the overall process:

![Connector Sequence Diagram](/Docs/Screenshots/connector-sequence-diagram.png?raw=true "Connector Sequence Diagram")

Now let's dig a bit deeper into how this works. The following is a sample SQL script which this connector treats like a stored procedure:

```sql
SELECT $FIRST_NAME AS FIRST_NAME, 'Doe' AS LAST_NAME, 1 AS AGE;

SELECT CONCAT($FIRST_NAME, 'Bar') AS OUTPUT_1, $AGE + 100 AS OUTPUT_2;
```

The connector expects all input to be supplied via the `POST` body. Here is a sample `POST` body to execute the script above:

```json
{
    "databaseName": "MyDatabase",
    "schemaName": "MySchema",
    "storedProcedureName": "MyStoredProcedure",
    "parameters": [
        {
            "name": "FIRST_NAME",
            "type": "VARCHAR",
            "value": "Foo"
        },
        {
            "name": "AGE",
            "type": "NUMBER",
            "value": "10"
        }
    ]
}
```

And in this case the connector would return the following JSON object:

```json
{
    "OUTPUT_1": "FooBar",
    "OUTPUT_2": "110"
}
```

### Parameters
The connector leverages [Snowflake SQL session variables](https://docs.snowflake.net/manuals/sql-reference/session-variables.html) to pass values to the stored procedure script code.

The connector expects all parameters to be supplied via the `parameters` JSON array in the `POST` body. Here is a snippet from `POST` body in the [High Level Overview](#high-level-overview) section above which contains the parameter values:

```json
{
    "parameters": [
        {
            "name": "FIRST_NAME",
            "type": "VARCHAR",
            "value": "Foo"
        },
        {
            "name": "AGE",
            "type": "NUMBER",
            "value": "10"
        }
    ]
}
```

Basically it's an array of objects, with each object representing one parameter. Each parameter JSON object needs to contain the following three attributes:

1. name
1. type
1. value

With the following values for `type` currently supported:

* VARCHAR
* NUMBER

The connector generates a single `SET` query based of the parameters provided and executes it before running any of the queries in the SQL script. In this case the following `SET` query is generated:

```sql
SET (FIRST_NAME,AGE) = ('Foo',10)
```

The SQL queries in the script then access the parameters as standard Snowflake session variables. Here is the second query from the [High Level Overview](#high-level-overview) above which uses each parameter/variable:

```sql
SELECT CONCAT($FIRST_NAME, 'Bar') AS OUTPUT_1, $AGE + 100 AS OUTPUT_2;
```

For more advanced scenarios you can also leverage the Snowflake `IDENTIFIER()` function to dynamically reference Snowflake objects like databases, schemas, tables, columns, stage names, etc. See [String Literals / Session Variables / Bind Variables as Identifiers](https://docs.snowflake.net/manuals/sql-reference/identifier-literal.html) for more details.

### Multiple Queries
Executing multiple SQL statements in a stored procedure is critical to support non-trival use cases. As of September 2019 the Snowflake API does not support running multiple SQL statements in a single API call. Because of that we need to manualy break up the script into each individual statement and run them sequentially. We also need to make sure that we run then in the same Snowflake session so that session scoped variables (the parameters) can be used across queries.

The connector uses the standard SQL semicolon to identify query boundaries. As such the semicolon is required after each query.

### Return Values
Returning values from a stored procedure is critical so that we can pass status and other elements (like row counts) to the caller. To enable return values the connector expects the final query in the script to return a result set with a single row with one or more columns. Here is the second query again from the [High Level Overview](#high-level-overview) above that does just that:

```sql
SELECT CONCAT($FIRST_NAME, 'Bar') AS OUTPUT_1, $AGE + 100 AS OUTPUT_2;
```

The columns in the result are pivoted so that each column and value pair becomes one return value. The query above then returns two values `OUTPUT_1` and `OUTPUT_2`. Here is the resulting JSON object which the connector returns:

```json
{
    "OUTPUT_1": "FooBar",
    "OUTPUT_2": "110"
}
```

*Note:* If more than one row is returned in the final result the connector will fail with the following error: "Property with the same name already exists on object."


### Script Storage
All "stored procedure" scripts executed by this connector are stored in an Azure Blob Storage account. The container name is configurable but defaults to `storedprocedures`. Within the container all files are organized according to the following convention (all case sensitive):

```
/<Database Name>
    /<Schema Name>
        /<Stored Procedure Name>.sql
```

The connector expects all three parameters to be supplied via the JSON `POST` body. Here is a snippet from `POST` body in the [High Level Overview](#high-level-overview) section above which contains the required parameter values:

```json
{
    "databaseName": "MyDatabase",
    "schemaName": "MySchema",
    "storedProcedureName": "MyStoredProcedure"
}
```

The connector builds a blob storage path based off of those values and reads in the corresponding script file. In this case the full path is `/MyDatabase/MySchema/MyStoredProcedure.sql`. You can find the sample [MyStoredProcedure.sql](/Docs/SampleStoredProcedures/MyDatabase/MySchema/MyStoredProcedure.sql) script in the `/Docs` folder of this repo.

## Integration with ADF
### ADF Overview
This project comes with a few sample ADF pipelines which demonstrates how to use this connector within ADF. The sample ADF resources are deployed during the [Deployment](#deployment) section below and are contained in the [SnowflakeConnectorAdfArmTemplate.json](/Docs/SnowflakeConnectorAdfArmTemplate.json) script in the `/Docs` folder of this repo. Here is a screenshot showing one of the sample pipelines:

![ADF Pipeline Overview](/Docs/Screenshots/adf-pipeline-overview.png?raw=true "ADF Pipeline Overview")

As shown in the screenshot above we use the native `Azure Function` Activity in ADF to interact with the Snowflake connector. The HTTP method is `POST` and the body contains the elements described above.

### ADF Expressions
[Expressions in ADF](https://docs.microsoft.com/en-us/azure/data-factory/control-flow-expression-language-functions) are very powerful and allow us to make the parameters passed to the connector very flexible. The screenshot above shows how to make use of ADF pipeline parameters when calling the connector. Here is the body of the HTTP request from the first ADF activity:

```json
{
  "databaseName": "MyDatabase",
  "schemaName": "MySchema",
  "storedProcedureName": "MyStoredProcedure",
  "parameters": [
    {
      "name": "FIRST_NAME",
      "type": "VARCHAR",
      "value": "@{pipeline().parameters.FIRST_NAME}"
    },
    {
      "name": "AGE",
      "type": "NUMBER",
      "value": "@{pipeline().parameters.AGE}"
    }
  ]
}
```

And in order to access return values from an ADF activity we make use of the `activity()` expression function. Here is the body of the HTTP request from the second ADF activity:

```json
{
  "databaseName": "MyDatabase",
  "schemaName": "MySchema",
  "storedProcedureName": "MyStoredProcedure",
  "parameters": [
    {
      "name": "FIRST_NAME",
      "type": "VARCHAR",
      "value": "@{activity('StoredProcedure1').output.OUTPUT_1}"
    },
    {
      "name": "AGE",
      "type": "NUMBER",
      "value": "@{activity('StoredProcedure1').output.OUTPUT_2}"
    }
  ]
}
```

## Prerequisites
In order to deploy the connector and associate Azure resources you must have the following:

1. A Snowflake account and Snowflake user with ACCOUNTADMIN role access
1. An Azure Subscription with at least Contributor access to a resource group
1. [.NET Core 3.1](https://dotnet.microsoft.com/download/dotnet-core/3.1) installed on your computer (choose the newest version)
1. [Visual Studio Code](https://code.visualstudio.com) installed on your computer with the following Extensions installed
   1. C#
   1. Azure Functions
1. Azure Functions Core Tools installed on your computer (see the [Azure Functions Getting Started](https://code.visualstudio.com/tutorials/functions-extension/getting-started) guide for details)
1. [Azure Storage Explorer](https://azure.microsoft.com/en-us/features/storage-explorer/) installed on your computer
1. This entire repository either downloaded (or cloned) to your computer

## Deployment
Please complete the steps outlined in the [Prerequisites](#Prerequisites) section first and then do the following:

1. Login to the [Azure Portal](https://portal.azure.com)
1. Create an Azure Resource Group to contain the required Azure resources (we'll be creating 5 resources total)
1. Lookup your Azure Active Directory Object Id
   1. Open the Azure Active Directory (AAD) pane by clicking on the "Azure Active Directory" link in the left navbar or by searching for "Azure Active Directory" in the top search bar
   1. Click on the "Users" link in the AAD left navbar
   1. Search the list of users by using your name or email then open your user account
   1. Copy the "Object ID" and save it for later
1. Deploy the Azure Resource Manager (ARM) template for the solution
   1. Search for and open the "Deploy a custom template" service in the top search bar
   1. Click on "Build your own template in the editor" and then copy and paste the entire contents of the `\Docs\SnowflakeConnectorAdfArmTemplate.json` file in this repo and click "Save"
   1. On the "Custom deployment" update the following fields
      1. *Resource Group*: Select the Resource Group you created earlier
      1. *Resource Name Prefix*: Pick a unique prefix which will be appended to all Azure resources created
      1. *Key Vault Owner Object Id*: Paste the Active Directory Object Id you looked up earlier
      1. *Snowflake Connection String*: Enter the Snowflake connection string to your Snowflake account in the following format: `account=<Snowflake Account Name>;host=<Snowflake Fully Qualified Host Name>;user=ADF_DEMO_USER;password=<Snowflake User Password>` (see the [Snowflake Connector for .NET](https://github.com/snowflakedb/snowflake-connector-net) page for more details, and remember the password you pick here because you'll use it again when creating the Snowflake objects)
      1. Check the "I agree to the terms and conditions stated above" and click "Purchase" (*Note*: this just means that you agree to pay for the Azure resources being created by the ARM template)
   1. Wait for the ARM deployment to complete
1. Create a Shared Access Signature (SAS) for the Snowflake STAGE object
   1. Open the Storage Account resource that was created for the connector
   1. Click on "Shared access signature" in the left nav bar
   1. Update the SAS access policy details as appropriate. Here is a suggested setup for the sample pipeline:
      1. For "Allowed services" make sure only "Blob" is selected
      1. For "Allowed resource types" make sure only "Container" and "Object" are selected
      1. For "Allowed permissions" you'll only need "Read" and "List" to load data
      1. For the "End" time pick a date in the future, maybe a month out (depending)
      1. For "Allowed protocols" make sure that "HTTPS only" is selected
   1. Click on "Generate SAS and connection string"
   1. Copy the "SAS token" and save for the later
   1. Please note that once you leave this page you can't get this value again, so save it now.
1. Deploy the required Snowflake objects for the sample pipelines
   1. Login to your Snowflake account with a user that has ACCOUNTADMIN role access
   1. Open the `\Docs\SnowflakeDbSetup.sql` script in Snowflake, or copy and paste the contents to a blank Worksheet in Snowflake
   1. Follow the steps in the "Script setup" section to update a few values in the script
   1. Run all queries in the Worksheet
1. Create a Key Vault Access Policy for the Function App
   1. Open the Key Vault resource that was created
   1. Click on "Access policies" in the Key Vault left nav bar
   1. Click on the "+Add Access Policy" link
   1. Under *Secret permissions* click on "Get" and "list"
   1. Click on *Select principal* then *Select* the Function App you created (the name will be the *Resource Name Prefix* you select earlier + "fa")
   1. Click on "Select" and the "Add"
   1. Click on "Save" to save the new Access Policy (**important**)
1. Create a Key Vault Access Policy for the Azure Data Factory
   1. Follow the steps above except this time for the *Select principal* step enter the name of the Azure Data Factory you created (the name will be the *Resource Name Prefix* you select earlier + "adf")
1. Update the `snowflakeConnectionString` Function App setting with Key Vault secret version number (**note**: this is a temporary workaround until Azure Key Vault integration with Azure Functions is GA)
   1. Open the Key Vault resource that was created
   1. Click on the "Secrets" link in the Key Vault left nav bar and then click on the "snowflakeConnectionString" secret
   1. Copy the "CURRENT VERSION" ID and save it for later
   1. Open the Function App resource that was created
   1. Click on the "Platform features" link and then on "Configuration"
   1. Edit the "snowflakeConnectionString" setting and replace the `VERSION` string with the "CURRENT VERSION" ID from earlier
   1. Click "Save" (**important**)
1. Update the `storageAccountConnectionString` Function App setting with Key Vault secret version number (**note**: this is a temporary workaround until Azure Key Vault integration with Azure Functions is GA)
   1. Follow the steps above except this time use the "storageAccountConnectionString" secret
1. Upload the sample stored procedure scripts to the new connector storage account
   1. Open Azure Storage Explorer and find your new connector storage account
   1. Open the "storedprocedures" blob container
   1. Click on "Upload" and then "Upload Folder..."
   1. Select the "ADF_DEMO" folder from the `\Docs\SampleStoredProcedures` folder and click "Upload"
   1. Select the "MyDatabase" folder from the `\Docs\SampleStoredProcedures` folder and click "Upload"
   1. Note: You can also upload these files via the [Azure Portal UI](https://portal.azure.com) but you'll need to manually create the appropriate folder structure and upload each file individually
1. Deploy the Azure Function code to the new Function App (from **Visual Studio Code**)
   1. Open the solution in Visual Studio Code (VS Code)
   1. Click on the "Azure" icon in the left nav bar
   1. Click on the "Deploy to Function App..." (up arrow) icon in the Azure Function pane
   1. Select your new Function App from the list and click "Deploy"
1. Run the sample Azure Data Factory (ADF) pipeline
   1. Open the Azure Data Factory resource that was created
   1. Click on the "Author & Monitor" icon
   1. Click on the "Author" (pencil) icon in the left navbar
   1. Click on the `SampleSnowflakePipeline_P` pipeline in the Factory Resources section
   1. Click on the "Debug" link and then "Finish" to execute the pipeline
   1. Do the same to execute the `DataIngestion_P` pipeline

In order to debug/run the Azure Function locally you need to create a `local.settings.json` file and add the three environment variables expected by the function. Here is a template for the contents of the file (you'll need to replace all <> placeholders with real values for your environment):

```json
{
    "IsEncrypted": false,
    "Values": {
        "AzureWebJobsStorage": "",
        "FUNCTIONS_WORKER_RUNTIME": "dotnet",
        "storageAccountConnectionString": "DefaultEndpointsProtocol=https;AccountName=<Storage Account Name>;AccountKey=<Storage Account Key>;EndpointSuffix=core.windows.net",
        "storageAccountContainerName": "storedprocedures",
        "snowflakeConnectionString": "account=<Snowflake Account Name>;host=<Snowflake Fully Qualified Host Name>;user=ADF_DEMO_USER;password=<Snowflake User Password>"
    }
}
```

See the [Snowflake Connector for .NET](https://github.com/snowflakedb/snowflake-connector-net) page for important details around the Snowflake account name.

## Legal
Copyright (c) 2019 Snowflake Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this connector except in compliance with the License. You may obtain a copy of the License at: [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
