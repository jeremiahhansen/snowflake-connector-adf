# Snowflake Connector for Azure Data Factory (ADF)
This connector is an Azure Function which allows Azure Data Factory (ADF) to connect to Snowflake in a flexible way. It provides SQL-based stored procedure functionality with dyamic parameters and return values. Used with ADF you can build complete end-to-end data warehouse solutions for Snowflake while following Microsoft and Azure best practices around portability and security.

To get started please follow the steps outlined in the [Prerequisites](#Prerequisites) and [Setup](#Setup) sections below.

_**Note**_: As of September 2019 ADF does not provide a native Snowflake connector and Snowflake does not provide native SQL-based stored procedures. The goal of this connector is to enable SQL-based stored procedures against Snowflake from ADF so that when both native connectors are available the migration will be as painless as possible.

## Table of Contents
1. Connector Overview
   1. [High Level Overview](#high-level-overview)
   1. [Parameters](#parameters)
   1. [Multiple Queries](#multiple-queries)
   1. [Return Values](#return-values)
   1. [Script Storage](#script-storage)
1. [ADF Overview](#adf-overview)
1. [Prerequisites](#prerequisites)
1. [Setup](#setup)

## Connector Overview
### High Level Overview
At a really high level stored procedures provide the ability to pass parameters (or arguments), run multiple SQL statments, and return values. To enable these capabilities in a Snowflake query I had to come up with a macro-like syntax. It was also important to make sure the queries could be run in Snowflake as-is to enable better debugging and development lifecycle support. I accomplished this by using standard Snowflake SQL comments which leverage the conventions outlined below.

The following is a sample SQL script which this connector treats like a stored procedure:

```sql
SELECT /*Parameter_With_Quotes*/'John'/*firstName*/ AS FIRST_NAME, 'Doe' AS LAST_NAME, 1 AS AGE;

/*Sql_Query_Separator*/

SELECT CONCAT(/*Parameter_With_Quotes*/'John'/*firstName*/, 'Bar') AS OUTPUT_1, /*Parameter*/1/*age*/ + 100 AS OUTPUT_2;
```

The connector expects all input to be supplied via the `POST` body. Here is a sample `POST` body to execute the script above:

```json
{
    "databaseName": "MyDatabase",
    "schemaName": "MySchema",
    "storedProcedureName": "MyStoredProcedure",
    "firstName": "Foo",
    "age": 10
}
```
And in this case the connector would return the following JSON object:

```json
{
    "customOutput": {
        "OUTPUT_1": "FooBar",
        "OUTPUT_2": "110"
    }
}
```

### Parameters
Passing parameters (or arguments) to a stored procedure is critical so that the same code can be reused. At a high level there are two types of parameters:

1. Quoted Parameters
1. Non-Quoted Parameters

Here is the second query from the [High Level Overview](#high-level-overview) above, with one of each parameter type:

```sql
SELECT CONCAT(/*Parameter_With_Quotes*/'John'/*firstName*/, 'Bar') AS OUTPUT_1, /*Parameter*/1/*age*/ + 100 AS OUTPUT_2;
```

The macro-like syntax is that the parameter to replace always begins with either `/*Parameter_With_Quotes*/` or `/*Parameter*/` (depending on the type of parameter desired). It is then followed with a default value for the parameter and another comment which contains the name of the parameter (which is case sensitive). As you can see `/*Parameter_With_Quotes*/` replaces the value with single quotes while `/*Parameter*/` does not.

The connector expects all parameters to be supplied via the `POST` body. Here is a snippet from `POST` body in the [High Level Overview](#high-level-overview) section above which contains the parameter values:

```json
{
    ...
    "firstName": "Foo",
    "age": 10
}
```

These two attributes, `firstName` and `age`, correspond to the placeholders in the SQL query above. This works entirely on a name matching basis, the attribute name in the `POST` body must match the placeholder name in the SQL query exactly (i.e. they are case sensitive).

### Multiple Queries
Executing multiple SQL statements in a stored procedure is critical to support non-trival use cases. As of September 2019 the Snowflake API does not support running multiple SQL statements in a single API call. Because of that we need some way to identify the boundaries between queries. Ideally we would parse the script and use the semicolon statement separator for this purpose. But currenlty the connector does not have such parsing logic and relies instead on the following convention.

Each query in the script must be separated by the following comment: `/*Sql_Query_Separator*/`

### Return Values
Returning values from a stored procedure is critical so that we can pass status and other elements (like row counts) to the caller. To enable return values the connector expects the final query in the script to return a result set with a single row with one or more columns. Here is the second query again from the [High Level Overview](#high-level-overview) above that does just that:

```sql
SELECT CONCAT(/*Parameter_With_Quotes*/'John'/*firstName*/, 'Bar') AS OUTPUT_1, /*Parameter*/1/*age*/ + 100 AS OUTPUT_2;
```

The columns in the result are pivoted so that each column and value pair becomes one return value. The query above then returns two values `OUTPUT_1` and `OUTPUT_2`. Here is the resulting JSON object which the connector returns:

```json
{
    "customOutput": {
        "OUTPUT_1": "FooBar",
        "OUTPUT_2": "110"
    }
}
```

*Note*: The return values are attributes of the `customOutput` object. This was originally done to match the behaviour of the *Custom Activity* in ADF and can be easily changed in the connector code.

### Script Storage
All "stored procedure" scripts executed by this connector are stored in an Azure Blob Storage account. The container name is configurable but defaults to `storedprocedures`. Within the container all files are organized according to the following convention (all case sensitive):

```
/<Database Name>
    /<Schema Name>
        /<Stored Procedure Name>.sql
```

The connector expects all three parameters to be supplied via the `POST` body. Here is a snippet from `POST` body in the [High Level Overview](#high-level-overview) section above which contains the required parameter values:

```json
{
    "databaseName": "MyDatabase",
    "schemaName": "MySchema",
    "storedProcedureName": "MyStoredProcedure",
    ...
}
```

The connector builds a blob storage path based off of those values and reads in the corresponding script file. In this case the full path is `/MyDatabase/MySchema/MyStoredProcedure.sql`. You can find the sample [MyStoredProcedure.sql](/Docs/MyStoredProcedure.sql) script in the */Docs* folder of this repo.

## ADF Overview
Here is an overview of the ADF pipeline created durig the [Setup](#setup) section below:

![ADF Pipeline Overview](/Docs/Screenshots/adf-pipeline-overview.png?raw=true "ADF Pipeline Overview")

## Prerequisites
In order to deploy the connector and associate Azure resources you must have the following:

1. An Azure Subscription with at least Contributor access to a resource group
1. [Visual Studio Code](https://code.visualstudio.com) installed on your computer with the following Extensions installed
   1. C#
   1. Azure Functions (see the [Azure Functions Getting Started](https://code.visualstudio.com/tutorials/functions-extension/getting-started) guide for more setup steps)

## Setup
Please complete the steps outlined in the [Prerequisites](#Prerequisites) section first and then do the following:

1. Create an Azure Resource Group to contain the required Azure resources (we'll be creating 5 resources total)
1. Lookup your Azure Active Directory Object Id
   1. Login to the [Azure Portal](https://portal.azure.com)
   1. Open the Azure Active Directory (AAD) pane by clicking on the "Azure Active Directory" link in the left navbar or by searching for "Azure Active Directory" in the top search bar
   1. Click on the "Users" link in the AAD left navbar
   1. Search the list of users by using your name or email then open your user account
   1. Copy the "Object ID" and save it for later
1. Deploy the Azure Resource Manager (ARM) template for the solution
   1. Login to the [Azure Portal](https://portal.azure.com) (if you aren't already)
   1. Search for and open the "Deploy a custom template" service in the top search bar
   1. Click on "Build your own template in the editor" and then copy and paste the entire contents of the `\Docs\SnowflakeConnectorAdfArmTemplate.json` file in this repo and click "Save"
   1. On the "Custom deployment" update the following fields
      1. *Resource Group*: Select the Resource Group you created earlier
      1. *Resource Name Prefix*: Pick a unique prefix which will be appended to all Azure resources created
      1. *Key Vault Owner Object Id*: Paste the Active Directory Object Id you looked up earlier
      1. *Snowflake Connection String*: Enter the Snowflake connection string to your Snowflake account in the following format: `account=<account name>;user=<user name>;password=<user password>` (see the [Snowflake Connector for .NET](https://github.com/snowflakedb/snowflake-connector-net) page for more details)
      1. Check the "I agree to the terms and conditions stated above" and click "Purchase" (*Note*: this just means that you agree to pay for the Azure resources being created by the ARM template)
   1. Wait for the ARM deployment to complete
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
1. Upload the stored procedure scripts to the new storage account
   1. Connect to the new storage account either through the [Azure Portal UI](https://portal.azure.com) or the [Azure Storage Explorer](https://azure.microsoft.com/en-us/features/storage-explorer/)
   1. Create the following folder structure in the "storedprocedures" container: `MyDatabase\MySchema`
   1. Upload the `\Docs\MyStoredProcedure.sql` file in this repo to that new folder
1. Deploy the Azure Function code to the new Function App
   1. Open the solution in Visual Studio Code (VS Code)
   1. Click on the "Azure" icon in the left nav bar
   1. Click on the "Deploy to Function App..." (up arrow) icon in the Azure Function pane
   1. Select your new Function App from the list and click "Deploy"
1. Run the sample Azure Data Factory (ADF) pipeline
   1. Open the Azure Data Factory resource that was created
   1. Click on the "Author & Monitor" icon
   1. Click on the "Author" (pencil) icon in the left navbar
   1. Click on the "SampleSnowflakePipeline_P" pipeline in the Factory Resources section
   1. Click on the "Debug" link and then "Finish" to execute the pipeline

In order to debug/run the Azure Function locally you need to create a `local.settings.json` file and add the three environment variables expected by the function. Here is a template for the contents of the file (you'll need to replace all <> placeholders with real values for your environment):

```json
{
    "IsEncrypted": false,
    "Values": {
        "AzureWebJobsStorage": "",
        "FUNCTIONS_WORKER_RUNTIME": "dotnet",
        "storageAccountConnectionString": "DefaultEndpointsProtocol=https;AccountName=<Storage Account Name>;AccountKey=<Storage Account Key>;EndpointSuffix=core.windows.net",
        "storageAccountContainerName": "storedprocedures",
        "snowflakeConnectionString": "account=<Snowflake Account Name>;user=<Snowflake User Name;password=<Snowflake User Password>"
    }
}
```
