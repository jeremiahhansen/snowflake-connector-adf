# Snowflake Connector for Azure Data Factory (ADF)
This connector is an Azure Function which allows Azure Data Factory (ADF) to connect to Snowflake in a flexible way.

## Prerequisites
In order to deploy the connector and associate Azure resources you must have the following:

1. An Azure Subscription with at least Contributor access to a resource group
1. [Visual Studio Code](https://code.visualstudio.com) installed on your computer with the following Extensions installed
   1. C#
   1. Azure Functions (see the [Azure Functions Getting Started](https://code.visualstudio.com/tutorials/functions-extension/getting-started) guide for more setup steps)

## Setup
Please complete the steps outlined in the **Prerequisites** section first and then do the following:

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


## Creating Stored Procedure Scripts
TODO: Add details here
