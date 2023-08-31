# Kusto Light Ingest

LightIngest is a command-line utility for ad-hoc data ingestion into Azure Data Explorer. The utility can pull source data from a local folder or from a storage container. LightIngest is most useful when you want to ingest a large amount of data, because there is no time constraint on ingestion duration. It's also useful when you want to later query records according to the time they were created, and not the time they were ingested.

## Usage

Binaries are found under each release in the [release section](https://github.com/Azure/Kusto-Lightingest/releases) as stand-alone binaries runtime specific ready to use.
Windows example: LightIngest.exe "https://ingest-{Cluster name and region}.kusto.windows.net;Fed=True" -db:{Database} -table:{table} -source:"https://{Account}.blob.core.windows.net/{ROOT_CONTAINER};{StorageAccountKey}" -creationTimePattern:"'historicalvalues'yyyyMMdd'.parquet'" -pattern:"*.parquet" -format:parquet -limit:2 -cr:10.0

## Documentation

See the full documentation [here](https://learn.microsoft.com/en-us/azure/data-explorer/lightingest)

## Content

This repo contains a command-line project for ingesting to Azure Data Explorer

## API Package

This source code is also available as a [package on nuget.org](https://www.nuget.org/packages/Microsoft.Azure.Kusto.Tools/)

## SDK API

The code is using the [ADX C# SDK]([package on nuget.org](https://www.nuget.org/packages/Microsoft.Azure.Kusto.Ingest)) 

## Contribute

There are many ways to contribute to Kusto Query Language.

* [Submit bugs](https://github.com/Azure/Kusto-Lightingest/issues) and help us verify fixes as they are checked in.
* Review the [source code changes](https://github.com/Azure/Kusto-Lightingest/issues/commits/master).

## Getting Help / Reporting Problems

* [Stack Overflow](https://stackoverflow.com/questions/tagged/azure-data-explorer) - Ask questions about how to use Kusto. Start posts with 'LightIngest'. This is monitored by Kusto team members.
* [User Voice](https://aka.ms/adx.uservoice) - Suggest new features or changes to existing features.
* [Azure Data Explorer](https://dataexplorer.azure.com) - Give feedback or report problems using the user feedback button (top-right near settings).
* [Azure Support](https://learn.microsoft.com/en-us/azure/azure-portal/supportability/how-to-create-azure-support-request) - Report problems with the Kusto service.
* Open an issue here - for problems specifically with this library.
* Start a discussion - talk about this library, or anything related to Kusto.

## Microsoft Open Source Code of Conduct

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).

Resources:

* [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/)
* [Microsoft Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/)
* Contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with questions or concerns




