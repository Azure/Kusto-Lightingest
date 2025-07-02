# Kusto Light Ingest

LightIngest is a command-line utility for ad-hoc data ingestion into Azure Data Explorer. The utility can pull source data from a local folder or from a storage container. LightIngest is most useful when you want to ingest a large amount of data, because there is no time constraint on ingestion duration. It's also useful when you want to later query records according to the time they were created, and not the time they were ingested.

## Download and Usage

### Binaries
Binaries are found under each release in [release section](https://github.com/Azure/Kusto-Lightingest/releases).
They are standalone and as such have no prerequisites, simply download the one tergeting your operating system and run:

```bat
LightIngest.exe "https://ingest-{Cluster name and region}.kusto.windows.net;Fed=True" -db:{Database} -table:{table} -source:"https://{Account}.blob.core.windows.net/{ROOT_CONTAINER};{StorageAccountKey}" -creationTimePattern:"'historicalvalues'yyyyMMdd'.parquet'" -pattern:"*.parquet" -format:parquet -limit:2 -cr:10.0
````

### Dotnet Tool
LightIngest is published as a [dotnet tool](https://learn.microsoft.com/en-us/dotnet/core/tools/global-tools) from [this feed](https://www.nuget.org/packages/Microsoft.Azure.Kusto.LightIngest/12.0.0-preview.1)
Dotnet tools require .Net SDK >= 6.0 installed and run installation command:
```.NET CLI
dotnet tool install --global Microsoft.Azure.Kusto.LightIngest --version 13.0.2
```
To run the tool simply use its name:
```.NET CLI
LightIngest "https://ingest-{Cluster name and region}.kusto.windows.net;Fed=True" -db:{Database} -table:{table} -source:"https://{Account}.blob.core.windows.net/{ROOT_CONTAINER};{StorageAccountKey}" -creationTimePattern:"'historicalvalues'yyyyMMdd'.parquet'" -pattern:"*.parquet" -format:parquet -limit:2 -cr:10.0
```

## Documentation

See the full documentation [here](https://learn.microsoft.com/en-us/azure/data-explorer/lightingest)

## Content

This repo contains a command-line project for ingesting to Azure Data Explorer

## API Package

This tool is also available as a [package on nuget.org](https://www.nuget.org/packages/Microsoft.Azure.Kusto.Tools/)

## SDK API

The code is using the [ADX C# SDK]([package on nuget.org](https://www.nuget.org/packages/Microsoft.Azure.Kusto.Ingest)) 

## Contribute

There are many ways to contribute to the project.

* [Submit bugs](https://github.com/Azure/Kusto-Lightingest/issues)
* Review the [source code changes](https://github.com/Azure/Kusto-Lightingest/issues/commits/master).

## Getting Help / Reporting Problems

* [Microsoft Documentation](https://learn.microsoft.com/azure/data-explorer/lightingest)
* [Stack Overflow](https://stackoverflow.com/questions/tagged/azure-data-explorer) - Ask questions about how to use Kusto. Start posts with 'LightIngest'. This is monitored by Kusto team members.
* [User Voice](https://aka.ms/adx.uservoice) - Suggest new features or changes to existing features.
* [Azure Data Explorer](https://dataexplorer.azure.com) - Give feedback or report problems using the user feedback button (top-right near settings).
* [Azure Support](https://learn.microsoft.com/en-us/azure/azure-portal/supportability/how-to-create-azure-support-request) - Report problems with the Kusto service.
* [Open an issue here](https://github.com/Azure/Kusto-Lightingest/issues) - for problems specifically with this library.

## Microsoft Open Source Code of Conduct

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).

Resources:

* [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/)
* [Microsoft Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/)
* Contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with questions or concerns




