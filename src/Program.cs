// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using Kusto.Cloud.Platform.Msal;
using Kusto.Cloud.Platform.Utils;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using Kusto.Ingest;

namespace LightIngest
{
    #region class CommandLineArgs
    internal class CommandLineArgs
    {
        #region Target cluster/database/table to ingest to (must be the first)
        [CommandLineArg(
            "",
            "Connection string pointing at a Kusto service endpoint,\r\n" +
            "For example, \"https://ingest-contoso.westus.kusto.windows.net;Fed=true\"\r\n" +
            "Database can also be specified within the connection string: \"https://ingest-contoso.westus.kusto.windows.net;Fed=true;Initial Catalog=NetDefaultDB.\"\r\n" +
            "For complete Kusto Connection String documentation, see https://docs.microsoft.com/azure/kusto/api/connection-strings/kusto",
            DefaultValue = "https://ingest-contoso.westus.kusto.windows.net;Fed=true")]
        public string ConnectionString = "https://ingest-contoso.westus.kusto.windows.net;Fed=true";

        [CommandLineArg(
            "managedIdentity",
            "Client Id of the managed identity (user-assigned or system-assigned) to be used for connecting to Kusto. Use \"system\" for system-assigned identity.\r\n" +
            "The managed identity needs at least 'Table Ingestor' permissions on the target Ksuto table.\r\n" +
            "The managed identity needs to be configured in the azure service that currently runs this tool.",
            ShortName = "mi", Mandatory = false)]
        public string ConnectWithManagedIdentity = null;

        [CommandLineArg(
            "ingestWithManagedIdentity",
            "Client id of the managed identity (user-assigned or system-assigned) to be used by Kusto to download the data. Use \"system\" for system-assigned identity.\r\n" +
            "The managed identity needs 'Read' permissions over the blobs and to be configured in the Kusto service.",
            ShortName = "ingestmi", Mandatory = false)]
        public string IngestWithManagedIdentity = null;

        [CommandLineArg(
            "database",
            "Name of the Kusto database to receive the data. If set, overrides the database specified in the connection string.",
            ShortName = "db", Mandatory = false)]
        public string DatabaseName = null;

        [CommandLineArg(
            "table",
            "Name of the Kusto table to receive the data.",
            Mandatory = true)]
        public string TableName = null;
        #endregion

        #region Source data to ingest, varies between ingestion jobs
        [CommandLineArg(
            "sourcePath",
            "When the source data to ingest resides on a local disk, this is the path to the directory where the data is located\r\n" +
            "When the source data resides on cloud storage, this is the root URI of the container, adls filesystem or s3 bucket, affixed with an account key or SAS\r\n" +
            "For example, \"https://ACCOUNT_NAME.blob.core.windows.net/CONTAINER_NAME?SAS_TOKEN\" or \"https://ACCOUNT_NAME.blob.core.windows.net/CONTAINER_NAME;ACCOUNT_KEY\"",
            "For the complete list see: \"https://learn.microsoft.com/en-us/azure/data-explorer/kusto/api/connection-strings/storage-connection-strings#storage-connection-string-templates\"",
            ShortName = "source", Mandatory = true)]
        public string SourcePath = null;

        [CommandLineArg(
            "ConnectToStorageWithUserAuth",
            "Optionally authenticate to the data source storage service with user credentials for listing of the container. Use this if you cannot configure keys on the sourcePath.\r\n" +
            "The identity used here needs at least 'Read' and 'List' privileges on the container.\r\n" +
            "Options: 'PROMPT', 'DEVICE_CODE'\r\n" +
            "Default: Off",
            ShortName = "storageUserAuth",
            Mandatory = false)]
        public string ConnectToStorageWithUserAuth = "";

        [CommandLineArg(
            "ConnectToStorageLoginUri",
            "If 'ConnectToStorageWithUserAuth' is not default, optionally provide the AAD login Uri.\r\n" +
            "Default: Public Azure Cloud login Uri",
            ShortName = "storageLoginUri",
            Mandatory = false)]
        public string ConnectToStorageLoginUri = "";

        [CommandLineArg(
          "connectToStorageWithManagedIdentity",
          "Optionally authenticate to the data source storage service with managed identity credentials.\r\n" +
          "Provide here the Client id of the managed identity (user-assigned or system-assigned). Use \"system\" for system-assigned identity." +
          "The managed identity needs to be configured in the azure service that currently runs this tool.",
          ShortName = "storageMi", Mandatory = false)]
        public string ConnectToStorageWithManagedIdentity = null;

        [CommandLineArg(
            "pattern",
            "A wildcard pattern, such as ‘MyData*.csv’, that indicates which source data files or blobs to ingest. This includes the folder path.",
            Mandatory = false)]
        public string Pattern = "*.csv";

        [CommandLineArg(
            "prefix",
            "When the source data resides on blob storage, this is the URL prefix shared by all files or blobs, excluding the container name.\r\n" +
            "For example, 'Dir1/Dir2'",
            Mandatory = false)]
        public string Prefix = null;

        [CommandLineArg(
            "tag",
            "A name/value pair (or simply a string) that is attached to the ingested data as ‘extent tags’. This switch can be used multiple times.\r\n" +
            "See https://docs.microsoft.com/azure/kusto/management/extents-overview#extent-tagging for details",
            Mandatory = false)]
        public string[] Tags = null;

        [CommandLineArg(
            "creationTimePattern",
            "When set, will be used to extract the CreationTime property from the file/blob path.",
            Mandatory = false)]
        public string CreationTimePattern = null;
        #endregion

        #region Source data format and mapping to target table, usually stable across multiple ingestion jobs
        [CommandLineArg(
            "format",
            @"Data format. For the list of supported formats, consult https://docs.microsoft.com/azure/kusto/management/data-ingestion/#supported-data-formats",
            ShortName = "f", Mandatory = false)]
        public string Format = string.Empty;

        [CommandLineArg(
            "zipPattern",
            "String value indicating the regular expression to use when selecting which files in a ZIP archive to ingest. All other files in the archive will be ignored. For example: -zipPattern:\"*.csv\"",
            Mandatory = false)]
        public string ZipPattern = string.Empty;

        [CommandLineArg(
            "ignoreFirstRow",
            "Ignores the first row in a file. This is useful when ingesting CSV or similar data that has a header row.",
            ShortName = "ignoreFirst", Mandatory = false)]
        public bool IgnoreFirstRecord = false;

        [CommandLineArg(
            "ingestionMappingPath",
            @"Path to the ingestion mapping file. See https://docs.microsoft.com/azure/kusto/management/mappings for details.",
            ShortName = "mappingPath", Mandatory = false)]
        public string IngestionMappingPath = String.Empty;

        [CommandLineArg(
            "ingestionMappingRef",
            @"Pre-created ingestion mapping name to use. See https://docs.microsoft.com/azure/kusto/management/mappings for details.",
            ShortName = "mappingRef", Mandatory = false)]
        public string IngestionMappingName = String.Empty;

        [CommandLineArg(
            "dontWait",
            "If set to 'true', does not wait for ingestion completion. Useful when ingesting large amounts of files/blobs.",
            Mandatory = false, DefaultValue = false)]
        public bool DontWait = false;

        [CommandLineArg(
            "interactive",
            "If set to 'false', does not prompt for arguments confirmation. For unattended flows and non-interactive environments.",
            ShortName = "i", Mandatory = false, DefaultValue = true)]
        public bool InteractiveMode = true;
        #endregion
    }

    internal class ExtendedCommandLineArgs : CommandLineArgs
    {
        #region Advanced arguments
        [CommandLineArg(
            "compression",
            "Compression ratio hint. Useful when ingesting compressed files/blobs to help Kusto assess the raw data size.",
            ShortName = "cr", Mandatory = false)]
        public double EstimatedCompressionRatio = Constants.DefaultCompressionRatio;
        #endregion

        [CommandLineArg(
            "noSizeLimit",
            "Ignore built-in file size limitations. Will attempt to ingest file of any size.",
            Mandatory = false, DefaultValue = false)]
        public bool NoSizeLimit = false;

        [CommandLineArg(
            "limit",
            "Limit to first N files.",
            ShortName = "l", Mandatory = false)]
        public int Limit = -1;

        [CommandLineArg(
            "listOnly",
            "Only lists the files/blobs that would have been posted for ingestion, without performing the ingestion itself.",
            ShortName = "list", Mandatory = false)]
        public bool ListOnly = false;

        [CommandLineArg(
            "ingestTimeout",
            "Timeout (minutes) for the ingest operations. Defaults to 60 minutes.",
            Mandatory = false, DefaultValue = 60)]
        public int IngestTimeoutInMinutes = 60;

        #region Parameters to tune the ingestion process (perf-wise)
        [CommandLineArg(
            "forceSync",
            "Forces synchronous ingestion for direct ingestion mode",
            Mandatory = false)]
        public bool? ForceSync = null;

        [CommandLineArg(
            "dataBatchSize",
            "Batch size of data ingestion chunks in MBs",
            ShortName = "batch", Mandatory = false)]
        public int? BatchSizeInMBs = null;

        [CommandLineArg(
            "filesInBatch",
            "Maximal number of blobs in a single direct ingest command",
            ShortName = "filesInBatch", Mandatory = false)]
        public int? FilesInBatch = null;

        [CommandLineArg(
            "parallelRequests",
            "Max parallel direct ingestion requests",
            ShortName = "parallel", Mandatory = false)]
        public int? ParallelRequests = null; //10;

        [CommandLineArg(
            "devTracing",
            "If set, write trace logs to a local directory (by default, 'RollingLogs' in the current directory, or can be modified by setting the switch value).",
            ShortName = "trace", AllowNull = true)]
        public string DevTracing = "*";
        #endregion

        #region Parameters to re-ingest data periodically for ingestion tests
        [CommandLineArg("repeatPause", "The period of time to pause between repetitions", ShortName = "pause", Mandatory = false)]
        public int RepeatPauseInSeconds = -1;

        [CommandLineArg("repeatCount", "Number of repetitions of the whole ingest cycle (0: no repetitions, -1: repeat indefinitely)", ShortName = "repeat", Mandatory = false)]
        public int RepeatCount = 0;
        #endregion

        #region Parameters to tune the rate of requests
        [CommandLineArg("ingestionRateCount", "Number of blobs queued in one second.", ShortName = "rateCount", Mandatory = false, DefaultValue = 500)]
        public uint IngestionRateCount = 500;

        [CommandLineArg("listingRateCount", "Number of blobs listed in one second.", ShortName = "listRateCount", Mandatory = false, DefaultValue = 500)]
        public uint ListingRateCount = 500;
        #endregion

        public string FormattedParametersSummary()
        {
            var esb = new ExtendedStringBuilder();
            esb.Indent();

            esb.AppendLine($"Connection string          : {ConnectionString}");
            if (!string.IsNullOrWhiteSpace(ConnectWithManagedIdentity)) { esb.AppendLine($"-managedIdentity           : {ConnectWithManagedIdentity}"); }

            esb.AppendLine($"-database                  : {DatabaseName}");
            esb.AppendLine($"-table                     : {TableName}");
            esb.AppendLine();

            esb.AppendLine($"-sourcePath                : {SourcePath}");
            if (!string.IsNullOrWhiteSpace(ConnectToStorageWithUserAuth)) { esb.AppendLine($"-connectToStorageWithUserAuth : {ConnectToStorageWithUserAuth}"); }
            if (!string.IsNullOrWhiteSpace(IngestWithManagedIdentity)) { esb.AppendLine($"-ingestWithManagedIdentity : {IngestWithManagedIdentity}"); }
            if (!string.IsNullOrWhiteSpace(ConnectToStorageWithManagedIdentity)) { esb.AppendLine($"-ConnectToStorageWithManagedIdentity : {ConnectToStorageWithManagedIdentity}"); }
            if (!string.IsNullOrWhiteSpace(ConnectToStorageLoginUri)) { esb.AppendLine($"-connectToStorageLoginUri : {ConnectToStorageLoginUri}"); }

            if (!string.IsNullOrWhiteSpace(Prefix)) { esb.AppendLine($"-prefix                    : {Prefix}"); }
            esb.AppendLine($"-pattern                   : {Pattern}");
            esb.AppendLine($"-creationTimePattern       : {CreationTimePattern}");
            esb.AppendLine($"-format                    : {Format}");
            if (!string.IsNullOrWhiteSpace(ZipPattern)) { esb.AppendLine($"-zipPattern                : {ZipPattern}"); }
            esb.AppendLine($"-ignoreFirstRow            : {IgnoreFirstRecord}");
            if (!string.IsNullOrWhiteSpace(IngestionMappingPath)) { esb.AppendLine($"-ingestionMappingPath      : {IngestionMappingPath}"); }
            if (!string.IsNullOrWhiteSpace(IngestionMappingName)) { esb.AppendLine($"-ingestionMappingRef       : {IngestionMappingName}"); }
            if (Tags.SafeFastAny()) { esb.AppendLine($"-tags                      : [{Tags.SafeFastStringJoin(", ")}]"); }
            esb.AppendLine();

            esb.AppendLine($"-compression               : {EstimatedCompressionRatio}");
            esb.AppendLine($"-ingestTimeout (min)       : {IngestTimeoutInMinutes}");
            esb.AppendLine($"-noSizeLimit               : {NoSizeLimit}");
            if (Limit >= 0) { esb.AppendLine($"-limit                     : {Limit}"); }
            if (ListOnly)   { esb.AppendLine($"-listOnly                  : {ListOnly}"); }
            esb.AppendLine($"-dontWait                  : {DontWait}");
            esb.AppendLine();

            if (ForceSync.HasValue) { esb.AppendLine($"-forceSync                 : {ForceSync.Value}"); }
            if (BatchSizeInMBs.HasValue) { esb.AppendLine($"-dataBatchSize (MB)        : {BatchSizeInMBs.Value}"); }
            if (FilesInBatch.HasValue) { esb.AppendLine($"-filesInBatch              : {FilesInBatch.Value}"); }
            if (ParallelRequests.HasValue) { esb.AppendLine($"-parallelRequests          : {ParallelRequests.Value}"); }
            if (RepeatPauseInSeconds >= 0) { esb.AppendLine($"-repeatPause (sec)         : {RepeatPauseInSeconds}"); }
            if (RepeatCount > 0) { esb.AppendLine($"-repeatCount               : {RepeatCount}"); }
            if (!string.Equals(DevTracing, "*")) { esb.AppendLine($"-trace                     : {DevTracing}"); }
            esb.AppendLine($"-ingestionRateCount        : {IngestionRateCount}");
            esb.AppendLine($"-listingRateCount        : {IngestionRateCount}");

            esb.Unindent();
            return esb.ToString();
        }
    }
    #endregion

    #region class Program
    internal class Program
    {
        private static readonly string[] m_basicHelpHints = { "/?", "-?", "?", "/help", "-help", "help" };
        private static readonly string[] m_advancedHelpHints = { "/??", "-??", "??", "/helpall", "-helpall", "helpall" };

        private KustoConnectionStringBuilder m_kcsb = null;
        private KustoConnectionStringBuilder m_engineKcsb = null;
        private string m_targetServiceType = null;
        private IDictionary<string, string> m_additionalProperties = new Dictionary<string, string>();
        private IList<string> m_tags = null;
        private DataSourceFormat? m_dataFormat = null;
        private AdditionalArguments m_additionalArguments = null;

        private static Program s_instance;
        private ExtendedCommandLineArgs m_args;
        private LoggerTracer m_logger;

        static int Main(string[] args)
        {
            Kusto.Cloud.Platform.Utils.Library.Initialize(
                CloudPlatformExecutionMode.HostEnvironment,
                ClientServerProfile.Client,
                typeof(Program).Assembly);

            // Allow tool users to use local credential
            Kusto.Data.KustoConnectionStringBuilder.DefaultPreventAccessToLocalSecretsViaKeywords = false;

            try
            {
                if (args.SafeFastNone() || m_basicHelpHints.SafeFastAny(h => string.Equals(h, args[0], StringComparison.OrdinalIgnoreCase)))
                {
                    PrintUsage(advanced: false);
                    return 1;
                }
                if (m_advancedHelpHints.SafeFastAny(h => string.Equals(h, args[0], StringComparison.OrdinalIgnoreCase)))
                {
                    PrintUsage(advanced: true);
                    return 1;
                }

                s_instance = new Program(args);

                if (s_instance.ConfirmRuntimeArguments())
                {
                    return s_instance.Run();
                }

                Console.WriteLine();
                ExtendedConsole.WriteLine(ConsoleColor.Yellow, "*** Aborted. ***");
                Console.WriteLine();
                return 1;
            }
            finally
            {
            }
        }

        private Program(string[] args)
        {
            m_args = new ExtendedCommandLineArgs();
            CommandLineArgsParser.Parse(args, m_args, autoHelp: true);

            RollingCsvTraceListener2.CreateAndInitializeByCommandLineUtilitiesIfNeeded(
                toolAssembly: typeof(Program).Assembly,
                commandLineDevTracingValue: m_args.DevTracing);

            m_logger = new LoggerTracer(SharedTracer.Tracer);
            m_logger.LogVerbose($"LightIngest invoked with the following arguments: {args.SafeFastStringJoin(" ")}");
        }

        private bool ConfirmRuntimeArguments()
        {
            // If Console redirection is on, we're in unattended mode
            if (m_args.InteractiveMode == false || Console.IsInputRedirected)
            {
                return true;
            }

            // Print the arguments to make sure this is what the caller intends to do
            Console.WriteLine();
            ExtendedConsole.WriteLine(ConsoleColor.Cyan, "Please review the run parameters:");
            Console.WriteLine();
            ExtendedConsole.WriteLine(ConsoleColor.Gray, m_args.FormattedParametersSummary());
            Console.WriteLine();
            ExtendedConsole.Write(ConsoleColor.Cyan, "Press [Ctrl+Q] to abort, press any other key or wait for 10 seconds to proceed ");

            var key = Utilities.ReadKeyWithTimeout(TimeSpan.FromSeconds(10.0));
            Console.WriteLine();

            if (key.HasValue)
            {
                if (key.Value.Modifiers.HasFlag(ConsoleModifiers.Control) && key.Value.Key == ConsoleKey.Q)
                {
                    return false;
                }
            }

            return true;
        }

        private static void PrintUsage(bool advanced = false)
        {
            var esb = new ExtendedStringBuilder();
            if (advanced)
            {
                CommandLineArgsParser.WriteHelpString(esb, new ExtendedCommandLineArgs());
            }
            else
            {
                CommandLineArgsParser.WriteHelpString(esb, new CommandLineArgs());
            }
            esb.AppendLine();
            esb.AppendLine("Usage examples:");
            esb.AppendLine();
            esb.Indent();
            esb.AppendLine(@"[Ingest JSON data from blobs]");
            esb.AppendLine(@"LightIngest ""https://ingest-contoso.kusto.windows.net;Federated=true""");
            esb.Indent();
            esb.AppendLine(@"-database:MyDb");
            esb.AppendLine(@"-table:MyTable");
            esb.AppendLine(@"-sourcePath:""https://ACCOUNT_NAME.blob.core.windows.net/CONTAINER_NAME?SAS_TOKEN""");
            esb.AppendLine(@"-prefix:MyDir1/MySubDir2");
            esb.AppendLine(@"-format:json");
            esb.AppendLine(@"-mappingRef:DefaultJsonMapping");
            esb.AppendLine(@"-pattern:*.json");
            esb.AppendLine(@"-limit:100");
            esb.Unindent();
            esb.AppendLine();
            esb.AppendLine(@"[Ingest CSV data with headers from local files]");
            esb.AppendLine(@"LightIngest ""https://ingest-contoso.kusto.windows.net;Federated=true""");
            esb.Indent();
            esb.AppendLine(@"-database:MyDb");
            esb.AppendLine(@"-table:MyTable");
            esb.AppendLine(@"-sourcePath:""D:\MyFolder\Data""");
            esb.AppendLine(@"-format:csv");
            esb.AppendLine(@"-ignoreFirstRow:true");
            esb.AppendLine(@"-mappingPath:""D:\MyFolder\CsvMapping.txt""");
            esb.AppendLine(@"-pattern:*.csv.gz");
            esb.AppendLine(@"-limit:100");
            esb.Unindent();
            esb.AppendLine();
            esb.AppendLine(@"Use LighIngest /?? for advanced options");
            esb.Unindent();
            esb.AppendLine();

            Console.WriteLine(esb.ToString());
            Environment.Exit(1);
        }

        private int Run()
        {
            try
            {
                // Setup the connection string: if database name argument is specified explicitly, it overrides the one provided in the connection string
                m_args.ConnectionString = WellKnownConnectionStrings.GetConnectionStringByAliasOrNull(m_args.ConnectionString) ?? m_args.ConnectionString;

                m_kcsb = CreateKcsbFromArgs();
                using (var adminClient = KustoClientFactory.CreateCslAdminProvider(m_kcsb))
                {
                    m_targetServiceType = GetTargetServiceType(adminClient);
                    m_engineKcsb = string.Equals(m_targetServiceType, KustoIngestionConstants.EngineClusterServiceType, StringComparison.OrdinalIgnoreCase)
                        ? m_kcsb
                        : GetEngineKcsb(adminClient);
                }

                m_dataFormat = ParseDataFormatFromArgs();
                m_additionalProperties = ParseAdditionalPropertiesFromArgs();
                m_tags = ParseTagsFromArgs();
                m_additionalArguments = ParseAdditionalArgumentsFromArgs();

                ValidateArgumentsCoherency();

                // Generate ingestion properties to be used for this session
                var ingestionProperties = CreateIngestionProperties();

                // Initialize our ingestor
                var ingestor = Ingestor.CreateFromCommandLineArgs(m_args, m_additionalArguments, ingestionProperties, m_logger);
                // Iterations == 1 + Repetitions. Negative values indicate infinite loop
                int iterations = (m_args.RepeatCount >= 0 ? m_args.RepeatCount + 1 : Int32.MaxValue);

                for (int i = 1; i <= iterations; i++)
                {
                    RunIngest(ingestor);

                    if (m_args.RepeatPauseInSeconds > 0 && i < iterations)
                    {
                        m_logger.LogInfo($"Pausing for {m_args.RepeatPauseInSeconds} seconds before next iteration");
                        Thread.Sleep(TimeSpan.FromSeconds(m_args.RepeatPauseInSeconds));
                    }
                }

                return 0;
            }
            catch (Exception ex)
            {
                m_logger.LogError(ex.Message);
                return 2;
            }
        }

        private KustoConnectionStringBuilder CreateKcsbFromArgs(string connectionString = null)
        {
            var kcsb = new KustoConnectionStringBuilder(connectionString ?? m_args.ConnectionString);
            if (string.IsNullOrWhiteSpace(m_args.DatabaseName))
            {
                m_args.DatabaseName = kcsb.InitialCatalog;
            }
            else
            {
                kcsb.InitialCatalog = m_args.DatabaseName;
            }

            // Add a touch for managed identities:
            if (!string.IsNullOrWhiteSpace(m_args.ConnectWithManagedIdentity))
            {
                if (kcsb.FederatedSecurity || kcsb.DstsFederatedSecurity)
                {
                    kcsb.EmbeddedManagedIdentity = m_args.ConnectWithManagedIdentity;
                }
                else
                {
                    throw new UtilsArgumentException($"Command line arguments error. 'ManagedIdentity' can only be used with federated authentication.", null);
                }
            }
            
            kcsb.SetConnectorDetails("LightIngest", Assembly.GetExecutingAssembly().GetProductVersionString() , sendUser: true);
            
            return kcsb;
        }

        private string GetTargetServiceType(ICslAdminProvider adminClient)
        {
            string serviceType = null;

            try
            {
                var cmd = CslCommandGenerator.GenerateVersionShowCommand();
                var result = adminClient.ExecuteControlCommand<VersionShowCommandResult>(cmd);

                serviceType = result.First().ServiceType;
            }
            catch (Exception ex)
            {
                m_logger.LogWarning($"LightIngest failed to receive response from endpoint at '{m_kcsb.DataSource}'. Error: '{ex.Message}'");
            }

            if (!string.IsNullOrWhiteSpace(serviceType) &&
                (string.Equals(serviceType, KustoIngestionConstants.DmClusterServiceType, StringComparison.OrdinalIgnoreCase) ||
                 string.Equals(serviceType, KustoIngestionConstants.EngineClusterServiceType, StringComparison.OrdinalIgnoreCase)))
            {
                return serviceType;
            }

            throw new UtilsArgumentException($"Invalid service URI specified: '{m_kcsb.DataSource}'. Please make sure you are using the correct URI and that the service is accessible.", null);
        }

        private KustoConnectionStringBuilder GetEngineKcsb(ICslAdminProvider adminClient)
        {
            try
            {
                var cmd = CslCommandGenerator.GenerateDmTargetQueryServiceUriShowCommand();
                var result = adminClient.ExecuteControlCommand<TargetQueryServiceUriShowCommandResult>(cmd);
                var engineUri = result.First().QueryServiceUri;

                if (!string.IsNullOrWhiteSpace(engineUri))
                {
                    return new KustoConnectionStringBuilder(m_kcsb)
                    {
                        DataSource = engineUri
                    };
                }
            }
            catch
            {
                // Remove this warning as it scares users and make them drop
                // m_logger.LogWarning($"LightIngest failed to receive response from endpoint at '{m_kcsb.DataSource}'. Error: '{ex.Message}'");
            }

            return CreateKcsbFromArgs(m_args.ConnectionString.Replace("ingest-", string.Empty));
        }

        private DataSourceFormat? ParseDataFormatFromArgs()
        {
            if (string.IsNullOrEmpty(m_args.Format))
            {
                return null;
            }
            DataSourceFormat dsf;
            if (!Enum.TryParse<DataSourceFormat>(value: m_args.Format, ignoreCase: true, result: out dsf))
            {
                throw new UtilsArgumentException(
                    $"Command line arguments error. Format '{m_args.Format}' is not supported. Supported formats are: '{string.Join(", ", Enum.GetNames(typeof(DataSourceFormat)))}'.",
                    null);
            }

            return dsf;
        }

        private AdditionalArguments ParseAdditionalArgumentsFromArgs()
        {
            var additionalArgs = new AdditionalArguments();

            if (!string.IsNullOrEmpty(m_args.CreationTimePattern))
            {
                var parts = m_args.CreationTimePattern.Split(new char[] { '\'' }, StringSplitOptions.RemoveEmptyEntries);
                if (parts.Length != 3)
                {
                    throw new UtilsArgumentException(
                        $"Command line arguments error. Failed to parse argument 'creationTimePattern' [{m_args.CreationTimePattern}]. Expected syntax: -creationTimePattern:\"'prefix'DateTime format'suffix'\".",
                        null);
                }
                additionalArgs.DateTimePattern = new DateTimeFormatPattern(prefix: parts[0], format: parts[1], suffix: parts[2]);
            }

            return additionalArgs;
        }

        private IDictionary<string, string> ParseAdditionalPropertiesFromArgs()
        {
            var additionalProperties = new Dictionary<string, string>();

            if (!string.IsNullOrWhiteSpace(m_args.IngestionMappingPath) && !string.IsNullOrWhiteSpace(m_args.IngestionMappingName))
            {
                throw new UtilsArgumentException("Command line arguments error. At most one of {{IngestionMappingPath, IngestionMappingRef}} arguments can be set.", null);
            }

            // Ingestion mapping arguments must be accompanied by format argument
            if ((!string.IsNullOrWhiteSpace(m_args.IngestionMappingPath) || !string.IsNullOrWhiteSpace(m_args.IngestionMappingName)) &&
                m_dataFormat.HasValue == false)
            {
                throw new UtilsArgumentException(
                    $"Command line arguments error. Format argument is mandatory when IngestionMappingPath or IngestionMappingRef are set.", null);
            }

            if (!string.IsNullOrWhiteSpace(m_args.IngestionMappingPath))
            {
                if (!File.Exists(m_args.IngestionMappingPath))
                {
                    throw new UtilsArgumentException($"Command line arguments error. Ingestion mapping file '{m_args.IngestionMappingPath}' was not found.", null);
                }

                var mappingObject = File.ReadAllText(m_args.IngestionMappingPath).Replace(Environment.NewLine, " ");
                additionalProperties.Add(KustoIngestionProperties.IngestionMappingPropertyName, mappingObject);
                additionalProperties.Add(KustoIngestionProperties.IngestionMappingKindPropertyName, m_dataFormat.Value.ToIngestionMappingKind().ToString());
            }

            if (!string.IsNullOrWhiteSpace(m_args.IngestionMappingName))
            {
                additionalProperties.Add(KustoIngestionProperties.IngestionMappingReferencePropertyName, m_args.IngestionMappingName);
                additionalProperties.Add(KustoIngestionProperties.IngestionMappingKindPropertyName, m_dataFormat.Value.ToIngestionMappingKind().ToString());
            }

            if (m_args.IgnoreFirstRecord)
            {
                additionalProperties.Add(Constants.IgnoreFirstRecordPropertyName, true.ToString());
            }

            if (!string.IsNullOrWhiteSpace(m_args.ZipPattern))
            {
                additionalProperties.Add(Constants.ZipPatternPropertyName, m_args.ZipPattern);
            }

            return additionalProperties;
        }

        private IList<string> ParseTagsFromArgs()
        {
            if (m_args.Tags.SafeFastNone())
            {
                return null;
            }

            var tags = new List<string>(m_args.Tags.Length);

            foreach (var t in m_args.Tags)
            {
                if (string.IsNullOrWhiteSpace(t))
                {
                    continue;
                }

                tags.Add(t.SafeFastTrim());
            }

            return tags;
        }

        private void ValidateArgumentsCoherency()
        {
            // Queued ingestion
            if (string.Equals(m_targetServiceType, KustoIngestionConstants.DmClusterServiceType, StringComparison.OrdinalIgnoreCase))
            {
                var sb = new StringBuilder();

                if (m_args.ForceSync.HasValue)
                {
                    sb.Append("forceSync");
                }
                if (m_args.BatchSizeInMBs.HasValue)
                {
                    sb.Append(", dataBatchSize");
                }
                if (m_args.FilesInBatch.HasValue)
                {
                    sb.Append(", filesInBatch");
                }
                if (m_args.ParallelRequests.HasValue)
                {
                    sb.Append(", parallelRequests");
                }

                if (sb.Length > 0)
                {
                    throw new UtilsArgumentException(
                        $"Command line arguments error. The following arguments are not supported when working with '{m_kcsb.ServiceName}' endpoint: '{sb.ToString()}'", null);
                }
            }

            ValidateSourcePath();

            if (!string.IsNullOrEmpty(m_args.ConnectToStorageWithManagedIdentity))
            {
                if (!m_args.ConnectToStorageWithManagedIdentity.Equals(AadManagedIdentityTokenCredentialsProvider.SystemAssignedManagedIdentityKeyword) && 
                    !Guid.TryParse(m_args.ConnectToStorageWithManagedIdentity, out _))
                {
                    throw new UtilsArgumentException(
                       $"Command line arguments error. If 'ConnectToStorageWithManagedIdentity' expected either Guid or \"system\".", null);
                }
               
            }
        }

        private void ValidateSourcePath()
        {
            if (Utilities.IsFileSystemPath(m_args.SourcePath))
            {
                if (!string.IsNullOrWhiteSpace(m_args.Prefix))
                {
                    throw new UtilsArgumentException(
                        $"Command line arguments error. 'prefix' argument is only supported for Azure Blob inputs", null);
                }
            }
            else if (!Utilities.IsBlobStorageUri(m_args.SourcePath, out var error))
            {
                throw new UtilsArgumentException(
                    $"Command line arguments error. 'sourcePath' argument '{m_args.SourcePath}' does not indicate a valid file system path or blob URI." +
                    (string.IsNullOrWhiteSpace(error) ? string.Empty : $" Error: '{error}'"), null);
            }
        }

        private KustoQueuedIngestionProperties CreateIngestionProperties()
        {
            var ingestionProperties = new KustoQueuedIngestionProperties(m_args.DatabaseName, m_args.TableName)
            {
                ReportLevel = IngestionReportLevel.FailuresOnly,
                ReportMethod = IngestionReportMethod.Table
            };

            if (m_additionalProperties.SafeFastAny())
            {
                ingestionProperties.AdditionalProperties = m_additionalProperties;
            }

            if (m_dataFormat.HasValue)
            {
                ingestionProperties.Format = m_dataFormat.Value;
            }

            if (m_tags.SafeFastAny())
            {
                ingestionProperties.AdditionalTags = m_tags;
            }

            return ingestionProperties;
        }

        private void RunIngest(Ingestor ingestor)
        {
            Ensure.ArgIsNotNull(ingestor, nameof(ingestor));
            Ensure.ArgIsNotNull(m_kcsb, nameof(m_kcsb));

            if (m_args.ListOnly)
            {
                ingestor.RunIngestSimulation();
            }
            else
            {
                // Check permissions on target table
                CheckPermissionsOnTable();
#if OPEN_SOURCE_COMPILATION
                if (!string.Equals(m_targetServiceType, KustoIngestionConstants.DmClusterServiceType, StringComparison.OrdinalIgnoreCase))
                {
                    throw new Exception("Support only Ingest service target type in open source compilation. Add '-ingest' to the url host.");
                }
#endif

                if (string.Equals(m_targetServiceType, KustoIngestionConstants.DmClusterServiceType, StringComparison.OrdinalIgnoreCase))
                {
                    ingestor.RunQueuedIngest(m_kcsb);
                }
                else
                {
                    ingestor.RunDirectIngest(m_kcsb);
                }
            }
        }

        private void CheckPermissionsOnTable()
        {
            Ensure.ArgIsNotNull(m_engineKcsb, nameof(m_engineKcsb));

            using (var client = KustoClientFactory.CreateCslAdminProvider(m_engineKcsb))
            {
                var showPrincipalAccessCommand = CslCommandGenerator.GenerateShowPrincipalAccessCommand("ingest", m_args.DatabaseName, m_args.TableName, useCurrentPrincipal: true);
                var isAllowed = true;

                try
                {
                    var result = client.ExecuteControlCommand<ShowPrincipalAccessCommandResult>(m_args.DatabaseName, showPrincipalAccessCommand).Single();
                    isAllowed = result.IsAllowed;
                }
                catch
                { }

                if (!isAllowed)
                {
                    throw new Exception($"Current principal is not authorized to ingest data into table '{m_args.TableName}' in database '{m_args.DatabaseName}'");
                }
            }
        }
    }

#endregion

    #region class PrivateTracer
    internal class SharedTracer : TraceSourceBase<SharedTracer>
    {
        /// <summary>
        /// The string that identifies this trace source
        /// </summary>
        public const string IdentifierString = "LightIngest";

        /// <summary>
        /// Implements <see cref="TraceSourceBase{T}.Id"/>
        /// </summary>
        public override String Id
        {
            get { return IdentifierString; }
        }

        /// <summary>
        /// Implements <see cref="TraceSourceBase{T}.DefaultVerbosity"/>
        /// </summary>
        public override TraceVerbosity DefaultVerbosity
        {
            get { return TraceVerbosity.Info; }
        }
    }
    #endregion
}
