// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

using Kusto.Cloud.Platform.Azure.Storage.PersistentStorage;
using Kusto.Cloud.Platform.Data;
using Kusto.Cloud.Platform.Modularization;
using Kusto.Cloud.Platform.Security;
using Kusto.Cloud.Platform.Storage.PersistentStorage;
using Kusto.Cloud.Platform.Utils;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using Kusto.Ingest;
using Kusto.Common.Svc.Storage;

namespace LightIngest
{
    internal class Ingestor
    {
        #region Data members
        private const int c_BatchesLimitForSyncIngest = 64;
        private const int c_DefaultDirectIngestBatchSizeBytes = 500 * MemoryConstants._1MB;
        private const int c_MaxParallelismDegree = 256;

        private readonly ExtendedCommandLineArgs m_args;
        private readonly KustoQueuedIngestionProperties m_ingestionProperties;
        private LoggerTracer m_logger;
        private IPersistentStorageFactory m_persistentStorageFactory;
        private readonly bool m_bFileSystem;
        private readonly bool m_bWaitForIngestCompletion = false;
        private readonly TimeSpan m_ingestCompletionTimeout = TimeSpan.Zero;
        private Regex m_patternRegex = null;

        private DateTimeFormatPattern m_creationTimeInNamePattern = null;

        private readonly double m_estimatedCompressionRatio = Constants.DefaultCompressionRatio;
        private readonly int m_directIngestParallelRequests = 1;
        private readonly long m_directIngestBatchSizeLimitInBytes = 0;
        private readonly int m_directIngestFilesLimitPerBatch = 0;
        private readonly bool m_bDirectIngestUseSyncMode = false;
        private FixedWindowThrottlerPolicy m_fixedWindowThrottlerPolicy;

        private object m_objectsListingLock = new object();

        // Ingestion results for queued ingest flow when confirmation is required
        private object m_ingestionResultsLock = new object();
        private IList<IKustoIngestionResult> m_ingestionResults = null;

        // Intermediate placeholder for direct ingest flow
        private object m_listIntermediateSourcesLock = new object();
        private IList<DataSource> m_listIntermediateSources = new List<DataSource>();

        // Operations tracking for direct ingest flow
        private object m_operationResultsLock = new object();
        private IList<OperationsShowCommandResult> m_operationResults = new List<OperationsShowCommandResult>();

        // Pipeline counters
        private readonly int m_objectsCountQuota = 0;
        private long m_objectsListed = 0;
        private long m_objectsAccepted = 0;

        // Queued ingest counters
        private long m_objectsPosted = 0;

        // Direct ingest counters
        private long m_filesUploaded = 0;
        private long m_batchesProduced = 0;
        private long m_batchesIngested = 0;
        private Disposer m_disposer;

        // PSL
        private BlobPersistentStorageFactory2 m_blob;
        #endregion // Data members

        #region Statistics methods
        private string BasicCountersSnapshot()
        {
            return $"Items discovered: [{Interlocked.Read(ref m_objectsListed),7}], filtered: [{Interlocked.Read(ref m_objectsAccepted),7}]";
        }

        private string QueuedIngestStats()
        {
            return $"{BasicCountersSnapshot()}, posted for ingestion: [{Interlocked.Read(ref m_objectsPosted),7}]";
        }

        private string DirectIngestStats()
        {
            return $"{BasicCountersSnapshot()}, uploaded to blob store: [{Interlocked.Read(ref m_filesUploaded),7}]. " +
                   $"Batches produced: [{Interlocked.Read(ref m_batchesProduced),7}], ingested: [{Interlocked.Read(ref m_batchesIngested),7}]";
        }
        #endregion Statistics methods

        #region Construction and initialization
        private Ingestor(ExtendedCommandLineArgs args, AdditionalArguments additionalArgs, KustoQueuedIngestionProperties ingestionProperties, LoggerTracer logger)
        {
            m_args = args;
            m_ingestionProperties = ingestionProperties;
            m_logger = logger;

            m_ingestionProperties.ReportLevel = IngestionReportLevel.None;
            m_ingestionProperties.ReportMethod = IngestionReportMethod.Queue;

            m_bFileSystem = Utilities.IsFileSystemPath(m_args.SourcePath);

            m_estimatedCompressionRatio = args.EstimatedCompressionRatio;

            m_directIngestParallelRequests =
                (args.ParallelRequests.HasValue && args.ParallelRequests.Value >= 1) ? Math.Min(args.ParallelRequests.Value, c_MaxParallelismDegree) : 8;
            m_directIngestBatchSizeLimitInBytes =
                (args.BatchSizeInMBs.HasValue && args.BatchSizeInMBs.Value > 0) ? args.BatchSizeInMBs.Value * MemoryConstants._1MB : c_DefaultDirectIngestBatchSizeBytes;
            m_directIngestFilesLimitPerBatch =
                (args.FilesInBatch.HasValue && args.FilesInBatch.Value >= 0) ? args.FilesInBatch.Value : 0;
            m_bDirectIngestUseSyncMode = args.ForceSync ?? false;

            m_objectsCountQuota = m_args.Limit;

            if (!string.IsNullOrEmpty(m_args.Pattern) && !string.Equals(m_args.Pattern, "*", StringComparison.Ordinal))
            {
                string regexExpression = Regex.Escape(m_args.Pattern).Replace(@"\*", ".*").Replace(@"\?", ".") + "$";
                m_patternRegex = new Regex(regexExpression, RegexOptions.Compiled);
            }

            m_creationTimeInNamePattern = additionalArgs.DateTimePattern;

            m_ingestCompletionTimeout = TimeSpan.FromMinutes(m_args.IngestTimeoutInMinutes);
            m_bWaitForIngestCompletion = (!m_args.DontWait && m_ingestCompletionTimeout > TimeSpan.Zero);

            if (m_bWaitForIngestCompletion)
            {
                m_ingestionProperties.ReportLevel = IngestionReportLevel.FailuresAndSuccesses;
                m_ingestionProperties.ReportMethod = IngestionReportMethod.Table;
                m_ingestionResults = new List<IKustoIngestionResult>();
            }
            else
            {
                m_ingestionProperties.ReportLevel = IngestionReportLevel.None;
                m_ingestionProperties.ReportMethod = IngestionReportMethod.Queue;
            }

            m_ingestionProperties.IgnoreSizeLimit = m_args.NoSizeLimit;

            m_fixedWindowThrottlerPolicy = new FixedWindowThrottlerPolicy(args.IngestionRateCount, TimeSpan.FromSeconds(args.IngestionRateTime));
            InitPSLFields();
        }

        private void InitPSLFields()
        {
            m_disposer = new Disposer(GetType().FullName, "LightIngest");
            var serviceLocator = new ServiceLocator();
            var hostnameValidatorFactory = new VoidServiceCalloutHostnameValidatorFactory();
            var persistentStorageManager = KustoPersistentStorageManager.CreateAndRegister(
                "LightIngest", serviceLocator, hostnameValidatorFactory, featureFlags: null, registerOnelakeFactory: false);
            m_persistentStorageFactory = persistentStorageManager.Factory;
            var azureStorageHostnameValidator = hostnameValidatorFactory.GetServiceCalloutHostnameValidator("AzureStorage");
            m_blob = new BlobPersistentStorageFactory2(azureStorageHostnameValidator);
            m_disposer.Add(persistentStorageManager);
        }

        private void Reset()
        {
            if (m_ingestionResults != null)
            {
                lock (m_ingestionResultsLock)
                {
                    m_ingestionResults.Clear();
                }
            }

            if (m_listIntermediateSources != null)
            {
                lock (m_listIntermediateSourcesLock)
                {
                    m_listIntermediateSources.Clear();
                }
            }

            if (m_operationResults != null)
            {
                lock (m_operationResultsLock)
                {
                    m_operationResults.Clear();
                }
            }

            m_objectsListed = 0;
            m_objectsAccepted = 0;
            m_objectsPosted = 0;
            m_filesUploaded = 0;
            m_batchesProduced = 0;
            m_batchesIngested = 0;
        }

        internal static Ingestor CreateFromCommandLineArgs(ExtendedCommandLineArgs args,
                                                           AdditionalArguments additionalArgs,
                                                           KustoQueuedIngestionProperties ingestionProperties,
                                                           LoggerTracer logger)
        {
            Ensure.ArgIsNotNull(args, nameof(args));
            Ensure.ArgIsNotNull(additionalArgs, nameof(additionalArgs));
            Ensure.ArgIsNotNull(ingestionProperties, nameof(ingestionProperties));
            Ensure.ArgIsNotNull(logger, nameof(logger));

            return new Ingestor(args, additionalArgs, ingestionProperties, logger);
        }
        #endregion Construction and initialization

        internal void RunQueuedIngest(KustoConnectionStringBuilder kcsb)
        {
            Reset();

            var stopwatch = ExtendedStopwatch.StartNew();
            using (var ingestClient = KustoIngestFactory.CreateQueuedIngestClient(kcsb))
            {
                ActionBlock<string> listObjectsBlock = null;
                ActionBlock<IPersistentStorageFile> filterObjectsBlock = null;
                ActionBlock<DataSource> ingestBlock = null;

                if (m_bFileSystem) // Data is in local files
                {
                    ingestBlock = new ActionBlock<DataSource>(
                        record => IngestSingle(record, m_objectsCountQuota, ingestClient, m_bFileSystem, false, m_ingestionProperties),
                        new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = ExtendedEnvironment.RestrictedProcessorCount });

                    listObjectsBlock = new ActionBlock<string>(
                        sourcePath => ListAndFilterFiles(sourcePath, m_args.Pattern, m_objectsCountQuota, ingestBlock),
                        new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 1 });
                    listObjectsBlock.Completion.ContinueWith(delegate { ingestBlock.Complete(); });
                }
                else // Input is in blobs
                {
                    ingestBlock = new ActionBlock<DataSource>(
                        record => IngestSingle(record, m_objectsCountQuota, ingestClient, m_bFileSystem, false, m_ingestionProperties),
                        new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = ExtendedEnvironment.RestrictedProcessorCount });

                    // ListFiles calls PSL EnumerateFiles which accepts a pattern but BlobPersistentStorageFactory2 doesn't use the full pattern
                    // but only its prefix, therefore we still have to filter ourselves.
                    filterObjectsBlock = new ActionBlock<IPersistentStorageFile>(
                        file => FilterFiles(file, m_patternRegex, m_objectsCountQuota, ingestBlock),
                        new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = ExtendedEnvironment.RestrictedProcessorCount });
                    filterObjectsBlock.Completion.ContinueWith(delegate { ingestBlock.Complete(); });

                    listObjectsBlock = new ActionBlock<string>(
                        sourcePath => ListFiles(sourcePath, m_args.Prefix, m_objectsCountQuota, filterObjectsBlock),
                        new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 1 });
                    listObjectsBlock.Completion.ContinueWith(delegate { filterObjectsBlock.Complete(); });
                }
                // Debugging: Allow the debugger to retrieve the DataFlow blocks:
                Kusto.Cloud.Platform.Debugging.RegisterWeakReference(nameof(ingestBlock), ingestBlock);
                Kusto.Cloud.Platform.Debugging.RegisterWeakReference(nameof(filterObjectsBlock), filterObjectsBlock);
                Kusto.Cloud.Platform.Debugging.RegisterWeakReference(nameof(listObjectsBlock), listObjectsBlock);

                m_logger.LogVerbose("==> Starting...");
                listObjectsBlock.Post(m_args.SourcePath);
                listObjectsBlock.Complete();

                bool bPipelineCompleted = false;
                do
                {
                    bPipelineCompleted = ingestBlock.Completion.Wait(TimeSpan.FromSeconds(10));

                    m_logger.LogInfo($"==> {QueuedIngestStats()}");
                } while (!bPipelineCompleted);

                stopwatch.Stop();
            }

            m_logger.LogSuccess($"    Done. Time elapsed: {stopwatch.Elapsed:c}");
            m_logger.LogSuccess($"    {QueuedIngestStats()}");

            // Wait for ingestion completion, if required
            if (m_bWaitForIngestCompletion && m_ingestionResults.SafeFastAny())
            {
                m_logger.LogInfo("==> Waiting for ingestion completion...");
                WaitForIngestionResult(m_ingestionResults, m_ingestCompletionTimeout);
                m_logger.LogInfo("==> Done.");
            }
        }

        internal void RunDirectIngest(KustoConnectionStringBuilder kcsb)
        {
            Reset();

            var stopwatch = ExtendedStopwatch.StartNew();
            var currentPhaseStopwatch = ExtendedStopwatch.StartNew();

            // small patch
            if (m_ingestionProperties.Format.HasValue && !m_ingestionProperties.AdditionalProperties.ContainsKey(KustoIngestionProperties.FormatPropertyName))
            {
                m_ingestionProperties.AdditionalProperties.Add(KustoIngestionProperties.FormatPropertyName, Enum.GetName(typeof(DataSourceFormat), (m_ingestionProperties.Format)));
            }

            using (var kustoClient = KustoClientFactory.CreateCslAdminProvider(kcsb))
            {
                bool bKustoRunningLocally = Utilities.IsLocalKustoConnection(kcsb);

                // Prepare batches for ingest:
                RunPrepareBatchesForDirectIngest(kustoClient, kustoRunningLocally: bKustoRunningLocally);

                currentPhaseStopwatch.Stop();
                m_logger.LogSuccess($"    RunPrepareBatchesForDirectIngest done. Time elapsed: {currentPhaseStopwatch.Elapsed:c}");
                m_logger.LogSuccess($"    {DirectIngestStats()}");

                // Split into batches and ingest...
                m_logger.LogVerbose($"==> Splitting [{m_listIntermediateSources.SafeFastCount()}] sources into batches for ingestion...");
                var batches = SplitIntoBatches(m_listIntermediateSources, m_bFileSystem, m_directIngestBatchSizeLimitInBytes, m_directIngestFilesLimitPerBatch);
                Interlocked.Add(ref m_batchesProduced, batches.SafeFastCount());
                m_logger.LogVerbose($"==> Done. Prepared [{batches.SafeFastCount()}] batches.");
                m_logger.LogSuccess($"    {DirectIngestStats()}");

                currentPhaseStopwatch.Restart();

                // Ingest
                // Twist: if we have small enough number of batches, or command line argument set, we perform a synchronous parallel ingest
                bool bIngestLocally = (bKustoRunningLocally && m_bFileSystem);

                if (!bIngestLocally && (batches.SafeFastCount() <= c_BatchesLimitForSyncIngest || m_bDirectIngestUseSyncMode))
                {
                    m_logger.LogVerbose($"==> Ingesting [{batches.SafeFastCount()}] batches synchronously...");
                    RunSyncDirectIngestInBatches(kustoClient, batches, m_ingestionProperties);
                    m_logger.LogInfo($"==> Ingestion complete.");
                }
                else
                {
                    RunDirectIngestInBatches(kustoClient, batches, bIngestLocally);
                }

                currentPhaseStopwatch.Stop();
                stopwatch.Stop();

                var failedOperations = m_operationResults.Where(r => string.Equals(r.State, "Failed", StringComparison.OrdinalIgnoreCase));
                if (failedOperations.SafeFastNone())
                {
                    m_logger.LogSuccess($"    RunDirectIngestInBatches done. Time elapsed: {currentPhaseStopwatch.Elapsed:c}");
                    m_logger.LogSuccess($"    RunDirectIngest completed without errors. Total time elapsed: {stopwatch.Elapsed:c}");
                    m_logger.LogSuccess($"    {DirectIngestStats()}");
                }
                else
                {
                    m_logger.LogWarning($"    RunDirectIngestInBatches done. Time elapsed: {currentPhaseStopwatch.Elapsed:c}");
                    m_logger.LogWarning($"    RunDirectIngest completed with errors. Total time elapsed: {stopwatch.Elapsed:c}");
                    m_logger.LogWarning($"    {DirectIngestStats()}");

                    m_logger.LogError($"==> [{failedOperations.SafeFastCount()}] out of [{m_batchesIngested}] ingest operations failed:");
                    var cmd = CslCommandGenerator.GenerateIngestionFailuresShowCommand(failedOperations.Select(op => op.OperationId));
                    var result = kustoClient.ExecuteControlCommand<IngestionFailuresSummarizedShowCommandResult>(cmd);
                    result.ForEach(fo =>
                    {
                        m_logger.LogError($"    Failed to ingest data source '{fo.IngestionSourcePath}'.");
                        m_logger.LogError($"    Failure details: {fo.Details}");
                    });
                }
            }
        }

        internal void RunIngestSimulation()
        {
            Reset();

            var stopwatch = ExtendedStopwatch.StartNew();

            ActionBlock<string> listObjectsBlock = null;
            ActionBlock<IPersistentStorageFile> filterObjectsBlock = null;
            ActionBlock<DataSource> simulatedIngestBlock = null;

            if (m_bFileSystem) // Data is in local files
            {
                simulatedIngestBlock = new ActionBlock<DataSource>(
                    record => LogSingleObject(record),
                    new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = ExtendedEnvironment.RestrictedProcessorCount });

                listObjectsBlock = new ActionBlock<string>(
                    sourcePath => ListAndFilterFiles(sourcePath, m_args.Pattern, m_objectsCountQuota, simulatedIngestBlock),
                    new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 1 });
                listObjectsBlock.Completion.ContinueWith(delegate { simulatedIngestBlock.Complete(); });
            }
            else // Input is in blobs
            {
                simulatedIngestBlock = new ActionBlock<DataSource>(
                    record => LogSingleObject(record),
                    new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = ExtendedEnvironment.RestrictedProcessorCount });

                filterObjectsBlock = new ActionBlock<IPersistentStorageFile>(
                    file => FilterFiles(file, m_patternRegex, m_objectsCountQuota, simulatedIngestBlock),
                    new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = ExtendedEnvironment.RestrictedProcessorCount });
                filterObjectsBlock.Completion.ContinueWith(delegate { simulatedIngestBlock.Complete(); });

                listObjectsBlock = new ActionBlock<string>(
                    sourcePath => ListFiles(sourcePath, m_args.Prefix, m_objectsCountQuota, filterObjectsBlock),
                    new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 1 });
                listObjectsBlock.Completion.ContinueWith(delegate { filterObjectsBlock.Complete(); });
            }

            // Debugging: Allow the debugger to retrieve the DataFlow blocks:
            Kusto.Cloud.Platform.Debugging.RegisterWeakReference(nameof(simulatedIngestBlock), simulatedIngestBlock);
            Kusto.Cloud.Platform.Debugging.RegisterWeakReference(nameof(filterObjectsBlock), filterObjectsBlock);
            Kusto.Cloud.Platform.Debugging.RegisterWeakReference(nameof(listObjectsBlock), listObjectsBlock);

            m_logger.LogWarning("*** NO DATA WILL BE INGESTED IN THIS RUN ***" + Environment.NewLine);
            listObjectsBlock.Post(m_args.SourcePath);
            listObjectsBlock.Complete();

            simulatedIngestBlock.Completion.Wait();
            stopwatch.Stop();

            m_logger.LogWarning(Environment.NewLine + "*** NO DATA WAS INGESTED IN THIS RUN ***" + Environment.NewLine);
            m_logger.LogSuccess($"    Done. Time elapsed: {stopwatch.Elapsed:c}");
            m_logger.LogSuccess($"    {BasicCountersSnapshot()}, accepted: [{Interlocked.Read(ref m_objectsAccepted),7}]");
        }

        #region Private helper methods
        private void RunPrepareBatchesForDirectIngest(ICslAdminProvider kustoClient, bool kustoRunningLocally)
        {
            ActionBlock<string> listObjectsBlock = null;
            ActionBlock<IPersistentStorageFile> filterObjectsBlock = null;
            ActionBlock<DataSource> uploadOrAccumulateBlock = null;

            IPersistentStorageContainer tempContainer = null;

            if (m_bFileSystem) // Data is in local files
            {
                // We only need to upload to a real blob container is if we are *not* working with a local Kusto service
                if (!kustoRunningLocally)
                {
                    tempContainer = AcquireTempBlobContainer(kustoClient);
                }

                uploadOrAccumulateBlock = new ActionBlock<DataSource>(
                    record => UploadFiles(record, tempContainer),
                    new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = ExtendedEnvironment.RestrictedProcessorCount });

                listObjectsBlock = new ActionBlock<string>(
                    sourcePath => ListAndFilterFiles(sourcePath, m_args.Pattern, m_objectsCountQuota, uploadOrAccumulateBlock),
                    new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 1 });
                listObjectsBlock.Completion.ContinueWith(delegate { uploadOrAccumulateBlock.Complete(); });
            }
            else // Input is not local files
            {
                uploadOrAccumulateBlock = new ActionBlock<DataSource>(
                    record => AccumulateObjects(record),
                    new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = ExtendedEnvironment.RestrictedProcessorCount });

                filterObjectsBlock = new ActionBlock<IPersistentStorageFile>(
                    file => FilterFiles(file, m_patternRegex, m_objectsCountQuota, uploadOrAccumulateBlock),
                    new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = ExtendedEnvironment.RestrictedProcessorCount });
                filterObjectsBlock.Completion.ContinueWith(delegate { uploadOrAccumulateBlock.Complete(); });

                listObjectsBlock = new ActionBlock<string>(
                    sourcePath => ListFiles(sourcePath, m_args.Prefix, m_objectsCountQuota, filterObjectsBlock),
                    new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 1 });
                listObjectsBlock.Completion.ContinueWith(delegate { filterObjectsBlock.Complete(); });
            }

            // Debugging: Allow the debugger to retrieve the DataFlow blocks:
            Kusto.Cloud.Platform.Debugging.RegisterWeakReference(nameof(listObjectsBlock), listObjectsBlock);
            Kusto.Cloud.Platform.Debugging.RegisterWeakReference(nameof(filterObjectsBlock), filterObjectsBlock);
            Kusto.Cloud.Platform.Debugging.RegisterWeakReference(nameof(uploadOrAccumulateBlock), uploadOrAccumulateBlock);

            m_logger.LogVerbose("==> Flow RunPrepareBatchesForDirectIngest starting...");
            listObjectsBlock.Post(m_args.SourcePath);
            listObjectsBlock.Complete();

            bool bPipelineCompleted = false;
            do
            {
                bPipelineCompleted = uploadOrAccumulateBlock.Completion.Wait(TimeSpan.FromSeconds(10));
                m_logger.LogInfo($"==> {DirectIngestStats()}");
            } while (!bPipelineCompleted);
            m_logger.LogVerbose("==> Flow RunPrepareBatchesForDirectIngest done.");
        }

        private void RunDirectIngestInBatches(ICslAdminProvider kustoClient, IEnumerable<DataSourcesBatch> batches, bool ingestLocally)
        {
            ActionBlock<DataSourcesBatch> ingestBatchesBlock = new ActionBlock<DataSourcesBatch>(
                batch => IngestBatch(batch, kustoClient, ingestLocally, m_ingestionProperties, m_ingestCompletionTimeout),
                new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = m_directIngestParallelRequests });

            // Debugging: Allow the debugger to retrieve the DataFlow blocks:
            Kusto.Cloud.Platform.Debugging.RegisterWeakReference(nameof(ingestBatchesBlock), ingestBatchesBlock);

            m_logger.LogVerbose("==> Flow RunDirectIngestInBatches starting...");
            batches.ForEach(ds => ingestBatchesBlock.Post(ds));

            bool bPipelineCompleted = false;
            do
            {
                bPipelineCompleted = ingestBatchesBlock.Completion.Wait(TimeSpan.FromSeconds(10));
                m_logger.LogInfo($"==> {DirectIngestStats()}");
            } while (!bPipelineCompleted);
            m_logger.LogVerbose("==> Flow RunDirectIngestInBatches done.");
        }

        private void RunSyncDirectIngestInBatches(ICslAdminProvider kustoClient,
                                                            IEnumerable<DataSourcesBatch> batches,
                                                            KustoQueuedIngestionProperties ingestionProperties)
        {
            ExtendedParallel.ForEachEx(batches, m_directIngestParallelRequests, (b) =>
            {
                try
                {
                    var batchUris = b.Sources.Select((s) => s.CloudFileUri).ToList();
                    var cmd = CslCommandGenerator.GenerateTableIngestPullCommand(ingestionProperties.TableName, batchUris, false,
                        extensions: ingestionProperties.AdditionalProperties,
                        tags: ingestionProperties.AdditionalTags);
                    var cmdResult = kustoClient.ExecuteControlCommand<DataIngestPullCommandResult>(ingestionProperties.DatabaseName, cmd);

                    // Get the operation result
                    cmd = CslCommandGenerator.GenerateOperationsShowCommand(cmdResult.First().OperationId);
                    var showOperationResult = kustoClient.ExecuteControlCommand<OperationsShowCommandResult>(cmd);

                    lock (m_operationResultsLock)
                    {
                        m_operationResults.Add(showOperationResult.First());
                    }
                    Interlocked.Increment(ref m_batchesIngested);
                }
                catch (Exception ex)
                {
                    m_logger.LogError($"Error in RunSyncDirectIngestInBatches: {ex.Message}");
                }
            });

        }

        private void ListAndFilterFiles(string sourcePath, string pattern, int filesToTake, ITargetBlock<DataSource> targetBlock)
        {
            try
            {
                m_logger.LogVerbose($"ListAndFilterFiles: enumerating files under '{sourcePath}'");

                // sourcePath is a file path
                if (File.Exists(sourcePath))
                {
                    m_logger.LogVerbose($"ListAndFilterFiles: found 1 file: '{sourcePath}'");
                    Interlocked.Increment(ref m_objectsListed);
                    Interlocked.Increment(ref m_objectsAccepted);
                    targetBlock.SendAsync(new DataSource { FileSystemPath = sourcePath, SizeInBytes = Utilities.TryGetFileSize(sourcePath, m_estimatedCompressionRatio) });
                    return;
                }

                // sourcePath is a directory path
                var files = Directory.EnumerateFiles(sourcePath, pattern, SearchOption.AllDirectories);
                if (files.SafeFastNone())
                {
                    m_logger.LogWarning($"ListAndFilterFiles: files matching the pattern '{pattern}' found under '{sourcePath}' path.");
                    throw new FileNotFoundException($"No files matching the pattern '{pattern}' found under '{sourcePath}' path.");
                }

                int fileCount = (int)files.SafeFastCount();
                Interlocked.Add(ref m_objectsListed, fileCount);

                filesToTake = (filesToTake >= 0 ? Math.Min(filesToTake, fileCount) : fileCount);

                files.SafeFastTake(filesToTake).ForEach((f) =>
                {
                    long fileSize = Utilities.TryGetFileSize(f, m_estimatedCompressionRatio);
                    DateTime? fileCreationTime = Utilities.InferFileCreationTimeUtc(f, m_creationTimeInNamePattern);

                    targetBlock.SendAsync(new DataSource
                    {
                        FileSystemPath = f,
                        SizeInBytes = fileSize,
                        CreationTimeUtc = fileCreationTime
                    });

                    Interlocked.Increment(ref m_objectsAccepted);
                });
            }
            catch (Exception ex)
            {
                m_logger.LogError($"ListAndFilterFiles failed: {ex.Message}");
            }
        }

        private void ListFiles(string sourcePath, string sourceVirtualDirectory,
                               int filesToTake, ITargetBlock<IPersistentStorageFile> targetBlock)
        {
            try
            {
                IPersistentStorageContainer container = m_persistentStorageFactory.CreateContainerRef(sourcePath);

                m_logger.LogVerbose($"ListFiles: enumerating files under container '{sourcePath.SplitFirst(";").SplitFirst("?")}' with prefix '{sourceVirtualDirectory}'");

                if (filesToTake >= 0 && filesToTake <= Interlocked.Read(ref m_objectsAccepted))
                {
                    return;
                }

                var sourceFiles = container.EnumerateFiles(
                    pattern: sourceVirtualDirectory + "*"
                );

                ExtendedParallel.ForEach(sourceFiles, m_directIngestParallelRequests, r =>
                {
                    targetBlock.SendAsync(r);
                    Interlocked.Increment(ref m_objectsListed);
                });

            }
            catch (Exception ex)
            {
                m_logger.LogError($"Error: ListFiles failed: {ex.MessageEx()}");
            }
        }

        private void FilterFiles(IPersistentStorageFile cloudFile, Regex patternRegex, int filesToTake, ITargetBlock<DataSource> targetBlock)
        {
            try
            {
                if (cloudFile != null && (patternRegex == null || patternRegex.IsMatch(cloudFile.GetFileName())))
                {
                    // we are taking this lock here in order to not process more items than specified.
                    lock (m_objectsListingLock)
                    {
                        if (filesToTake >= 0 && filesToTake <= Interlocked.Read(ref m_objectsAccepted))
                        {
                            // we're done, don't need new stuff
                            return;
                        }

                        long size = Utilities.EstimateFileSize(cloudFile, m_estimatedCompressionRatio);
                        DateTime? creationTime = Utilities.InferFileCreationTimeUtc(cloudFile, m_creationTimeInNamePattern);

                        targetBlock.SendAsync(new DataSource
                        {
                            CloudFileUri = $"{cloudFile.GetUnsecureUri()}",
                            SafeCloudFileUri = cloudFile.GetFileUri(),
                            SizeInBytes = size,
                            CreationTimeUtc = creationTime
                        });

                        Interlocked.Increment(ref m_objectsAccepted);
                    }
                }
            }
            catch (Exception ex)
            {
                m_logger.LogError($"FilterFiles failed: {ex.Message}");
            }
        }

        private async Task IngestSingle(DataSource storageObject,
                                  int objectsToTake,
                                  IKustoIngestClient ingestClient,
                                  bool fromFileSystem,
                                  bool deleteSourcesOnSuccess,
                                  KustoQueuedIngestionProperties baseIngestionProperties)
        {
            try
            {
                if (objectsToTake >= 0 && objectsToTake <= Interlocked.Read(ref m_objectsPosted))
                {
                    // we're done, don't need new stuff
                    return;
                }

                KustoQueuedIngestionProperties ingestionProperties = null;

                // Take care of the CreationTime
                if (storageObject.CreationTimeUtc.HasValue)
                {
                    ingestionProperties = new KustoQueuedIngestionProperties(baseIngestionProperties);
                    ingestionProperties.AdditionalProperties.Add("creationTime", storageObject.CreationTimeUtc.Value.ToString("s"));
                }
                else
                {
                    ingestionProperties = baseIngestionProperties;
                }

                while (!await m_fixedWindowThrottlerPolicy.ShouldInvokeAsync().ConfigureAwait(false))
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(50)).ConfigureAwait(false);
                }

                var result = await ingestClient.IngestFromStorageAsync(
                            fromFileSystem ? storageObject.FileSystemPath : storageObject.CloudFileUri,
                            ingestionProperties,
                            new StorageSourceOptions() { DeleteSourceOnSuccess = deleteSourcesOnSuccess, Size = storageObject.SizeInBytes }
                        ).ConfigureAwait(false);

                Interlocked.Increment(ref m_objectsPosted);

                if (m_bWaitForIngestCompletion)
                {
                    lock (m_ingestionResultsLock)
                    {
                        m_ingestionResults.Add(result);
                    }
                }
            }
            catch (Exception ex)
            {
                m_logger.LogError($"IngestSingle failed: {ex.Message}");
            }
        }

        /// <summary>
        /// Logs the <see cref="DataSource"/> details to the console
        /// </summary>
        private void LogSingleObject(DataSource storageObject)
        {
            // This path only prints out stats for objects that would have been ingested, without actually ingesting
            var sizeString = $"Size (bytes): '{storageObject.SizeInBytes}', ";
            var creationTimeString = (storageObject.CreationTimeUtc.HasValue ? $"CreationTime: '{storageObject.CreationTimeUtc.Value.ToString("s")}', " : string.Empty);
            var pathString = $"Path: '{(string.IsNullOrWhiteSpace(storageObject.FileSystemPath) ? storageObject.SafeCloudFileUri : storageObject.FileSystemPath)}'";

            m_logger.LogInfo($"==> {sizeString}{creationTimeString}{pathString}");
        }


        private IPersistentStorageContainer AcquireTempBlobContainer(ICslAdminProvider kustoClient)
        {
            IPersistentStorageContainer blobContainerRef = null;
            try
            {
                var cmd = CslCommandGenerator.GenerateCreateTempStorageCommand();
                var temp = kustoClient.ExecuteControlCommand(cmd);
                var reader = ExtendedDataReader.ToEnumerable<TempStorageCreateCommandResult>(temp).ToList();
                if (reader.SafeFastAny())
                {
                    var uriWithSas = reader.First().StorageRoot;
                    if (!string.IsNullOrWhiteSpace(uriWithSas))
                    {
                        blobContainerRef = m_blob.CreateContainerRef(uriWithSas);
                    }
                }
            }
            catch (Exception ex)
            {
                m_logger.LogError($"AcquireTempBlobContainer failed: {ex.Message}");
            }
            return blobContainerRef;
        }

        private void UploadFiles(DataSource fileRef, IPersistentStorageContainer blobContainer)
        {
            try
            {
                if (blobContainer != null)
                {
                    var blobName = Path.GetFileName(ExtendedPath.RandomizeFileName(fileRef.FileSystemPath));
                    var blobReference = blobContainer.CreateFileRef(blobName);
                    blobReference.UploadFromFileAsync(fileRef.FileSystemPath).WaitEx();
                    fileRef.CloudFileUri = blobReference.GetUnsecureUri();
                    fileRef.SafeCloudFileUri = blobReference.GetFileUri();
                }

                lock (m_listIntermediateSourcesLock)
                {
                    m_listIntermediateSources.Add(fileRef);
                }
                Interlocked.Increment(ref m_filesUploaded);
            }
            catch (Exception ex)
            {
                m_logger.LogError($"UploadFiles failed: {ex.Message}");
            }
        }

        private void AccumulateObjects(DataSource objectRef)
        {
            try
            {
                lock (m_listIntermediateSourcesLock)
                {
                    m_listIntermediateSources.Add(objectRef);
                }
                Interlocked.Increment(ref m_filesUploaded);
            }
            catch (Exception ex)
            {
                m_logger.LogError($"AccumulateObjects failed: {ex.Message}");
            }
        }

        private void IngestBatch(DataSourcesBatch batch,
                                 ICslAdminProvider kustoClient,
                                 bool bIngestLocally,
                                 KustoIngestionProperties baseIngestionProperties,
                                 TimeSpan ingestOperationTimeout)
        {
            try
            {
                List<string> batchUris = null;
                if (bIngestLocally)
                {
                    batchUris = batch.Sources.Select((s) => s.FileSystemPath).ToList();
                }
                else
                {
                    batchUris = batch.Sources.Select((s) => s.CloudFileUri).ToList();
                }

                KustoIngestionProperties ingestionProperties = null;

                // Take care of the CreationTime
                if (batch.CreationTimeUtc.HasValue)
                {
                    ingestionProperties = new KustoIngestionProperties(baseIngestionProperties);
                    ingestionProperties.AdditionalProperties.Add("creationTime", batch.CreationTimeUtc.Value.ToString("s"));
                }
                else
                {
                    ingestionProperties = baseIngestionProperties;
                }

                var cmd = CslCommandGenerator.GenerateTableIngestPullCommand(ingestionProperties.TableName, batchUris, true,
                                                                             extensions: ingestionProperties.AdditionalProperties,
                                                                             tags: ingestionProperties.AdditionalTags);
                var operationResults = kustoClient.ExecuteAsyncControlCommand(ingestionProperties.DatabaseName, cmd, ingestOperationTimeout, TimeSpan.FromSeconds(2));

                lock (m_operationResultsLock)
                {
                    m_operationResults.Add(operationResults);
                }
                Interlocked.Increment(ref m_batchesIngested);
            }
            catch (Exception ex)
            {
                m_logger.LogError($"IngestBatch failed: {ex.Message}");
            }
        }
        private static IEnumerable<DataSourcesBatch> SplitIntoBatches(IEnumerable<DataSource> objects,
                                                                      bool localFiles,
                                                                      long batchSizeLimitInBytes,
                                                                      int filesPerBatch)
        {
            bool limitBatchSize = (batchSizeLimitInBytes > 0);
            bool limitFilesPerBatch = (filesPerBatch > 0);

            int processedFiles = 0;
            var ingestionBatches = new List<DataSourcesBatch>();
            DataSourcesBatch currentBatch = null;
            int runningBatchNumber = 1;

            foreach (var f in objects)
            {
                if (currentBatch == null)
                {
                    currentBatch = new DataSourcesBatch(runningBatchNumber++);
                }
                else
                {
                    if (!Utilities.EquivalentTimestamps(currentBatch.CreationTimeUtc, f.CreationTimeUtc))
                    {
                        ingestionBatches.Add(currentBatch);
                        currentBatch = new DataSourcesBatch(runningBatchNumber++);
                    }
                }

                currentBatch.AddSource(f);
                currentBatch.CreationTimeUtc = f.CreationTimeUtc;
                processedFiles++;

                if ((limitBatchSize && currentBatch.TotalSizeBytes >= batchSizeLimitInBytes) ||
                    (limitFilesPerBatch && currentBatch.Sources.Count >= filesPerBatch))
                {
                    ingestionBatches.Add(currentBatch);
                    currentBatch = null;
                }
            }

            if (currentBatch != null)
            {
                ingestionBatches.Add(currentBatch);
            }

            return ingestionBatches;
        }

        private void WaitForIngestionResult(IEnumerable<IKustoIngestionResult> ingestionResults, TimeSpan ingestOperationTimeout)
        {
            if (ingestionResults.SafeFastNone())
            {
                return;
            }

            var stopwatch = ExtendedStopwatch.StartNew();
            m_logger.LogInfo($"==> Waiting for ingest operation(s) completion (will timeout after {ingestOperationTimeout.TotalMinutes} minutes)...");

            IEnumerable<IngestionStatus> ingestionStatuses = null;
            long monitoredIngestionOperations = 0, completedIngestionOperations = 0;

            do
            {
                ingestionStatuses = ingestionResults.SelectMany((ir) => ir.GetIngestionStatusCollection()).ToList();
                monitoredIngestionOperations = ingestionStatuses.SafeFastCount();
                completedIngestionOperations = ingestionStatuses.Count((status) => status.Status != Status.Pending);

                m_logger.LogVerbose($"==> [{completedIngestionOperations,7}] out of [{monitoredIngestionOperations,7}] ingest operations completed. Time elapsed: {stopwatch.Elapsed:c}");
                if (completedIngestionOperations == monitoredIngestionOperations)
                {
                    break;
                }

                Thread.Sleep(TimeSpan.FromSeconds(30));
            } while (stopwatch.Elapsed < ingestOperationTimeout);
            stopwatch.Stop();

            // Status breakdown:
            // Succeeded            [Terminal, operation completed successfully]
            // Failed               [Terminal, operation failed]
            // PartiallySucceeded   [Terminal, operation succeeded for part of the data]
            // Skipped              [Terminal, operation ignored (no data or was already ingested)]
            // Queued / Pending     [Intermediate, operation has been posted off for execution on the service]

            var successfulOperations = ingestionStatuses.Count((status) => status.Status == Status.Succeeded);
            var partiallySucceededOperations = ingestionStatuses.Count((status) => status.Status == Status.PartiallySucceeded);
            var failedOperations = ingestionStatuses.Count((status) => status.Status == Status.Failed);
            var skippedOperations = ingestionStatuses.Count((status) => status.Status == Status.Skipped);
            var pendingOperations = ingestionStatuses.Count((status) => status.Status == Status.Pending || status.Status == Status.Queued);

            if (successfulOperations == monitoredIngestionOperations)
            {
                m_logger.LogSuccess($"    Successfully completed          : [{successfulOperations,7}] out of [{monitoredIngestionOperations,7}] ingest operations.");
                return;
            }

            // Break down non-successful operations
            m_logger.LogWarning("Not all the operations completed successfully:");

            var sb = new StringBuilder();
            sb.AppendLine();

            if (failedOperations > 0)
            {
                sb.AppendLine($"Failed operations: [{failedOperations,7}] out of [{monitoredIngestionOperations,7}] ingest operations have failed.");
                sb.AppendLine("These operations failed permanently and no data was ingested. See the complete list for details.");
                ingestionStatuses.Where(record => record.Status == Status.Failed).ForEach(record =>
                {
                    sb.AppendLine($"-   Failed to ingest '{record.IngestionSourcePath}', Id '{record.IngestionSourceId}'. Operation status is '{record.Status}'.");
                    sb.AppendLine($"    Failure details: {record.Details}");
                });
                sb.AppendLine();
            }

            if (partiallySucceededOperations > 0)
            {
                sb.AppendLine($"Partially succeeded operations: [{partiallySucceededOperations,7}] out of [{monitoredIngestionOperations,7}] ingest operations have completed partially.");
                sb.AppendLine("These operations succeeded partially, i.e., some data could have been ingested. See the complete list for details.");
                ingestionStatuses.Where(record => record.Status == Status.PartiallySucceeded).ForEach(record =>
                {
                    sb.AppendLine($"-   Partially succeeded to ingest '{record.IngestionSourcePath}', Id '{record.IngestionSourceId}'. Operation status is '{record.Status}'.");
                    sb.AppendLine($"    Operation details: {record.Details}");
                });
                sb.AppendLine();
            }

            if (skippedOperations > 0)
            {
                sb.AppendLine($"Skipped operations: [{skippedOperations,7}] out of [{monitoredIngestionOperations,7}] ingest operations have been skipped.");
                sb.AppendLine("These operations were skipped and resulted in no data being ingested. These could indicate empty streams. See the complete list for details.");
                ingestionStatuses.Where(record => record.Status == Status.Skipped).ForEach(record =>
                {
                    sb.AppendLine($"-   Did not ingest '{record.IngestionSourcePath}', Id '{record.IngestionSourceId}'. Operation status is '{record.Status}'.");
                    sb.AppendLine($"    Operation details: {record.Details}");
                });
                sb.AppendLine();
            }

            if (pendingOperations > 0)
            {
                sb.AppendLine($"Pending operations: [{pendingOperations,7}] out of [{monitoredIngestionOperations,7}] ingest operations are still pending.");
                sb.AppendLine("These operations have not completed yet, and are waiting to be executed by the service. See the complete list for details.");
                ingestionStatuses.Where(record => record.Status == Status.Pending || record.Status == Status.Queued).ForEach(record =>
                {
                    sb.AppendLine($"-   Operation pending for '{record.IngestionSourcePath}', Id '{record.IngestionSourceId}'. Operation status is '{record.Status}'.");
                });
                sb.AppendLine();
            }

            sb.AppendLine($"Successfully completed          : [{successfulOperations,7}] out of [{monitoredIngestionOperations,7}] ingest operations.");
            sb.Append(partiallySucceededOperations > 0 ? $"Partially succeeded (see above) : [{partiallySucceededOperations,7}] out of [{monitoredIngestionOperations,7}] ingest operations.{Environment.NewLine}" : string.Empty);
            sb.Append(failedOperations > 0 ? $"Failed (see above)              : [{failedOperations,7}] out of [{monitoredIngestionOperations,7}] ingest operations.{Environment.NewLine}" : string.Empty);
            sb.Append(skippedOperations > 0 ? $"Skipped (see above)             : [{skippedOperations,7}] out of [{monitoredIngestionOperations,7}] ingest operations.{Environment.NewLine}" : string.Empty);
            sb.Append(pendingOperations > 0 ? $"Pending (still in progress)     : [{pendingOperations,7}] out of [{monitoredIngestionOperations,7}] ingest operations.{Environment.NewLine}" : string.Empty);
            sb.AppendLine();

            m_logger.LogInfo(sb.ToString());
        }
        #endregion // Private helper methods

        #region DataSource and DataSourcesBatch

        internal class DataSource
        {
            public string FileSystemPath { get; set; } = null;
            public string CloudFileUri { get; set; } = null;
            public string SafeCloudFileUri { get; set; } = null;
            public long SizeInBytes { get; set; } = 0L;
            public DateTime? CreationTimeUtc { get; set; } = null;
        }


        internal class DataSourcesBatch
        {
            public int Id { get; private set; }
            public List<DataSource> Sources { get; private set; }
            public long TotalSizeBytes { get; private set; }
            public DateTime? CreationTimeUtc { get; set; } = null;

            public DataSourcesBatch(int id = 0)
            {
                Id = id;
                Sources = new List<DataSource>();
            }

            public void AddSource(DataSource ds)
            {
                Sources.Add(ds);
                TotalSizeBytes += ds.SizeInBytes;
            }

            public string Details
            {
                get
                {
                    return $"[{Sources.Count} objects, {TotalSizeBytes:#,##0}]";
                }
            }
        }
        #endregion
    }
}
