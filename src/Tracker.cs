using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Kusto.Cloud.Platform.Utils;
using Kusto.Ingest;


namespace LightIngest
{
    public class Tracker
    {
        private readonly TimeSpan m_trackingDelay = TimeSpan.FromSeconds(30);
        private readonly TimeSpan m_delayOnThrottling = TimeSpan.FromSeconds(1);
        private readonly TimeSpan m_singleStatusTimeout;
        private readonly LoggerTracer m_logger;

        private CancellationTokenSource m_abortTracking;
        private IXsvWriter m_csvWriter;
        private FileStream m_fileStream;
        private string m_reportFilePath;
        private IThrottlerPolicy m_throttler;

        private long m_successfulOperations;
        private long m_partiallySucceededOperations;
        private long m_failedOperations;
        private long m_skippedOperations;
        private long m_pendingOperations;
        private long m_totalTracked;

        public Tracker(TimeSpan maxIngestionWaitTime, LoggerTracer tracer, IThrottlerPolicy throttler) 
        {
            m_singleStatusTimeout = maxIngestionWaitTime;
            m_logger = tracer;
            m_throttler = throttler;
        }

        // Prepare to collect data
        public void Initialize()
        {
            m_abortTracking = new CancellationTokenSource();
            m_reportFilePath = ExtendedPath.GetTempFileName($"lightingest_results.csv");
            m_fileStream = File.Open(m_reportFilePath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read);
            m_csvWriter = CsvWriter.Create(m_fileStream, System.Text.Encoding.UTF8);

            m_logger.LogAlways($"==> Ingestion Tracking Enabled [Status Wait Time '{m_singleStatusTimeout}', Report Path '{m_reportFilePath}']");

            // print headline 
            WriteLineToReportFile(
                localFile: "Local File", 
                remotePath: "Remote Path", 
                sourceId: "Source Id", 
                ingestionStatus: "Ingestion Status", 
                ingestionDetails: "Ingestion Details",
                errors: "Errors"
                );
        }

        // Close tracking resources 
        public void Close()
        {
            m_csvWriter.Flush();
            m_fileStream.Close();
            m_fileStream.Dispose();
        }

        /// <summary>
        /// When set, overall tracking will abort if not completed by the end of another ingestion tracking duration as configured in the Tracker CTOR
        /// </summary>
        public void StartFinalWaitPeriod()
        {
            m_logger.LogAlways($"==> Waiting for ingest operation(s) completion (will timeout at {ExtendedDateTime.Now + m_singleStatusTimeout})...");

            m_abortTracking.CancelAfter(m_singleStatusTimeout);
        }

        public string GetBriefProgressReport()
        {
            var tracked = Interlocked.Read(ref m_totalTracked);
            var succeeded = Interlocked.Read(ref m_successfulOperations);
            var partial = Interlocked.Read(ref m_partiallySucceededOperations); 
            var failed = Interlocked.Read(ref m_failedOperations);
            var skipped = Interlocked.Read(ref m_skippedOperations);
            var pending = Interlocked.Read(ref m_pendingOperations);

            return $"Tracked: [{tracked,7}], Succeeded: [{succeeded,7}], Partially Succeeded: [{partial,7}], Failed: [{failed, 7}], Skipped: [{skipped, 7}], Status Unknown: [{pending,7}]";
        }

        public void ReportDetailedStatus(TimeSpan duration, long discovered, long filtered, long posted)
        {
            m_logger.LogAlways("");
            m_logger.LogAlways("Ingestion Report");
            m_logger.LogAlways("=================================");
            m_logger.LogAlways($"Duration                        : [{duration:c}]");
            m_logger.LogAlways($"Items Discovered                : [{discovered,7}]");
            m_logger.LogAlways($"Items filtered                  : [{filtered,7}]");
            m_logger.LogAlways($"Items posted                    : [{posted,7}]");
            m_logger.LogAlways($"Successfully completed          : [{m_successfulOperations,7}] out of [{m_totalTracked,7}] ingest operations.");
            m_logger.LogAlways($"Partially succeeded (see above) : [{m_partiallySucceededOperations,7}] out of [{m_totalTracked,7}] ingest operations.");
            m_logger.LogAlways($"Failed (see above)              : [{m_failedOperations,7}] out of [{m_totalTracked,7}] ingest operations.");
            m_logger.LogAlways($"Skipped (see above)             : [{m_skippedOperations,7}] out of [{m_totalTracked,7}] ingest operations.");
            m_logger.LogAlways($"Unknown or still in progress    : [{m_pendingOperations,7}] out of [{m_totalTracked,7}] ingest operations.");
            m_logger.LogAlways($"Detailed report can be found at : [{m_reportFilePath}");
        }

        /// <summary>
        ///  Wait for the status of a single operation and count its statistics 
        /// </summary>
        /// <param name="path">the file or blob being checked</param>
        /// <param name="ingestionResult"></param>
        public async Task TryWaitForStatusAsync(string path, IKustoIngestionResult ingestionResult, Action<string, IngestionStatus, Exception> followUpWith)
        {
            var maxWaitTime = ExtendedDateTime.UtcNow + m_singleStatusTimeout;
            IngestionStatus lastStatus = null;
            Exception lastException = null;

            try
            {
                // Attemt to get a permanent status 
                while (!m_abortTracking.IsCancellationRequested && ExtendedDateTime.UtcNow < maxWaitTime)
                {
                    while (!m_throttler.ShouldInvoke())
                    {
                        await Task.Delay(m_delayOnThrottling, m_abortTracking.Token).ConfigureAwait(false);
                    }

                    try
                    {
                        lastException = null;
                        lastStatus = ingestionResult.GetIngestionStatusCollection().Single();

                        if (lastStatus.Status != Status.Pending)
                        {
                            break;
                        }
                    }
                    catch (Exception ex)
                    {
                        lastException = ex;
                        m_logger.LogWarning($"==> Exception caught while tracking ingestion result for [{path}]: {ex.MessageEx()}");

                        if (ex.IsPermanent(treatDnsErrorsAsTransient: true).IsTrue)
                        {
                            break;
                        }
                    }

                    await Task.Delay(m_trackingDelay, m_abortTracking.Token).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
            }

            followUpWith(path, lastStatus, lastException);
        }

        /// <summary>
        /// Save the status
        /// </summary>
        /// <param name="path"></param>
        /// <param name="status"></param>
        public void SaveTrackingStatusUnderLock(string path, IngestionStatus status, Exception ex)
        {
            WriteLineToReportFile(
                localFile: path,
                remotePath: status?.IngestionSourcePath,
                sourceId: status?.IngestionSourceId.ToString(),
                ingestionStatus: status?.Status.ToString(),
                ingestionDetails: status?.Details,
                errors: ex?.ToStringEx()
                );

            if (status == null)
            {
                // We'd get here if a permanent exception was thrown or if we exceeded the overall max tracking time as tracked by the cancelation token
                Interlocked.Increment(ref m_pendingOperations);
                return;
            }

            m_logger.LogVerbose($"==> tracking for '{status.IngestionSourcePath}', Id '{status.IngestionSourceId}' ended with status is '{status.Status}': {status.Details}");

            // Status breakdown:
            // Succeeded            [Terminal, operation completed successfully]
            // Failed               [Terminal, operation failed]
            // PartiallySucceeded   [Terminal, operation succeeded for part of the data]
            // Skipped              [Terminal, operation ignored (no data or was already ingested)]
            // Queued / Pending     [Intermediate, operation has been posted for execution on the service but has no definte completion state]

            switch (status.Status)
            {
                case Status.Succeeded:
                    Interlocked.Increment(ref m_successfulOperations);
                    break;

                case Status.PartiallySucceeded:
                    Interlocked.Increment(ref m_partiallySucceededOperations);
                    break;

                case Status.Failed:
                    Interlocked.Increment(ref m_failedOperations);
                    break;

                case Status.Skipped:
                    Interlocked.Increment(ref m_skippedOperations);
                    break;

                case Status.Pending:
                case Status.Queued:
                default:
                    Interlocked.Increment(ref m_pendingOperations);
                    break;
            }

            var tracked = Interlocked.Increment(ref m_totalTracked);
        }

        private void WriteLineToReportFile(string localFile, string remotePath, string sourceId, string ingestionStatus, string ingestionDetails, string errors)
        {
            m_csvWriter.WriteField(localFile ?? string.Empty, false);
            m_csvWriter.WriteField(remotePath ?? string.Empty, false);
            m_csvWriter.WriteField(sourceId ?? string.Empty, false);
            m_csvWriter.WriteField(ingestionStatus ?? string.Empty, false);
            m_csvWriter.WriteField(ingestionDetails ?? string.Empty, false);
            m_csvWriter.WriteField(errors ?? string.Empty, false);

            m_csvWriter.CompleteRecord();
        }
    }
}
