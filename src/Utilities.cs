// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Threading;

using Kusto.Cloud.Platform.AWS.PersistentStorage;
using Kusto.Cloud.Platform.Azure.Storage;
using Kusto.Cloud.Platform.Storage.PersistentStorage;
using Kusto.Cloud.Platform.Utils;
using Kusto.Data;
using Newtonsoft.Json;

namespace LightIngest
{
    internal static class Utilities
    {
        private const long ReliableGzipSizeEstimationCutoff = 400 * MemoryConstants._1MB;

        internal static void CollapseTagsIntoAdditionalProperties(IEnumerable<string> tags, ref IDictionary<string, string> additionalProperties)
        {
            if (tags.SafeFastAny())
            {
                if (additionalProperties == null)
                {
                    additionalProperties = new Dictionary<string, string>();
                }
                additionalProperties.Add("tags" /* ExtentTagging.c_Tags */, JsonConvert.SerializeObject(tags));
            }
        }

        internal static long? GetPositiveLongProperty(IReadOnlyDictionary<string, string> bag, string propertyName)
        {
            if (TryGetValueAsString(bag, propertyName, caseSensitive: false, out string valueAsString))
            {
                long valueAsLong;
                bool parsed = Int64.TryParse(valueAsString, out valueAsLong);
                if (parsed && valueAsLong > 0)
                {
                    return valueAsLong;
                }
            }

            return null;
        }

        internal static DateTime? GetDateTimeProperty(IReadOnlyDictionary<string, string> bag, string propertyName)
        {
            if (TryGetValueAsString(bag, propertyName, caseSensitive: false, out string valueAsString))
            {
                DateTime valueAsDateTime;
                bool parsed = DateTime.TryParse(valueAsString, out valueAsDateTime);
                if (parsed && valueAsDateTime != DateTime.MinValue)
                {
                    return valueAsDateTime;
                }
            }

            return null;
        }

        internal static long TryGetFileSize(string path, double estimatedCompressionRatio)
        {
            try
            {
                var fileInfo = new FileInfo(path);
                long fileSize = 0L;

                if (fileInfo.Extension.EndsWith("zip", StringComparison.OrdinalIgnoreCase))
                {
                    // For Zip archive we can actually calculate the file size
                    using (ZipArchive archive = ZipFile.OpenRead(path))
                    {
                        if (archive.Entries.SafeFastAny())
                        {
                            archive.Entries.ForEach(entry => { fileSize += entry.Length; });
                        }
                    }
                }
                else if (fileInfo.Extension.EndsWith("gz", StringComparison.OrdinalIgnoreCase))
                {
                    // For GZ archives we employ the following logic: if the compressed file is under 400MB,
                    // we will read the uncompressed size from the last 4 bytes of the file.
                    // FOr larger files, we will not rely on the last 4 bytes, as they are modulo 4GB and revert to estimation.
                    if (fileInfo.Length > ReliableGzipSizeEstimationCutoff)
                    {
                        fileSize = (long)(fileInfo.Length * estimatedCompressionRatio);
                    }
                    else
                    {
                        using (var gzStream = File.OpenRead(path))
                        {
                            gzStream.Position = gzStream.Length - 4;
                            var byteArray = new byte[4];
                            gzStream.Read(byteArray, 0, 4);
                            fileSize = BitConverter.ToUInt32(byteArray, 0);
                        }
                    }
                }
                else
                {
                    fileSize = fileInfo.Length;
                }

                return fileSize;
            }
            catch (Exception ex)
            {
                ExtendedConsole.WriteLine(
                    ConsoleColor.DarkYellow, $"Failed to retrieve size for file '{path}'. Error was: {ex.Message}");
            }
            return 0L;
        }

        internal static long EstimateFileSize(IPersistentStorageFile cloudFile, double estimatedCompressionRatio)
        {
            if (cloudFile is IFileWithMetadata cloudFileWithMetadata)
            {
                IReadOnlyDictionary<string, string> metadata = cloudFileWithMetadata.GetFileMetaDataAsync().ResultEx();
                long? estimatedSizeBytes = GetPositiveLongProperty(metadata, (cloudFile is S3PersistentStorageFile) ? Constants.AwsMetadaRawDataSize : Constants.BlobMetadaRawDataSizeLegacy);
                if (estimatedSizeBytes.HasValue)
                {
                    return estimatedSizeBytes.Value;
                }

                estimatedSizeBytes = GetPositiveLongProperty(metadata, (cloudFile is S3PersistentStorageFile) ? Constants.AwsMetadaRawDataSize : Constants.BlobMetadaRawDataSize);
                if (estimatedSizeBytes.HasValue)
                {
                    return estimatedSizeBytes.Value;
                }
            }

            long blobSize = cloudFile.GetLength();
            string blobName = cloudFile.GetFileName();

            // TODO: we need to add proper handling per format
            if (blobName.EndsWith(".zip", StringComparison.OrdinalIgnoreCase) || blobName.EndsWith(".gz", StringComparison.OrdinalIgnoreCase))
            {
                blobSize = (long)(blobSize * estimatedCompressionRatio);
            }

            return blobSize;
        }

        internal static DateTime? InferFileCreationTimeUtc(string path, DateTimeFormatPattern fileCreationTimeFormat)
        {
            return TryParseDateTimeUtcFromString(path, fileCreationTimeFormat);
        }

        internal static DateTime? InferFileCreationTimeUtc(IPersistentStorageFile cloudFile, DateTimeFormatPattern blobCreationTimeFormat)
        {
            // Metadata always wins, as it is more deliberate
            if (cloudFile is IFileWithMetadata cloudFileWithMetadata)
            {
                var metadata = cloudFileWithMetadata.GetFileMetaDataAsync().ResultEx();
                DateTime? creationTimeUtc = GetDateTimeProperty(metadata, (cloudFile is S3PersistentStorageFile) ? Constants.AwsMetadataCreationTimeLegacy : Constants.BlobMetadataCreationTimeLegacy);
                if (creationTimeUtc.HasValue)
                {
                    return creationTimeUtc.Value;
                }

                creationTimeUtc = GetDateTimeProperty(metadata, (cloudFile is S3PersistentStorageFile) ? Constants.AwsMetadataCreationTimeUtc : Constants.BlobMetadataCreationTimeUtc);
                if (creationTimeUtc.HasValue)
                {
                    return creationTimeUtc.Value;
                }
            }


            // We use the entire blob URI absolute path (container and blob path) to infer creationTime
            return TryParseDateTimeUtcFromString(cloudFile.GetFileUri(), blobCreationTimeFormat);
        }

        internal static void TryDeleteFile(string path)
        {
            if (File.Exists(path))
            {
                ExceptionFilters.RunTraceSwallow(() => { File.Delete(path); }, $"TryDeleteFile: failed to delete file '{path}'", SharedTracer.Tracer);
            }
        }

        internal static void TryDeleteBlob(IPersistentStorageFile blobRef)
        {
            if (blobRef != null)
            {
                blobRef.DeleteIfExistsAsync().WaitEx();
            }
        }

        internal static bool PathExists(string input)
        {
            if (input == null)
            {
                return false;
            }

            // The common cases for which there should be no internal exception
            if (input.StartsWith("http://", StringComparison.OrdinalIgnoreCase) || input.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            try
            {
                if (File.Exists(input) || Directory.Exists(input))
                {
                    return true;
                }
            }
            catch { }
            return false;
        }

        internal static bool IsFileSystemPath(string input)
        {
            if (string.IsNullOrWhiteSpace(input))
            {
                return false;
            }

            try
            {
                if (File.Exists(input) || Directory.Exists(input))
                {
                    return true;
                }
            }
            catch { }
            return false;
        }

        internal static bool IsBlobStorageUri(string input, out string error)
        {
            try
            {
                var container =
                    CloudResourceUriParser.TryCreateCloudBlobContainer(input, out error, keyOrSasMandatory: false);
                if (container == null)
                {
                    return S3PersistentStorageUri.IsAmazonS3Uri(input);
                } 
                
                return true;
            }
            catch (Exception ex)
            {
                error = ex.Message;
            }

            return false;
        }

        internal static bool EquivalentTimestamps(DateTime? lhs, DateTime? rhs)
        {
            if (lhs.HasValue != rhs.HasValue)
            {
                return false;
            }
            if (!lhs.HasValue && !rhs.HasValue)
            {
                return true;
            }

            return (lhs.Value == rhs.Value);
        }

        internal static ConsoleKeyInfo? ReadKeyWithTimeout(TimeSpan timeout)
        {
            ConsoleKeyInfo? keyInfo = null;

            // We're not supposed to get here, but just in case
            if (Console.IsInputRedirected)
            {
                return keyInfo;
            }

            var stopwatch = ExtendedStopwatch.StartNew();

            do
            {
                if (Console.KeyAvailable)
                {
                    keyInfo = Console.ReadKey(intercept: true);
                    break;
                }

                Thread.Sleep(500);
            } while (stopwatch.Elapsed < timeout);

            stopwatch.Stop();
            return keyInfo;
        }

        internal static bool IsLocalKustoConnection(KustoConnectionStringBuilder kcsb)
        {
            try
            {
                var source = kcsb.DataSource;
                if (String.IsNullOrWhiteSpace(source))
                {
                    return false;
                }

                var uri = new Uri(source);
                var targetHost = uri.Host;

                var targetAddresses = System.Net.Dns.GetHostAddresses(targetHost);
                var localAddresses = System.Net.Dns.GetHostAddresses(System.Net.Dns.GetHostName());

                foreach (var targetAddress in targetAddresses)
                {
                    if (System.Net.IPAddress.IsLoopback(targetAddress))
                    {
                        return true;
                    }

                    foreach (var localAddress in localAddresses)
                    {
                        if (targetAddress.Equals(localAddress))
                        {
                            return true;
                        }
                    }
                }

                return false;
            }
            catch { }
            return false;
        }

        #region Private methods
        private static DateTime? TryParseDateTimeUtcFromString(string sourceString, DateTimeFormatPattern dateTimeFormat)
        {
            if (dateTimeFormat == null || string.IsNullOrWhiteSpace(sourceString))
            {
                return null;
            }

            if (sourceString.Length < dateTimeFormat.Prefix.Length + dateTimeFormat.Format.Length + dateTimeFormat.Suffix.Length)
            {
                return null;
            }

            int startIndex = 0;
            while ((startIndex = sourceString.IndexOf(dateTimeFormat.Prefix, startIndex)) >= 0)
            {
                // Advance post prefix
                startIndex += dateTimeFormat.Prefix.Length;

                // Look for suffix, skipping the DateTime format part
                int suffixStartIndex = sourceString.IndexOf(dateTimeFormat.Suffix, startIndex + dateTimeFormat.Format.Length);

                if (suffixStartIndex < 0)
                {
                    // If the suffix cannot be found - we are done searchng for it
                    break;
                }

                if (suffixStartIndex - startIndex == dateTimeFormat.Format.Length)
                {
                    var dateTimePortion = sourceString.Substring(startIndex, dateTimeFormat.Format.Length);

                    DateTime ret;
                    if (ExtendedDateTime.TryParseExactUtc(dateTimePortion, dateTimeFormat.Format, out ret))
                    {
                        return ret;
                    }
                }
            }

            return null;
        }

        // Allow handling case-insensitive collections
        private static bool TryGetValueAsString(IReadOnlyDictionary<string, string> bag, string propertyName, bool caseSensitive, out string valueAsString)
        {
            valueAsString = null;

            if (caseSensitive)
            {
                return bag.TryGetValue(propertyName, out valueAsString);
            }

            foreach (var kvp in bag)
            {
                if (string.Equals(kvp.Key, propertyName, StringComparison.OrdinalIgnoreCase))
                {
                    valueAsString = kvp.Value;
                    return true;
                }
            }

            return false;
        }

        #endregion
    }
}
