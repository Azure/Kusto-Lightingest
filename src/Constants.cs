// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

#if !OPEN_SOURCE_COMPILATION
using Kusto.Cloud.Platform.AWS.PersistentStorage;
#endif

namespace LightIngest
{
    internal static class Constants
    {
        // Kusto blob metadata keys
        internal const string BlobMetadaRawDataSize = "kustoUncompressedSizeBytes";
        internal const string BlobMetadaRawDataSizeLegacy = "rawSizeBytes";
        internal const string BlobMetadataCreationTimeUtc = "kustoCreationTimeUtc";
        internal const string BlobMetadataCreationTimeLegacy = "kustoCreationTime";

#if !OPEN_SOURCE_COMPILATION
        // Kusto aws metadata keys
        internal static readonly string AwsMetadaRawDataSize = (S3PersistentStorageFile.AwsUserDefinedMetadataPrefix + BlobMetadaRawDataSize).ToLower();
        internal static readonly string AwsMetadaRawDataSizeLegacy = (S3PersistentStorageFile.AwsUserDefinedMetadataPrefix + BlobMetadaRawDataSizeLegacy).ToLower();
        internal static readonly string AwsMetadataCreationTimeUtc = (S3PersistentStorageFile.AwsUserDefinedMetadataPrefix + BlobMetadataCreationTimeUtc).ToLower();
        internal static readonly string AwsMetadataCreationTimeLegacy = (S3PersistentStorageFile.AwsUserDefinedMetadataPrefix + BlobMetadataCreationTimeLegacy).ToLower();
#endif
        // Kusto ingestion properties
        internal const string IgnoreFirstRecordPropertyName = "ignoreFirstRecord";
        internal const string ZipPatternPropertyName = "zipPattern";

        // Numeric constants
        internal const double DefaultCompressionRatio = 10.0;
    }
}
