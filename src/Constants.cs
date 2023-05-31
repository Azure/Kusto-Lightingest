// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace LightIngest
{
    internal static class Constants
    {
        // Kusto blob metadata keys
        internal const string BlobMetadaRawDataSize = "kustoUncompressedSizeBytes";
        internal const string BlobMetadaRawDataSizeLegacy = "rawSizeBytes";
        internal const string BlobMetadataCreationTimeUtc = "kustoCreationTimeUtc";
        internal const string BlobMetadataCreationTimeLegacy = "kustoCreationTime";

        // Kusto ingestion properties
        internal const string IgnoreFirstRecordPropertyName = "ignoreFirstRecord";
        internal const string ZipPatternPropertyName = "zipPattern";

        // Numeric constants
        internal const double DefaultCompressionRatio = 10.0;
    }
}
