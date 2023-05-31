// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Kusto.Cloud.Platform.Utils;

namespace LightIngest
{
    internal class AdditionalArguments
    {
        internal DateTimeFormatPattern DateTimePattern { get; set; } = null;
    }

    internal class DateTimeFormatPattern
    {
        public DateTimeFormatPattern(string prefix, string format, string suffix)
        {
            Ensure.ArgIsNotNullOrWhiteSpace(prefix, nameof(prefix));
            Ensure.ArgIsNotNullOrWhiteSpace(format, nameof(format));
            Ensure.ArgIsNotNullOrWhiteSpace(suffix, nameof(suffix));

            Prefix = prefix;
            Format = format;
            Suffix = suffix;
        }

        public string Prefix { get; private set; } = null;
        public string Format { get; private set; } = null;
        public string Suffix { get; private set; } = null;
    }
}
