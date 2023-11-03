using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CsvBlobSplitterConsole
{
    internal class RunSettings
    {
        public Uri SourceBlob { get; }

        public Uri? DestinationBlobPrefix { get; }

        public BlobCompression Compression { get; }

        public bool HasCsvHeaders { get; }

        public int MaxRowsPerShard { get; }

        public int MaxMbPerShard { get; }

        #region Constructors
        public static RunSettings FromEnvironmentVariables()
        {
            var sourceBlob = GetUri("SourceBlob");
            var destinationBlobPrefix = GetUri("DestinationBlobPrefix", false);
            var compression = GetEnum<BlobCompression>("Compression", false);
            var hasCsvHeaders = GetBool("HasCsvHeaders", false);
            var maxRowsPerShard = GetInt("MaxRowsPerShard", false);
            var maxMbPerShard = GetInt("MaxMbPerShard", false);

            return new RunSettings(
                sourceBlob,
                destinationBlobPrefix,
                compression,
                hasCsvHeaders,
                maxRowsPerShard,
                maxMbPerShard);
        }

        public RunSettings(
            Uri sourceBlob,
            Uri? destinationBlobPrefix,
            BlobCompression? compression,
            bool? hasCsvHeaders,
            int? maxRowsPerShard,
            int? maxMbPerShard)
        {
            if (destinationBlobPrefix == null)
            {
                throw new NotSupportedException("No destination specified");
            }

            SourceBlob = sourceBlob;
            DestinationBlobPrefix = destinationBlobPrefix;
            Compression = compression ?? BlobCompression.None;
            HasCsvHeaders = hasCsvHeaders ?? true;
            MaxRowsPerShard = maxRowsPerShard ?? 1000000;
            MaxMbPerShard = maxMbPerShard ?? 200;
        }
        #endregion

        #region Environment variables
        #region String
        private static string GetString(string variableName)
        {
            var value = GetString(variableName, true);

            return value!;
        }

        private static string? GetString(string variableName, bool mustExist)
        {
            var value = Environment.GetEnvironmentVariable(variableName);

            if (mustExist && value == null)
            {
                throw new ArgumentNullException(variableName, "Environment variable missing");
            }

            return value;
        }
        #endregion

        #region Uri
        private static Uri GetUri(string variableName)
        {
            var uri = GetUri(variableName, true);

            return uri!;
        }

        private static Uri? GetUri(string variableName, bool mustExist)
        {
            var value = Environment.GetEnvironmentVariable(variableName);

            if (mustExist && value == null)
            {
                throw new ArgumentNullException(variableName, "Environment variable missing");
            }

            if (value != null)
            {
                try
                {
                    var uri = new Uri(value, UriKind.Absolute);

                    return uri;
                }
                catch
                {
                    throw new ArgumentNullException(variableName, $"Unsupported value:  '{value}'");
                }
            }
            else
            {
                return null;
            }
        }
        #endregion

        #region Enum
        private static T? GetEnum<T>(string variableName, bool mustExist)
            where T : struct
        {
            var value = Environment.GetEnvironmentVariable(variableName);

            if (mustExist && value == null)
            {
                throw new ArgumentNullException(variableName, "Environment variable missing");
            }

            if (value != null)
            {
                if (Enum.TryParse<T>(value, out var enumValue))
                {
                    return enumValue;
                }
                else
                {
                    throw new ArgumentNullException(variableName, $"Unsupported value:  '{value}'");
                }
            }
            else
            {
                return null;
            }
        }
        #endregion

        #region Bool
        private static bool? GetBool(string variableName, bool mustExist)
        {
            var value = Environment.GetEnvironmentVariable(variableName);

            if (mustExist && value == null)
            {
                throw new ArgumentNullException(variableName, "Environment variable missing");
            }

            if (value != null)
            {
                if (bool.TryParse(value, out var boolValue))
                {
                    return boolValue;
                }
                else
                {
                    throw new ArgumentNullException(variableName, $"Unsupported value:  '{value}'");
                }
            }
            else
            {
                return null;
            }
        }
        #endregion

        #region Int
        private static int? GetInt(string variableName, bool mustExist)
        {
            var value = Environment.GetEnvironmentVariable(variableName);

            if (mustExist && value == null)
            {
                throw new ArgumentNullException(variableName, "Environment variable missing");
            }

            if (value != null)
            {
                if (int.TryParse(value, out var boolValue))
                {
                    return boolValue;
                }
                else
                {
                    throw new ArgumentNullException(variableName, $"Unsupported value:  '{value}'");
                }
            }
            else
            {
                return null;
            }
        }
        #endregion
        #endregion
    }
}