using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CsvBlobSplitterConsole
{
    internal class RunSettings
    {
        public Format Format { get; }

        public Uri SourceBlob { get; }

        public Uri? DestinationBlobPrefix { get; }

        public BlobCompression InputCompression { get; }
        
        public BlobCompression OutputCompression { get; }

        public bool HasHeaders { get; }

        public int MaxMbPerShard { get; }

        #region Constructors
        public static RunSettings FromEnvironmentVariables()
        {
            var format = GetEnum<Format>("Format", false);
            var sourceBlob = GetUri("SourceBlob");
            var destinationBlobPrefix = GetUri("DestinationBlobPrefix", false);
            var inputCompression = GetEnum<BlobCompression>("InputCompression", false);
            var outputCompression = GetEnum<BlobCompression>("OutputCompression", false);
            var hasHeaders = GetBool("CsvHeaders", false);
            var maxMbPerShard = GetInt("MaxMbPerShard", false);

            return new RunSettings(
                format,
                sourceBlob,
                destinationBlobPrefix,
                inputCompression,
                outputCompression,
                hasHeaders,
                maxMbPerShard);
        }

        public RunSettings(
            Format? format,
            Uri sourceBlob,
            Uri? destinationBlobPrefix,
            BlobCompression? inputCompression,
            BlobCompression? outputCompression,
            bool? hasHeaders,
            int? maxMbPerShard)
        {
            if (destinationBlobPrefix == null)
            {
                throw new NotSupportedException("No destination specified");
            }

            Format = format?? Format.Text;
            SourceBlob = sourceBlob;
            DestinationBlobPrefix = destinationBlobPrefix;
            InputCompression = inputCompression ?? BlobCompression.None;
            OutputCompression = outputCompression ?? BlobCompression.None;
            HasHeaders = hasHeaders ?? true;
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