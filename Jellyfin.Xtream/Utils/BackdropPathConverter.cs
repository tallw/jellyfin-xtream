using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Jellyfin.Xtream.Utils.Converters
{
    /// <summary>
    /// Converts between single string and string array for backdrop_path property.
    /// </summary>
    public sealed class BackdropPathConverter : JsonConverter<ICollection<string>?>
    {
        /// <inheritdoc />
        public override bool CanWrite => true;

        /// <inheritdoc />
        public override ICollection<string>? ReadJson(
            JsonReader reader,
            Type objectType,
            ICollection<string>? existingValue,
            bool hasExistingValue,
            JsonSerializer serializer)
        {
            if (reader.TokenType == JsonToken.Null)
            {
                return null;
            }

            if (reader.TokenType == JsonToken.String)
            {
                string? singleValue = reader.Value?.ToString();
                return singleValue == null ? null : new List<string> { singleValue };
            }

            if (reader.TokenType == JsonToken.StartArray)
            {
                var array = JArray.Load(reader);
                return array.ToObject<List<string>>();
            }

            throw new JsonSerializationException($"Unexpected token type {reader.TokenType} when parsing backdrop_path");
        }

        /// <inheritdoc />
        public override void WriteJson(
            JsonWriter writer,
            ICollection<string>? value,
            JsonSerializer serializer)
        {
            if (value == null)
            {
                writer.WriteNull();
                return;
            }

            serializer.Serialize(writer, value);
        }
    }
}
