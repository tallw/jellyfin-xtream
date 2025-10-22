using System;
using System.Globalization;
using Newtonsoft.Json;

namespace Jellyfin.Xtream.Client.Models
{
    /// <summary>
    /// JSON converter para converter strings vazias em null para valores long.
    /// </summary>
    public class ParseEmptyStringToNullLongConverter : JsonConverter<long?>
    {
        /// <summary>
        /// Gets a value indicating whether this converter can write JSON.
        /// </summary>
        public override bool CanWrite => true;

        /// <summary>
        /// Escreve o JSON.
        /// </summary>
        /// <param name="writer">O escritor JSON.</param>
        /// <param name="value">O valor.</param>
        /// <param name="serializer">O serializador.</param>
        public override void WriteJson(JsonWriter writer, long? value, JsonSerializer serializer)
        {
            if (value.HasValue)
            {
                writer.WriteValue(value.Value);
            }
            else
            {
                writer.WriteNull();
            }
        }

        /// <summary>
        /// Lê o JSON.
        /// </summary>
        /// <param name="reader">O leitor JSON.</param>
        /// <param name="objectType">Tipo do objeto.</param>
        /// <param name="existingValue">O valor existente.</param>
        /// <param name="hasExistingValue">Indica se existe valor.</param>
        /// <param name="serializer">O serializador.</param>
        /// <returns>O valor convertido.</returns>
        public override long? ReadJson(JsonReader reader, Type objectType, long? existingValue, bool hasExistingValue, JsonSerializer serializer)
        {
            // Se for string vazia -> retorna null
            if (reader.TokenType == JsonToken.String && string.IsNullOrEmpty((string?)reader.Value))
            {
                return null;
            }

            // Se for nulo -> retorna null
            if (reader.TokenType == JsonToken.Null)
            {
                return null;
            }

            // Se for número inteiro -> converte normalmente
            if (reader.TokenType == JsonToken.Integer)
            {
                return Convert.ToInt64(reader.Value, CultureInfo.InvariantCulture);
            }

            // Qualquer outro caso inesperado -> retorna null
            return null;
        }
    }
}