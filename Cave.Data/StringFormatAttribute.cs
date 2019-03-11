using System;
using Cave.IO;

namespace Cave.Data
{
    /// <summary>
    /// Provides an <see cref="Attribute"/> for configuring string/varchar/text fields.
    /// </summary>
    [AttributeUsage(AttributeTargets.Field, Inherited = false)]
    public sealed class StringFormatAttribute : Attribute
    {
        /// <summary>
        /// Gets/sets the <see cref="StringEncoding"/>.
        /// </summary>
        public StringEncoding Encoding { get; private set; }

        /// <summary>
        /// Creates a new <see cref="StringFormatAttribute"/>.
        /// </summary>
        public StringFormatAttribute(StringEncoding encoding)
        {
            Encoding = encoding;
        }
    }
}
