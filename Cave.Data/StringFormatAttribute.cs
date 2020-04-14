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
        /// Initializes a new instance of the <see cref="StringFormatAttribute"/> class.
        /// </summary>
        /// <param name="encoding">String encoding to use.</param>
        public StringFormatAttribute(StringEncoding encoding)
        {
            Encoding = encoding;
        }

        /// <summary>
        /// Gets the <see cref="StringEncoding"/>.
        /// </summary>
        public StringEncoding Encoding { get; private set; }
    }
}
