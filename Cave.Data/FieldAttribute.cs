using System;

namespace Cave.Data
{
    /// <summary>
    ///     Provides a field name <see cref="Attribute" /> for renaming fields at database rows (Using different name at
    ///     struct and database).
    /// </summary>
    [AttributeUsage(AttributeTargets.Field)]
    public sealed class FieldAttribute : Attribute
    {
        /// <summary>Gets or sets the "real" field name (at the database).</summary>
        public string Name { get; set; }

        /// <summary>Gets or sets the maximum field length (at the database).</summary>
        public uint Length { get; set; }

        /// <summary>Gets or sets the type of the data.</summary>
        /// <value>The type of the data.</value>
        public DataType DataType { get; set; }

        /// <summary>Gets or sets the flags.</summary>
        public FieldFlags Flags { get; set; }

        /// <summary>Gets or sets the display format.</summary>
        /// <value>The display format.</value>
        public string DisplayFormat { get; set; }

        /// <summary>Gets or sets additional field names (at the database) that will be matched.</summary>
        public string AlternativeNames { get; set; }
    }
}
