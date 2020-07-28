using System;
using System.Globalization;
using System.Reflection;
using Cave.Data;
using Cave.IO;

namespace Cave
{
    /// <summary>Provides an interface for field properties.</summary>
    public interface IFieldProperties : IEquatable<IFieldProperties>
    {
        /// <summary>Gets the field index.</summary>
        int Index { get; }

        /// <summary>Gets a value indicating whether the field may contain null.</summary>
        bool IsNullable { get; }

        /// <summary>Gets the dotnet type of the value.</summary>
        Type ValueType { get; }

        /// <summary>Gets the <see cref="DataType" /> of the field.</summary>
        DataType DataType { get; }

        /// <summary>Gets the <see cref="FieldFlags" /> of the field.</summary>
        FieldFlags Flags { get; }

        /// <summary>Gets the name of the field.</summary>
        string Name { get; }

        /// <summary>Gets the name of the field at the database.</summary>
        string NameAtDatabase { get; }

        /// <summary>Gets the DataType of the field at the database.</summary>
        DataType TypeAtDatabase { get; }

        /// <summary>Gets the date time kind used at the database.</summary>
        DateTimeKind DateTimeKind { get; }

        /// <summary>Gets the date time type used at the database (has to match the <see cref="TypeAtDatabase" />).</summary>
        DateTimeType DateTimeType { get; }

        /// <summary>Gets the string encoding used at the database.</summary>
        StringEncoding StringEncoding { get; }

        /// <summary>Gets the maximum length of the field.</summary>
        float MaximumLength { get; }

        /// <summary>Gets the description of the field.</summary>
        string Description { get; }

        /// <summary>Gets the format arguments for displaying values.</summary>
        string DisplayFormat { get; }

        /// <summary>Gets the alternative names for this field.</summary>
        string AlternativeNames { get; }

        /// <summary>Gets the fieldinfo used to create this instance (if any).</summary>
        FieldInfo FieldInfo { get; }

        /// <summary>Gets the default value.</summary>
        object DefaultValue { get; }

        /// <summary>Gets the dot net type name of the field.</summary>
        string DotNetTypeName { get; }

        /// <summary>Creates a copy for editing properties.</summary>
        /// <returns>Returns a new instance.</returns>
        FieldProperties Clone();

        /// <summary>Parses a string and obtains the object by using the <see cref="ValueType" />s static Parse(string) method.</summary>
        /// <param name="text">The string to parse.</param>
        /// <param name="stringMarker">The string marker.</param>
        /// <param name="provider">The format provider (optional, defaults to <see cref="CultureInfo.InvariantCulture" />).</param>
        /// <returns>The native value.</returns>
        object ParseValue(string text, string stringMarker = null, IFormatProvider provider = null);

        /// <summary>
        ///     Retrieves a string for the specified value. The string may be parsed back to a value using
        ///     <see cref="ParseValue(string, string, IFormatProvider)" />.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <param name="stringMarker">The string marker.</param>
        /// <param name="provider">The format provider (optional, defaults to <see cref="CultureInfo.InvariantCulture" />).</param>
        /// <returns>A string containing the value.</returns>
        string GetString(object value, string stringMarker = null, IFormatProvider provider = null);

        /// <summary>Gets an enum value from the specified long value.</summary>
        /// <param name="value">64 bit long value for the enum.</param>
        /// <returns>Enum value.</returns>
        object EnumValue(long value);
    }
}
