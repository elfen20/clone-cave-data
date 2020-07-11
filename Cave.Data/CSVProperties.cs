using System;
using System.Globalization;
using Cave.IO;

namespace Cave.Data
{
    /// <summary>
    /// Provides properties for CSV files.
    /// </summary>
    public struct CsvProperties : IEquatable<CsvProperties>
    {
        #region public fields

        /// <summary>
        /// Gets or sets the format provider used to en/decode values.
        /// </summary>
        public IFormatProvider Format;

        /// <summary>
        /// Gets or sets the <see cref="Encoding"/>.
        /// </summary>
        public StringEncoding Encoding;

        /// <summary>
        /// Gets or sets the <see cref="NewLineMode"/>.
        /// </summary>
        public NewLineMode NewLineMode;

        /// <summary>
        /// Gets or sets the separator for reading / writing csv files.
        /// </summary>
        public char Separator;

        /// <summary>
        /// Gets or sets the string start and end marker for reading / writing csv files.
        /// </summary>
        public char? StringMarker;

        /// <summary>
        /// Gets or sets the format of date time fields.
        /// </summary>
        public string DateTimeFormat;

        /// <summary>
        /// Allow differnent FieldCount and sorting at CSVReader.
        /// </summary>
        public bool AllowFieldMatching;

        /// <summary>The save default values (null, 0, ...)</summary>
        public bool SaveDefaultValues;

        /// <summary>CSV does not contain a header.</summary>
        public bool NoHeader;

        /// <summary>The string used to indicate a null value.</summary>
        public string NullValue;

        /// <summary>The false value to indicate a bool.false value (see <see cref="bool.FalseString"/>).</summary>
        public string FalseValue;

        /// <summary>The false value to indicate a bool.true value (see <see cref="bool.FalseString"/>).</summary>
        public string TrueValue;

        #endregion

        /// <summary>
        /// Gets <see cref="CsvProperties"/> with default settings:
        /// Encoding=UTF8, Compression=None, Separator=';', StringMarker='"'.
        /// </summary>
        public static CsvProperties Default { get; } = new CsvProperties
        {
            Format = CultureInfo.InvariantCulture,
            Encoding = StringEncoding.UTF8,
            NewLineMode = NewLineMode.LF,
            Separator = ';',
            StringMarker = '"',
            DateTimeFormat = StringExtensions.InterOpDateTimeFormat,
            AllowFieldMatching = false,
        };

        /// <summary>
        /// Gets <see cref="CsvProperties"/> with default settings for Microsoft Excel:
        /// Encoding=Current System Default, Compression=None, Separator='Tab', StringMarker='"'.
        /// </summary>
        public static CsvProperties Excel { get; } = new CsvProperties
        {
            SaveDefaultValues = true,
            Format = CultureInfo.CurrentUICulture,
            Encoding = StringEncoding.UTF16,
            NewLineMode = NewLineMode.CRLF,
            Separator = '\t',
            StringMarker = '"',
            DateTimeFormat = "yyyy-MM-dd HH:mm:ss" + CultureInfo.CurrentUICulture.NumberFormat.NumberDecimalSeparator + "fff",
            AllowFieldMatching = false,
        };

        /// <summary>
        /// Gets a value indicating whether the properties are all set or not.
        /// </summary>
        public bool Valid =>
            Enum.IsDefined(typeof(StringEncoding), Encoding) &&
            (Encoding != StringEncoding.Undefined) &&
            Enum.IsDefined(typeof(NewLineMode), NewLineMode) &&
            (NewLineMode != NewLineMode.Undefined) &&
            (Separator != StringMarker) &&
            (Format != null);

        #region operator

        /// <summary>Implements the operator ==.</summary>
        /// <param name="properties1">The properties1.</param>
        /// <param name="properties2">The properties2.</param>
        /// <returns>The result of the conversion.</returns>
        public static bool operator ==(CsvProperties properties1, CsvProperties properties2) => properties1.Equals(properties2);

        /// <summary>Implements the operator !=.</summary>
        /// <param name="properties1">The properties1.</param>
        /// <param name="properties2">The properties2.</param>
        /// <returns>The result of the conversion.</returns>
        public static bool operator !=(CsvProperties properties1, CsvProperties properties2) => !properties1.Equals(properties2);

        #endregion

        /// <summary>Determines whether the specified <see cref="object" />, is equal to this instance.</summary>
        /// <param name="obj">The <see cref="object" /> to compare with this instance.</param>
        /// <returns><c>true</c> if the specified <see cref="object" /> is equal to this instance; otherwise, <c>false</c>.</returns>
        public override bool Equals(object obj)
        {
            return !(obj is CsvProperties) ? false : base.Equals((CsvProperties)obj);
        }

        /// <summary>Determines whether the specified <see cref="CsvProperties" />, are equal to this instance.</summary>
        /// <param name="other">The <see cref="CsvProperties" /> to compare with this instance.</param>
        /// <returns><c>true</c> if the specified <see cref="CsvProperties" /> are equal to this instance; otherwise, <c>false</c>.</returns>
        public bool Equals(CsvProperties other)
        {
            return other.AllowFieldMatching == AllowFieldMatching
                && other.Format == Format
                && other.DateTimeFormat == DateTimeFormat
                && other.Encoding == Encoding
                && other.SaveDefaultValues == SaveDefaultValues
                && other.Separator == Separator
                && other.StringMarker == StringMarker
                && other.NoHeader == NoHeader
                && other.NullValue == NullValue
                && other.FalseValue == FalseValue
                && other.TrueValue == TrueValue;
        }

        /// <summary>Returns a hash code for this instance.</summary>
        /// <returns>A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table. </returns>
        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
    }
}
