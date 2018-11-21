#region CopyRight 2018
/*
    Copyright (c) 2005-2018 Andreas Rohleder (andreas@rohleder.cc)
    All rights reserved
*/
#endregion
#region License LGPL-3
/*
    This program/library/sourcecode is free software; you can redistribute it
    and/or modify it under the terms of the GNU Lesser General Public License
    version 3 as published by the Free Software Foundation subsequent called
    the License.

    You may not use this program/library/sourcecode except in compliance
    with the License. The License is included in the LICENSE file
    found at the installation directory or the distribution package.

    Permission is hereby granted, free of charge, to any person obtaining
    a copy of this software and associated documentation files (the
    "Software"), to deal in the Software without restriction, including
    without limitation the rights to use, copy, modify, merge, publish,
    distribute, sublicense, and/or sell copies of the Software, and to
    permit persons to whom the Software is furnished to do so, subject to
    the following conditions:

    The above copyright notice and this permission notice shall be included
    in all copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
    EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
    MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
    NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
    LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
    OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
    WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/
#endregion License
#region Authors & Contributors
/*
   Author:
     Andreas Rohleder <andreas@rohleder.cc>

   Contributors:
 */
#endregion Authors & Contributors

using System;
using System.Globalization;
using Cave.Compression;
using Cave.IO;

namespace Cave.Data
{
    /// <summary>
    /// Provides properties for CSV files
    /// </summary>
    public struct CSVProperties : IEquatable<CSVProperties>
    {
        /// <summary>
        /// Obtains <see cref="CSVProperties"/> with default settings:
        /// Encoding=UTF8, Compression=None, Separator=';', StringMarker='"'
        /// </summary>
        public static CSVProperties Default
        {
            get
            {
                CSVProperties result = new CSVProperties
                {
                    Culture = CultureInfo.InvariantCulture,
                    Compression = CompressionType.None,
                    Encoding = StringEncoding.UTF8,
                    NewLineMode = NewLineMode.LF,
                    Separator = ';',
                    StringMarker = '"',
                    DateTimeFormat = StringExtensions.InterOpDateTimeFormat,
                    AllowFieldMatching = false
                };
                return result;
            }
        }

        /// <summary>
        /// Obtains <see cref="CSVProperties"/> with default settings for Microsoft Excel:
        /// Encoding=Current System Default, Compression=None, Separator='Tab', StringMarker='"'
        /// </summary>
        public static CSVProperties Excel
        {
            get
            {
                CSVProperties result = new CSVProperties();
                result.SaveDefaultValues = true;
                result.Culture = CultureInfo.CurrentUICulture;
                result.Compression = CompressionType.None;
                result.Encoding = StringEncoding.UTF16;
                result.NewLineMode = NewLineMode.CRLF;
                result.Separator = '\t';
                result.StringMarker = '"';
                result.DateTimeFormat = "yyyy-MM-dd HH:mm:ss" + CultureInfo.CurrentUICulture.NumberFormat.NumberDecimalSeparator + "fff";
                result.AllowFieldMatching = false;
                return result;
            }
        }

        /// <summary>Implements the operator ==.</summary>
        /// <param name="properties1">The properties1.</param>
        /// <param name="properties2">The properties2.</param>
        /// <returns>The result of the conversion.</returns>
        public static bool operator ==(CSVProperties properties1, CSVProperties properties2)
        {
            if (ReferenceEquals(null, properties1))
            {
                return ReferenceEquals(null, properties2);
            }

            if (ReferenceEquals(null, properties2))
            {
                return false;
            }

            return properties1.Equals(properties2);
        }

        /// <summary>Implements the operator !=.</summary>
        /// <param name="properties1">The properties1.</param>
        /// <param name="properties2">The properties2.</param>
        /// <returns>The result of the conversion.</returns>
        public static bool operator !=(CSVProperties properties1, CSVProperties properties2)
        {
            if (ReferenceEquals(null, properties1))
            {
                return !ReferenceEquals(null, properties2);
            }

            if (ReferenceEquals(null, properties2))
            {
                return true;
            }

            return !properties1.Equals(properties2);
        }

        /// <summary>
        /// Gets / sets the culture used to en/decode values
        /// </summary>
        public CultureInfo Culture;

        /// <summary>
        /// Gets / sets the <see cref="CompressionType"/>
        /// </summary>
        public CompressionType Compression;

        /// <summary>
        /// Gets / sets the <see cref="Encoding"/>
        /// </summary>
        public StringEncoding Encoding;

        /// <summary>
        /// Gets / sets the <see cref="NewLineMode"/>
        /// </summary>
        public NewLineMode NewLineMode;

        /// <summary>
        /// Gets / sets the separator for reading / writing csv files.
        /// </summary>
        public char Separator;

        /// <summary>
        /// Gets / sets the string start and end marker for reading / writing csv files.
        /// </summary>
        public char? StringMarker;

        /// <summary>
        /// Gets / sets the format of date time fields
        /// </summary>
        public string DateTimeFormat;

        /// <summary>
        /// Allow differnent FieldCount and sorting at CSVReader
        /// </summary>
        public bool AllowFieldMatching;

        /// <summary>The save default values (null, 0, ...)</summary>
        public bool SaveDefaultValues;

        /// <summary>CSV does not contain a header</summary>
        public bool NoHeader;

        /// <summary>The string used to indicate a null value</summary>
        public string NullValue;

        /// <summary>The false value to indicate a bool.false value (see <see cref="bool.FalseString"/>)</summary>
        public string FalseValue;

        /// <summary>The false value to indicate a bool.true value (see <see cref="bool.FalseString"/>)</summary>
        public string TrueValue;

        /// <summary>
        /// Obtains whether the properties are all set or not
        /// </summary>
        public bool Valid
        {
            get
            {
                return
                    Enum.IsDefined(typeof(CompressionType), Compression) &&
                    Enum.IsDefined(typeof(StringEncoding), Encoding) &&
                    (Encoding !=  StringEncoding.Undefined) &&
                    Enum.IsDefined(typeof(NewLineMode), NewLineMode) &&
                    (NewLineMode != NewLineMode.Undefined) &&
                    (Separator != StringMarker) &&
                    (Culture != null);
            }
        }

        /// <summary>Determines whether the specified <see cref="object" />, is equal to this instance.</summary>
        /// <param name="obj">The <see cref="object" /> to compare with this instance.</param>
        /// <returns><c>true</c> if the specified <see cref="object" /> is equal to this instance; otherwise, <c>false</c>.</returns>
        public override bool Equals(object obj)
        {
            if (!(obj is CSVProperties))
            {
                return false;
            }

            return base.Equals((CSVProperties)obj);
        }

        /// <summary>Determines whether the specified <see cref="CSVProperties" />, are equal to this instance.</summary>
        /// <param name="other">The <see cref="CSVProperties" /> to compare with this instance.</param>
        /// <returns><c>true</c> if the specified <see cref="CSVProperties" /> are equal to this instance; otherwise, <c>false</c>.</returns>
        public bool Equals(CSVProperties other)
        {
            if (ReferenceEquals(null, other))
            {
                return false;
            }

            return other.AllowFieldMatching == AllowFieldMatching
                && other.Compression == Compression
                && other.Culture == Culture
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
