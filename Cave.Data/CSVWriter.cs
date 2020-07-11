using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Cave.IO;

namespace Cave.Data
{
    /// <summary>
    /// Provides writing of csv files using a struct or class.
    /// </summary>
    public sealed class CsvWriter : IDisposable
    {
        DataWriter writer;

        #region constructor

        /// <summary>
        /// Initializes a new instance of the <see cref="CsvWriter"/> class.
        /// </summary>
        /// <param name="layout">The table layout.</param>
        /// <param name="fileName">Filename to write to.</param>
        /// <param name="properties">Extended properties.</param>
        public CsvWriter(RowLayout layout, string fileName, CsvProperties properties = default)
            : this(layout, File.Create(fileName), properties, true)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CsvWriter"/> class.
        /// </summary>
        /// <param name="properties">Extended properties.</param>
        /// <param name="layout">The table layout.</param>
        /// <param name="stream">The stream.</param>
        /// <param name="closeBaseStream">if set to <c>true</c> [close base stream on close].</param>
        public CsvWriter(RowLayout layout, Stream stream, CsvProperties properties = default, bool closeBaseStream = false)
        {
            BaseStream = stream ?? throw new ArgumentNullException(nameof(Stream));
            Layout = layout ?? throw new ArgumentNullException(nameof(layout));
            Properties = properties.Valid ? properties : CsvProperties.Default;
            CloseBaseStream = closeBaseStream;

            writer = new DataWriter(stream, Properties.Encoding, Properties.NewLineMode);
            if (Properties.NoHeader)
            {
                return;
            }

            // write header
            for (var i = 0; i < Layout.FieldCount; i++)
            {
                if (i > 0)
                {
                    writer.Write(Properties.Separator);
                }

                if (Properties.StringMarker.HasValue)
                {
                    writer.Write(Properties.StringMarker.Value);
                }

                writer.Write(Layout[i].NameAtDatabase);
                if (Properties.StringMarker.HasValue)
                {
                    writer.Write(Properties.StringMarker.Value);
                }
            }
            writer.WriteLine();
            writer.Flush();
        }

        #endregion

        #region properties

        /// <summary>
        /// Gets the underlying base stream.
        /// </summary>
        public Stream BaseStream { get; }

        /// <summary>
        /// Gets the <see cref="CsvProperties"/>.
        /// </summary>
        public CsvProperties Properties { get; }

        /// <summary>
        /// Gets the row layout.
        /// </summary>
        public RowLayout Layout { get; }

        /// <summary>Gets a value indicating whether [close base stream on close].</summary>
        /// <value><c>true</c> if [close base stream on close]; otherwise, <c>false</c>.</value>
        public bool CloseBaseStream { get; }

        #endregion

        #region public static functions

        /// <summary>
        /// Creates a string representation of the specified row.
        /// </summary>
        /// <typeparam name="TStruct">The structure type.</typeparam>
        /// <param name="row">The row.</param>
        /// <param name="provider">The format provider used for each field.</param>
        /// <returns>Returns a new string representing the row.</returns>
        public static string RowToString<TStruct>(TStruct row, IFormatProvider provider)
            where TStruct : struct
        {
            var layout = RowLayout.CreateTyped(typeof(TStruct));
            return RowToString(CsvProperties.Default, layout, layout.GetRow(row));
        }

        /// <summary>
        /// Creates a string representation of the specified row.
        /// </summary>
        /// <param name="properties">The csv properties.</param>
        /// <param name="layout">The row layout.</param>
        /// <param name="row">The row.</param>
        /// <returns>Returns a new string representing the row.</returns>
        public static string RowToString(CsvProperties properties, RowLayout layout, Row row)
        {
            var result = new StringBuilder();
            var values = row.Values;
            for (var i = 0; i < layout.FieldCount; i++)
            {
                if (i > 0)
                {
                    result.Append(properties.Separator);
                }

                if ((values != null) && (values[i] != null))
                {
                    var field = layout[i];
                    switch (field.DataType)
                    {
                        case DataType.Binary:
                        {
                            var str = Base64.NoPadding.Encode((byte[])values[i]);
                            result.Append(str);
                            break;
                        }
                        case DataType.Bool:
                        case DataType.Int8:
                        case DataType.Int16:
                        case DataType.Int32:
                        case DataType.Int64:
                        case DataType.UInt8:
                        case DataType.UInt16:
                        case DataType.UInt32:
                        case DataType.UInt64:
                        {
                            if (!properties.SaveDefaultValues && values[i].Equals(0))
                            {
                                break;
                            }

                            var str = values[i].ToString();
                            result.Append(str);
                            break;
                        }
                        case DataType.Char:
                        {
                            if (!properties.SaveDefaultValues && values[i].Equals((char)0))
                            {
                                break;
                            }

                            var str = values[i].ToString();
                            result.Append(str);
                            break;
                        }
                        case DataType.Decimal:
                        {
                            if (!properties.SaveDefaultValues && values[i].Equals(0m))
                            {
                                break;
                            }

                            var value = (decimal)values[i];
                            result.Append(value.ToString(properties.Format));
                            break;
                        }
                        case DataType.Single:
                        {
                            if (!properties.SaveDefaultValues && values[i].Equals(0f))
                            {
                                break;
                            }

                            var value = (float)values[i];
                            result.Append(value.ToString("R", properties.Format));
                            break;
                        }
                        case DataType.Double:
                        {
                            if (!properties.SaveDefaultValues && values[i].Equals(0d))
                            {
                                break;
                            }

                            var value = (double)values[i];
                            result.Append(value.ToString("R", properties.Format));
                            break;
                        }
                        case DataType.TimeSpan:
                        {
                            if (!properties.SaveDefaultValues && values[i].Equals(TimeSpan.Zero))
                            {
                                break;
                            }

                            var str = field.GetString(values[i], $"{properties.StringMarker}", properties.Format);
                            result.Append(str);
                            break;
                        }
                        case DataType.DateTime:
                        {
                            if (!properties.SaveDefaultValues && values[i].Equals(new DateTime(0)))
                            {
                                break;
                            }

                            string str;
                            if (properties.DateTimeFormat != null)
                            {
                                str = ((DateTime)values[i]).ToString(properties.DateTimeFormat, properties.Format);
                            }
                            else
                            {
                                str = field.GetString(values[i], $"{properties.StringMarker}", properties.Format);
                            }
                            result.Append(str);
                            break;
                        }
                        case DataType.User:
                        case DataType.String:
                        {
                            if (!properties.SaveDefaultValues && values[i].Equals(string.Empty))
                            {
                                break;
                            }

                            var str = (values[i] == null) ? string.Empty : values[i].ToString();
                            str = str.EscapeUtf8();
                            if (properties.StringMarker.HasValue)
                            {
                                str = str.Replace($"{properties.StringMarker}", $"{properties.StringMarker}{properties.StringMarker}");
                                result.Append(properties.StringMarker);
                            }
                            if (str.Length == 0)
                            {
                                result.Append(" ");
                            }
                            else
                            {
                                if (properties.StringMarker.HasValue)
                                {
                                    if (str.StartsWith(properties.StringMarker.ToString()))
                                    {
                                        result.Append(" ");
                                    }
                                }

                                result.Append(str);
                                if (properties.StringMarker.HasValue)
                                {
                                    if (str.EndsWith(properties.StringMarker.ToString()))
                                    {
                                        result.Append(" ");
                                    }
                                }
                            }
                            if (properties.StringMarker.HasValue)
                            {
                                result.Append(properties.StringMarker);
                            }

                            break;
                        }
                        case DataType.Enum:
                        {
                            if (!properties.SaveDefaultValues && Convert.ToInt32(values[i]).Equals(0))
                            {
                                break;
                            }

                            var str = values[i].ToString();
                            result.Append(str);
                            break;
                        }
                        default:
                            throw new NotImplementedException(string.Format("DataType {0} is not implemented!", layout[i].DataType));
                    }
                }
            }
            return result.ToString();
        }

        /// <summary>
        /// Creates a new csv file with the specified name and writes the whole table.
        /// </summary>
        /// <param name="table">Table to write to the csv file.</param>
        /// <param name="fileName">File name of the csv file.</param>
        /// <param name="properties">Properties of the csv file.</param>
        public static void WriteTable(ITable table, string fileName, CsvProperties properties = default)
        {
            if (table == null)
            {
                throw new ArgumentNullException("Table");
            }

            var writer = new CsvWriter(table.Layout, fileName, properties);
            try
            {
                writer.Write(table);
            }
            finally
            {
                writer.Close();
            }
        }

        /// <summary>
        /// Creates a new csv file with the specified name and writes the whole table.
        /// </summary>
        /// <param name="table">Table to write to the csv file.</param>
        /// <param name="stream">The stream to write to.</param>
        /// <param name="properties">Properties of the csv file.</param>
        public static void WriteTable(ITable table, Stream stream, CsvProperties properties = default)
        {
            if (table == null)
            {
                throw new ArgumentNullException("Table");
            }

            var writer = new CsvWriter(table.Layout, stream, properties);
            try
            {
                writer.Write(table);
            }
            finally
            {
                writer.Close();
            }
        }

        /// <summary>
        /// Creates a new csv file with the specified name and writes the whole table.
        /// </summary>
        /// <param name="rows">Rows to write to the csv file.</param>
        /// <param name="fileName">File name of the csv file.</param>
        /// <param name="properties">Properties of the csv file.</param>
        /// <typeparam name="TStruct">Structure type.</typeparam>
        public static void WriteRows<TStruct>(IEnumerable<TStruct> rows, string fileName, CsvProperties properties = default)
            where TStruct : struct
        {
            var layout = RowLayout.CreateTyped(typeof(TStruct));
            var writer = new CsvWriter(layout, fileName, properties);
            try
            {
                writer.Write(rows);
            }
            finally
            {
                writer.Close();
            }
        }

        /// <summary>
        /// Creates a new csv file with the specified name and writes the whole table.
        /// </summary>
        /// <param name="rows">Table to write to the csv file.</param>
        /// <param name="fileName">File name of the csv file.</param>
        /// <param name="properties">Properties of the csv file.</param>
        /// <typeparam name="TStruct">Structure type.</typeparam>
        public static void WriteAlien<TStruct>(IEnumerable<TStruct> rows, string fileName, CsvProperties properties = default)
            where TStruct : struct
        {
            var layout = RowLayout.CreateAlien(typeof(TStruct), false);
            var writer = new CsvWriter(layout, fileName, properties);
            try
            {
                writer.Write(rows);
            }
            finally
            {
                writer.Close();
            }
        }

        #endregion

        #region instance functions

        /// <summary>
        /// Writes a row to the file.
        /// </summary>
        /// <param name="row">The row to write.</param>
        /// <typeparam name="TStruct">Structure type.</typeparam>
        public void Write<TStruct>(TStruct row)
            where TStruct : struct
            => WriteRow(new Row(Layout, Layout.GetValues(row), false));

        /// <summary>
        /// Writes a row to the file.
        /// </summary>
        /// <param name="row">Row to write.</param>
        public void WriteRow(Row row)
            => writer.WriteLine(RowToString(Properties, Layout, row));

        /// <summary>
        /// Writes a number of rows to the file.
        /// </summary>
        /// <param name="table">Table to write.</param>
        /// <typeparam name="TStruct">Structure type.</typeparam>
        public void Write<TStruct>(IEnumerable<TStruct> table)
            where TStruct : struct
        {
            if (table == null)
            {
                throw new ArgumentNullException(nameof(Table));
            }

            foreach (TStruct row in table)
            {
                Write(row);
            }
        }

        /// <summary>
        /// Writes a number of rows to the file.
        /// </summary>
        /// <param name="table">Table to write.</param>
        public void WriteRows(IEnumerable<Row> table)
        {
            if (table == null)
            {
                throw new ArgumentNullException(nameof(Table));
            }

            foreach (Row row in table)
            {
                WriteRow(row);
            }
        }

        /// <summary>
        /// Writes a full table of rows to the file.
        /// </summary>
        /// <param name="table">Table to write.</param>
        public void Write(ITable table)
        {
            if (table == null)
            {
                throw new ArgumentNullException(nameof(Table));
            }

            foreach (Row row in table.GetRows())
            {
                WriteRow(row);
            }
        }

        /// <summary>
        /// Closes the writer and the stream.
        /// </summary>
        public void Close()
        {
            if (CloseBaseStream)
            {
                writer?.Close();
            }

            writer = null;
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            Dispose(true);
        }

        #endregion

        #region IDisposable Support

        /// <summary>Releases unmanaged and - optionally - managed resources.</summary>
        /// <param name="disposing"><c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources.</param>
        void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (CloseBaseStream)
                {
                    if (writer != null)
                    {
                        writer.BaseStream.Dispose();
                    }
                }
                writer = null;
            }
        }

        #endregion
    }
}
