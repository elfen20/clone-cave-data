using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Text;
using Cave.Compression;
using Cave.IO;

namespace Cave.Data
{
    /// <summary>
    /// Provides writing of csv files using a struct or class.
    /// </summary>
    public sealed class CSVWriter : IDisposable
    {
        /// <summary>
        /// Creates a new csv file with the specified name and writes the whole table.
        /// </summary>
        /// <param name="table">Table to write to the csv file.</param>
        /// <param name="properties">Properties of the csv file.</param>
        /// <param name="fileName">File name of the csv file.</param>
        public static void WriteTable(ITable table, CSVProperties properties, string fileName)
        {
            if (table == null)
            {
                throw new ArgumentNullException("Table");
            }

            CSVWriter writer = new CSVWriter(properties, fileName);
            try
            {
                writer.SetLayout(table.Layout);
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
        /// <param name="properties">Properties of the csv file.</param>
        /// <param name="fileName">File name of the csv file.</param>
        public static void WriteAllRows<T>(IEnumerable<T> table, CSVProperties properties, string fileName)
            where T : struct
        {
            CSVWriter writer = new CSVWriter(properties, fileName);
            try
            {
                writer.SetLayout(typeof(T));
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
        /// <param name="properties">Properties of the csv file.</param>
        /// <param name="fileName">File name of the csv file.</param>
        public static void WriteAlienTable<T>(IEnumerable<T> table, CSVProperties properties, string fileName)
            where T : struct
        {
            CSVWriter writer = new CSVWriter(properties, fileName);
            try
            {
                writer.SetLayout(RowLayout.CreateAlien(typeof(T), false));
                writer.Write(table);
            }
            finally
            {
                writer.Close();
            }
        }

        DataWriter m_Writer;

        string m_RowToString(Row row)
        {
            if (Layout == null)
            {
                throw new InvalidOperationException("Use SetLayout first!");
            }

            StringBuilder result = new StringBuilder();
            object[] values = row.GetValues();
            for (int i = 0; i < Layout.FieldCount; i++)
            {
                if (i > 0)
                {
                    result.Append(Properties.Separator);
                }

                if ((values != null) && (values[i] != null))
                {
                    switch (Layout.GetProperties(i).DataType)
                    {
                        case DataType.Binary:
                        {
                            string str = Base64.NoPadding.Encode((byte[])values[i]);
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
                            if (!Properties.SaveDefaultValues && (values[i].Equals(0)))
                            {
                                break;
                            }

                            string str = values[i].ToString();
                            result.Append(str);
                            break;
                        }
                        case DataType.Char:
                        {
                            if (!Properties.SaveDefaultValues && (values[i].Equals((char)0)))
                            {
                                break;
                            }

                            string str = values[i].ToString();
                            result.Append(str);
                            break;
                        }                        
                        case DataType.Decimal:
                        {
                            if (!Properties.SaveDefaultValues && (values[i].Equals(0m)))
                            {
                                break;
                            }

                            decimal value = (decimal)values[i];
                            result.Append(value.ToString(Properties.Culture));
                            break;
                        }
                        case DataType.Single:
                        {
                            if (!Properties.SaveDefaultValues && (values[i].Equals(0f)))
                            {
                                break;
                            }

                            float value = (float)values[i];
                            result.Append(value.ToString("R", Properties.Culture));
                            break;
                        }
                        case DataType.Double:
                        {
                            if (!Properties.SaveDefaultValues && (values[i].Equals(0d)))
                            {
                                break;
                            }

                            double value = (double)values[i];
                            result.Append(value.ToString("R", Properties.Culture));
                            break;
                        }
                        case DataType.TimeSpan:
                        {
                            if (!Properties.SaveDefaultValues && (values[i].Equals(TimeSpan.Zero)))
                            {
                                break;
                            }

                            string str = values[i].ToString();
                            result.Append(str);
                            break;
                        }
                        case DataType.DateTime:
                        {
                            if (!Properties.SaveDefaultValues && (values[i].Equals(new DateTime(0))))
                            {
                                break;
                            }

                            string str = ((DateTime)values[i]).ToString(Properties.DateTimeFormat, Properties.Culture);
                            result.Append(str);
                            break;
                        }
                        case DataType.User:
                        case DataType.String:
                        {
                            if (!Properties.SaveDefaultValues && (values[i].Equals(string.Empty)))
                            {
                                break;
                            }

                            string str = (values[i] == null) ? "" : values[i].ToString();
                            str = str.Replace("\r", @"\r").Replace("\n", @"\n");
                            if (Properties.StringMarker.HasValue)
                            {
                                str = str.Replace("" + Properties.StringMarker, "" + Properties.StringMarker + Properties.StringMarker);
                                result.Append(Properties.StringMarker);
                            }
                            if (str.Length == 0)
                            {
                                result.Append(" ");
                            }
                            else
                            {
                                if (Properties.StringMarker.HasValue)
                                {
                                    if (str.StartsWith(Properties.StringMarker.ToString()))
                                    {
                                        result.Append(" ");
                                    }
                                }

                                result.Append(str);
                                if (Properties.StringMarker.HasValue)
                                {
                                    if (str.EndsWith(Properties.StringMarker.ToString()))
                                    {
                                        result.Append(" ");
                                    }
                                }
                            }
                            if (Properties.StringMarker.HasValue)
                            {
                                result.Append(Properties.StringMarker);
                            }

                            break;
                        }
                        case DataType.Enum:
                        {
                            if (!Properties.SaveDefaultValues && (Convert.ToInt32(values[i]).Equals(0)))
                            {
                                break;
                            }

                            string str = values[i].ToString();
                            result.Append(str);
                            break;
                        }
                        default:
                            throw new NotImplementedException(string.Format("DataType {0} is not implemented!", Layout.GetProperties(i).DataType));
                    }
                }
            }
            result.Append("\r\n");
            return result.ToString();
        }

        /// <summary>
        /// Creates a new csv file writer with default properties.
        /// </summary>
        /// <param name="fileName"></param>
        public CSVWriter(string fileName)
            : this(CSVProperties.Default, File.Create(fileName))
        {
            CloseBaseStream = true;
        }

        /// <summary>
        /// Creates a new csv file writer with the specified properties.
        /// </summary>
        /// <param name="properties"></param>
        /// <param name="fileName"></param>
        public CSVWriter(CSVProperties properties, string fileName)
            : this(properties, File.Create(fileName))
        {
            CloseBaseStream = true;
        }

        /// <summary>Creates a new csv file writer with default properties.</summary>
        /// <param name="stream">The stream.</param>
        /// <param name="closeBaseStream">if set to <c>true</c> [close base stream].</param>
        public CSVWriter(Stream stream, bool closeBaseStream = false)
            : this(CSVProperties.Default, stream, closeBaseStream)
        {
        }

        /// <summary>Creates a new csv file writer with the specified properties.</summary>
        /// <param name="properties">The properties.</param>
        /// <param name="stream">The stream.</param>
        /// <param name="closeBaseStream">if set to <c>true</c> [close base stream on close].</param>
        /// <exception cref="ArgumentNullException">Stream.</exception>
        /// <exception cref="ArgumentException"></exception>
        /// <exception cref="InvalidDataException"></exception>
        public CSVWriter(CSVProperties properties, Stream stream, bool closeBaseStream = false)
        {
            if (stream == null)
            {
                throw new ArgumentNullException("Stream");
            }

            if (!properties.Valid)
            {
                throw new ArgumentException(string.Format("Invalid Property settings!"));
            }

            CloseBaseStream = closeBaseStream;

            Properties = properties;
            switch (Properties.Compression)
            {
                case CompressionType.Deflate:
                    stream = new DeflateStream(stream, CompressionMode.Compress, true);
                    break;

                case CompressionType.GZip:
                    stream = new GZipStream(stream, CompressionMode.Compress, true);
                    break;

                case CompressionType.None: break;

                default: throw new InvalidDataException(string.Format("Unknown Compression {0}", Properties.Compression));
            }

            m_Writer = new DataWriter(stream, Properties.Encoding, Properties.NewLineMode);
        }

        /// <summary>
        /// Obtains the <see cref="CSVProperties"/>.
        /// </summary>
        public readonly CSVProperties Properties;

        /// <summary>
        /// Obtains the row layout.
        /// </summary>
        public RowLayout Layout { get; private set; }

        /// <summary>Gets or sets a value indicating whether [close base stream on close].</summary>
        /// <value><c>true</c> if [close base stream on close]; otherwise, <c>false</c>.</value>
        public bool CloseBaseStream { get; set; }

        /// <summary>
        /// Sets the layout for the writer.
        /// </summary>
        /// <param name="type">Type of the row.</param>
        public void SetLayout(Type type)
        {
            SetLayout(RowLayout.CreateTyped(type));
        }

        /// <summary>
        /// Sets the layout for the writer.
        /// </summary>
        /// <param name="layout"></param>
        public void SetLayout(RowLayout layout)
        {
            if (Layout != null)
            {
                throw new InvalidOperationException("Layout already set!");
            }

            Layout = layout;
            if (Properties.NoHeader)
            {
                return;
            }
            //write header
            for (int i = 0; i < Layout.FieldCount; i++)
            {
                if (i > 0)
                {
                    m_Writer.Write(Properties.Separator);
                }

                if (Properties.StringMarker.HasValue)
                {
                    m_Writer.Write(Properties.StringMarker.Value);
                }

                m_Writer.Write(Layout.GetProperties(i).NameAtDatabase);
                if (Properties.StringMarker.HasValue)
                {
                    m_Writer.Write(Properties.StringMarker.Value);
                }
            }
            m_Writer.Write("\r\n");
            m_Writer.Flush();
        }

        /// <summary>
        /// Writes a row to the file.
        /// </summary>
        /// <param name="row"></param>
        public void Write<T>(T row)
            where T : struct
        {
            if (Layout == null)
            {
                throw new InvalidOperationException("Use SetLayout first!");
            }

            WriteRow(new Row(Layout.GetValues(row)));
        }

        /// <summary>
        /// Writes a row to the file.
        /// </summary>
        /// <param name="row"></param>
        public void WriteRow(Row row)
        {
            m_Writer.Write(m_RowToString(row));
        }

        /// <summary>
        /// Writes a number of rows to the file.
        /// </summary>
        /// <param name="table"></param>
        public void Write<T>(IEnumerable<T> table)
            where T : struct
        {
            if (table == null)
            {
                throw new ArgumentNullException("Table");
            }

            foreach (T row in table)
            {
                Write(row);
            }
        }

        /// <summary>
        /// Writes a number of rows to the file.
        /// </summary>
        /// <param name="table"></param>
        public void WriteRows(IEnumerable<Row> table)
        {
            if (table == null)
            {
                throw new ArgumentNullException("Table");
            }

            foreach (Row row in table)
            {
                WriteRow(row);
            }
        }

        /// <summary>
        /// Writes a full table of rows to the file.
        /// </summary>
        /// <param name="table"></param>
        public void Write(ITable table)
        {
            if (table == null)
            {
                throw new ArgumentNullException("Table");
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
                m_Writer?.Close();
            }

            m_Writer = null;
        }

        #region IDisposable Support

        /// <summary>Releases unmanaged and - optionally - managed resources.</summary>
        /// <param name="disposing"><c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources.</param>
        void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (CloseBaseStream)
                {
                    if (m_Writer != null)
                    {
                        m_Writer.BaseStream.Dispose();
                    }
                }
                m_Writer = null;
            }
        }

        /// <summary>
        /// Releases unmanaged and managed resources.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
        }
        #endregion
    }
}
