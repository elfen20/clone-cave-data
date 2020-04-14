using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using Cave.Compression;
using Cave.IO;

namespace Cave.Data
{
    /// <summary>
    /// Provides reading of csv files to a struct / class.
    /// </summary>
    public sealed class CsvReader : IDisposable
    {
        DataReader reader;
        int currentRowNumber;
        int[] fieldNumberMatching;

        #region constructors

        /// <summary>
        /// Initializes a new instance of the <see cref="CsvReader"/> class.
        /// </summary>
        /// <param name="properties">Properties to apply to the reader.</param>
        /// <param name="layout">Layout to use when reading from the csv file.</param>
        /// <param name="fileName">Filename to write to.</param>
        public CsvReader(RowLayout layout, string fileName, CsvProperties properties = default)
            : this(layout, File.OpenRead(fileName), properties, true)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CsvReader"/> class.
        /// </summary>
        /// <param name="properties">Properties to apply to the reader.</param>
        /// <param name="layout">Layout to use when reading the csv data.</param>
        /// <param name="stream">Stream to read data from.</param>
        /// <param name="closeBaseStream">if set to <c>true</c> [close base stream on close].</param>
        public CsvReader(RowLayout layout, Stream stream, CsvProperties properties = default, bool closeBaseStream = false)
        {
            Layout = layout;
            BaseStream = stream ?? throw new ArgumentNullException(nameof(Stream));
            Properties = properties.Valid ? properties : CsvProperties.Default;
            CloseBaseStream = closeBaseStream;

            switch (Properties.Compression)
            {
                case CompressionType.Deflate:
                    stream = new DeflateStream(stream, CompressionMode.Decompress, true);
                    break;
                case CompressionType.GZip:
                    stream = new GZipStream(stream, CompressionMode.Decompress, true);
                    break;
                case CompressionType.None: break;
                default: throw new ArgumentException(string.Format("Unknown Compression {0}", Properties.Compression), "Compression");
            }

            reader = new DataReader(stream, Properties.Encoding, Properties.NewLineMode);
            if (!Properties.NoHeader)
            {
                var header = reader.ReadLine();
                currentRowNumber++;

                var fields = header.Split(Properties.Separator);

                if (!Properties.AllowFieldMatching)
                {
                    if (fields.Length != Layout.FieldCount)
                    {
                        if (fields.Length - 1 != Layout.FieldCount)
                        {
                            throw new InvalidDataException(string.Format("Invalid header fieldcount (expected '{0}' got '{1}')!", Layout.FieldCount, fields.Length));
                        }
                    }
                }
                else
                {
                    if (fields.Length != Layout.FieldCount)
                    {
                        fieldNumberMatching = new int[Layout.FieldCount];
                    }
                }

                var count = Math.Min(Layout.FieldCount, fields.Length);
                for (var i = 0; i < count; i++)
                {
                    var fieldName = fields[i].UnboxText(false);
                    var fieldIndex = Layout.GetFieldIndex(fieldName, false);
                    if (fieldIndex < 0)
                    {
                        throw new InvalidDataException(string.Format("Error loading CSV Header! Got field name '{0}' instead of '{1}' at type '{2}'", fieldName, Layout[i].Name, Layout.Name));
                    }
                    if (!Properties.AllowFieldMatching)
                    {
                        if (fieldIndex != i)
                        {
                            throw new InvalidDataException(string.Format("Fieldposition of Field '{0}' does not match!", fieldName));
                        }

                        if (!string.Equals(Layout[fieldIndex].Name, fieldName))
                        {
                            throw new InvalidDataException(string.Format("Invalid header value at field number '{0}' name '{1}' expected '{2}'!", i, fieldName, Layout[fieldIndex].Name));
                        }
                    }
                    else
                    {
                        if ((fieldNumberMatching == null) && (fieldIndex != i))
                        {
                            fieldNumberMatching = new int[Layout.FieldCount];
                        }
                    }
                }

                if (fieldNumberMatching != null)
                {
                    var i = 0;
                    for (; i < count; i++)
                    {
                        var fieldName = fields[i].UnboxText(false);
                        fieldNumberMatching[i] = Layout.GetFieldIndex(fieldName, false);
                    }
                    for (; i < Layout.FieldCount; i++)
                    {
                        fieldNumberMatching[i] = -1;
                    }
                }
            }
        }

        #endregion

        #region properties

        /// <summary>Gets a value indicating whether [close base stream on close].</summary>
        /// <value><c>true</c> if [close base stream on close]; otherwise, <c>false</c>.</value>
        public bool CloseBaseStream { get; }

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

        #endregion

        /// <summary>
        /// Parses a single row of data from the specified string.
        /// </summary>
        /// <typeparam name="TStruct">The structure type.</typeparam>
        /// <param name="data">The buffer to parse.</param>
        /// <param name="provider">The format provider (optional).</param>
        /// <returns>Returns a new structure.</returns>
        public static TStruct ParseRow<TStruct>(string data, IFormatProvider provider)
            where TStruct : struct
        {
            var properties = CsvProperties.Default;
            if (provider != null)
            {
                properties.Format = provider;
            }
            var layout = RowLayout.CreateTyped(typeof(TStruct));
            var row = ParseRow(properties, layout, data);
            return row.GetStruct<TStruct>(layout);
        }

        /// <summary>
        /// Parses a single row of data from the specified string.
        /// </summary>
        /// <param name="properties">The csv properties.</param>
        /// <param name="layout">The row layout.</param>
        /// <param name="data">The buffer to parse.</param>
        /// <returns>Returns a new <see cref="Row"/> instance.</returns>
        public static Row ParseRow(CsvProperties properties, RowLayout layout, string data)
        {
            try
            {
                var fieldCount = layout.FieldCount;
                var fieldNumber = 0;
                var ident = new Queue<char>();
                var identInARowCount = 0;

                var currentValue = new List<char>();
                var i = -1;
                var values = new object[fieldCount];

                while (fieldNumber < fieldCount)
                {
                    ++i;
                    if ((i == data.Length) && (fieldNumber == fieldCount - 1))
                    {
                        break;
                    }

                    if (i >= data.Length)
                    {
                        throw new InvalidDataException("Unexpected end of input!");
                    }
                    if (properties.Separator == data[i])
                    {
                        if (ident.Count == 0)
                        {
                            identInARowCount = 0;
                            if (properties.StringMarker.HasValue)
                            {
                                values[fieldNumber] = layout.ParseValue(fieldNumber, new string(currentValue.ToArray()).Unescape(), properties.StringMarker.Value.ToString(), properties.Format);
                            }
                            else
                            {
                                values[fieldNumber] = layout.ParseValue(fieldNumber, new string(currentValue.ToArray()).Unescape(), string.Empty, properties.Format);
                            }
                            fieldNumber++;
                            currentValue.Clear();
                            continue;
                        }
                    }
                    if (properties.StringMarker == data[i])
                    {
                        identInARowCount++;
                        if ((ident.Count > 0) && (ident.Peek() == data[i]))
                        {
                            ident.Dequeue();
                            if (identInARowCount > 1)
                            {
                                // escaped char
                                currentValue.Add(data[i]);
                            }
                        }
                        else
                        {
                            ident.Enqueue(data[i]);
                        }
                    }
                    else
                    {
                        identInARowCount = 0;
                        currentValue.Add(data[i]);
                    }
                }
                if (ident.Count > 0)
                {
                    throw new InvalidDataException("Invalid ident/escaping!");
                }

                if (properties.StringMarker.HasValue)
                {
                    values[fieldNumber] = layout.ParseValue(fieldNumber, new string(currentValue.ToArray()).Unescape(), properties.StringMarker.Value.ToString(), properties.Format);
                }
                else
                {
                    values[fieldNumber] = layout.ParseValue(fieldNumber, new string(currentValue.ToArray()).Unescape(), string.Empty, properties.Format);
                }
                fieldNumber++;
                if (i < data.Length)
                {
                    if (properties.Separator == data[i])
                    {
                        i++;
                    }

                    if (i < data.Length)
                    {
                        throw new InvalidDataException("Additional data at end of line!");
                    }
                }
                return new Row(layout, values, false);
            }
            catch (EndOfStreamException)
            {
                if (data.Length > 0)
                {
                    throw;
                }

                return null;
            }
            catch (Exception ex)
            {
                throw new InvalidDataException("Error while reading row data!", ex);
            }
        }

        #region public static ReadList()

        /// <summary>Reads a whole list from the specified csv stream.</summary>
        /// <param name="properties">Properties of the csv file.</param>
        /// <param name="stream">The stream.</param>
        /// <returns>Returns a new <see cref="List{TStruct}"/>.</returns>
        /// <typeparam name="TStruct">Structure type.</typeparam>
        public static List<TStruct> ReadList<TStruct>(CsvProperties properties, Stream stream)
            where TStruct : struct
        {
            var layout = RowLayout.CreateTyped(typeof(TStruct));
            using (var reader = new CsvReader(layout, stream, properties))
            {
                return reader.ReadList<TStruct>();
            }
        }

        /// <summary>Reads a whole list from the specified csv file.</summary>
        /// <param name="properties">Properties of the csv file.</param>
        /// <param name="fileName">File name of the csv file.</param>
        /// <returns>Returns a new <see cref="List{TStruct}"/>.</returns>
        /// <typeparam name="TStruct">Structure type.</typeparam>
        public static List<TStruct> ReadList<TStruct>(CsvProperties properties, string fileName)
            where TStruct : struct
        {
            var layout = RowLayout.CreateTyped(typeof(TStruct));
            using (var reader = new CsvReader(layout, fileName, properties))
            {
                return reader.ReadList<TStruct>();
            }
        }

        /// <summary>Reads a whole list from the specified csv stream.</summary>
        /// <typeparam name="TStruct">Structure type.</typeparam>
        /// <param name="stream">The stream.</param>
        /// <returns>Returns a new <see cref="List{TStruct}"/>.</returns>
        public static List<TStruct> ReadList<TStruct>(Stream stream)
            where TStruct : struct
        {
            return ReadList<TStruct>(CsvProperties.Default, stream);
        }

        /// <summary>Reads a whole list from the specified csv file.</summary>
        /// <param name="fileName">File name of the csv file.</param>
        /// <returns>Returns a new <see cref="List{TStruct}"/>.</returns>
        /// <typeparam name="TStruct">Structure type.</typeparam>
        public static List<TStruct> ReadList<TStruct>(string fileName)
            where TStruct : struct
        {
            return ReadList<TStruct>(CsvProperties.Default, fileName);
        }

        /// <summary>Reads a whole list from the specified csv lines.</summary>
        /// <param name="lines">The lines.</param>
        /// <returns>Returns a new <see cref="List{TStruct}"/>.</returns>
        /// <typeparam name="TStruct">Structure type.</typeparam>
        public static List<TStruct> ReadList<TStruct>(string[] lines)
            where TStruct : struct
        {
            using (var ms = new MemoryStream())
            {
                var w = new DataWriter(ms);
                foreach (var line in lines)
                {
                    w.WriteLine(line);
                }

                ms.Position = 0;
                CsvProperties properties = CsvProperties.Default;
                properties.AllowFieldMatching = true;
                return ReadList<TStruct>(properties, ms);
            }
        }
        #endregion

        #region instance functions

        #region ReadRow()

        /// <summary>
        /// Reads a row from the file.
        /// </summary>
        /// <param name="row">The read row.</param>
        /// <returns>Returns true on success, false if no further row can be read.</returns>
        public bool ReadRow(out Row row)
        {
            if (reader == null)
            {
                throw new ObjectDisposedException(nameof(CsvReader));
            }

            row = ReadRowData();
            return row != null;
        }

        /// <summary>
        /// Reads a row from the file.
        /// </summary>
        /// <param name="row">The read row.</param>
        /// <returns>Returns true on success, false if no further row can be read.</returns>
        /// <typeparam name="TStruct">Structure type.</typeparam>
        public bool ReadRow<TStruct>(out TStruct row)
            where TStruct : struct
        {
            if (reader == null)
            {
                throw new ObjectDisposedException(nameof(CsvReader));
            }

            Row currentRow = ReadRowData();
            if (currentRow == null)
            {
                row = default;
                return false;
            }
            row = currentRow.GetStruct<TStruct>(Layout);
            return true;
        }
        #endregion

        #region ReadTable()

        /// <summary>
        /// Reads the whole file to the specified table.
        /// </summary>
        /// <param name="table">The read table.</param>
        public void ReadTable(ITable table)
        {
            if (reader == null)
            {
                throw new ObjectDisposedException(nameof(CsvReader));
            }

            if (table == null)
            {
                throw new ArgumentNullException("Table");
            }

            Row row;
            while ((row = ReadRowData()) != null)
            {
                table.Insert(row);
            }
        }

        #endregion

        #region ReadList()

        /// <summary>
        /// Reads the whole file to a new list.
        /// </summary>
        /// <returns>Returns a new <see cref="List{TStruct}"/>.</returns>
        /// <typeparam name="TStruct">Structure type.</typeparam>
        public List<TStruct> ReadList<TStruct>()
            where TStruct : struct
        {
            if (reader == null)
            {
                throw new ObjectDisposedException(nameof(CsvReader));
            }

            var result = new List<TStruct>();
            Row row;
            while ((row = ReadRowData()) != null)
            {
                result.Add(row.GetStruct<TStruct>(Layout));
            }
            return result;
        }

        #endregion

        #region SkipRows()

        /// <summary>
        /// Skips a number of rows.
        /// </summary>
        /// <param name="rows">The number of rows.</param>
        /// <returns>if all rows have been skipped.</returns>
        public bool SkipRows(long rows = 0)
        {
            if (reader == null)
            {
                throw new ObjectDisposedException(nameof(CsvReader));
            }

            long c = 0;
            while (c < rows && ReadRowData() != null)
            {
                c++;
            }
            return c == rows;
        }

        #endregion

        #region Close()

        /// <summary>
        /// Closes the reader.
        /// </summary>
        public void Close()
        {
            if (CloseBaseStream)
            {
                reader?.Close();
            }
            reader = null;
        }

        #endregion

        /// <summary>
        /// Releases unmanaged and managed resources.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
        }

        #region private ReadRowData()

        Row ReadRowData()
        {
            // TODO use ParseRow and do only escaping and identcounting here
            if (reader == null)
            {
                throw new ObjectDisposedException("CSVReader");
            }

            var buffer = string.Empty;
            try
            {
                if (reader.Available == 0)
                {
                    return null;
                }

                var fieldCount = Layout.FieldCount;
                var fieldNumber = 0;
                var ident = new Queue<char>();
                var identInARowCount = 0;

                var currentValue = new List<char>();
                var i = -1;
                var values = new object[fieldCount];

                while (fieldNumber < fieldCount)
                {
                    ++i;
                    if ((i == buffer.Length) && (fieldNumber == fieldCount - 1))
                    {
                        break;
                    }

                    while (i >= buffer.Length)
                    {
                        buffer = reader.ReadLine();
                        i = 0;
                    }
                    if (Properties.Separator == buffer[i])
                    {
                        if (ident.Count == 0)
                        {
                            identInARowCount = 0;
                            if (Properties.StringMarker.HasValue)
                            {
                                values[fieldNumber] = Layout.ParseValue(fieldNumber, new string(currentValue.ToArray()).Unescape(), Properties.StringMarker.Value.ToString(), Properties.Format);
                            }
                            else
                            {
                                values[fieldNumber] = Layout.ParseValue(fieldNumber, new string(currentValue.ToArray()).Unescape(), string.Empty, Properties.Format);
                            }
                            fieldNumber++;
                            currentValue.Clear();
                            continue;
                        }
                    }
                    if (Properties.StringMarker == buffer[i])
                    {
                        identInARowCount++;
                        if ((ident.Count > 0) && (ident.Peek() == buffer[i]))
                        {
                            ident.Dequeue();
                            if (identInARowCount > 1)
                            {
                                // escaped char
                                currentValue.Add(buffer[i]);
                            }
                        }
                        else
                        {
                            ident.Enqueue(buffer[i]);
                        }
                    }
                    else
                    {
                        identInARowCount = 0;
                        currentValue.Add(buffer[i]);
                    }
                }
                if (ident.Count > 0)
                {
                    throw new InvalidDataException(string.Format("Invalid ident at row {0}!", currentRowNumber));
                }

                if (Properties.StringMarker.HasValue)
                {
                    values[fieldNumber] = Layout.ParseValue(fieldNumber, new string(currentValue.ToArray()).Unescape(), Properties.StringMarker.Value.ToString(), Properties.Format);
                }
                else
                {
                    values[fieldNumber] = Layout.ParseValue(fieldNumber, new string(currentValue.ToArray()).Unescape(), string.Empty, Properties.Format);
                }
                fieldNumber++;
                if (i < buffer.Length)
                {
                    if (Properties.Separator == buffer[i])
                    {
                        i++;
                    }

                    if (i < buffer.Length)
                    {
                        throw new InvalidDataException(string.Format("Additional data at end of line in row {0}!", currentRowNumber));
                    }
                }
                currentRowNumber++;
                return new Row(Layout, values, false);
            }
            catch (EndOfStreamException)
            {
                if (buffer.Length > 0)
                {
                    throw;
                }

                return null;
            }
            catch (Exception ex)
            {
                throw new InvalidDataException(string.Format("Error while reading row data at row #{0}", currentRowNumber), ex);
            }
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
                    if (reader != null)
                    {
                        reader.BaseStream.Dispose();
                    }
                }
                reader = null;
            }
        }

        #endregion

        #endregion
    }
}
