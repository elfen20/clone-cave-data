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
    public sealed class CSVReader : IDisposable
    {
        #region static ReadTable
        /// <summary>Reads a whole table from the specified csv stream.</summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="table">Table to read the csv file into.</param>
        /// <param name="properties">Properties of the csv file.</param>
        /// <param name="stream">The stream.</param>
        /// <exception cref="ArgumentNullException">Table.</exception>
        public static void ReadTable<T>(ITable<T> table, CSVProperties properties, Stream stream)
            where T : struct
        {
            if (table == null)
            {
                throw new ArgumentNullException(nameof(table));
            }

            using (CSVReader reader = new CSVReader(table.Layout, properties, stream))
            {
                reader.ReadTable(table);
            }
        }

        /// <summary>Reads a whole table from the specified csv file.</summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="table">Table to read the csv file into.</param>
        /// <param name="properties">Properties of the csv file.</param>
        /// <param name="fileName">File name of the csv file.</param>
        /// <exception cref="ArgumentNullException">Table.</exception>
        public static void ReadTable<T>(ITable<T> table, CSVProperties properties, string fileName)
            where T : struct
        {
            if (table == null)
            {
                throw new ArgumentNullException(nameof(table));
            }

            using (CSVReader reader = new CSVReader(table.Layout, properties, fileName))
            {
                reader.ReadTable(table);
            }
        }

        /// <summary>Reads a whole table from the specified csv stream.</summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="table">Table to read the csv file into.</param>
        /// <param name="stream">The stream.</param>
        public static void ReadTable<T>(ITable<T> table, Stream stream)
            where T : struct
        {
            ReadTable(table, CSVProperties.Default, stream);

        }
        /// <summary>Reads a whole table from the specified csv file.</summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="table">Table to read the csv file into.</param>
        /// <param name="fileName">File name of the csv file.</param>
        public static void ReadTable<T>(ITable<T> table, string fileName)
            where T : struct
        {
            ReadTable(table, CSVProperties.Default, fileName);
        }

        /// <summary>Reads a whole table from the specified csv lines.</summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="table">The table.</param>
        /// <param name="lines">The lines.</param>
        /// <exception cref="Exception"></exception>
        public static void ReadTable<T>(ITable<T> table, string[] lines)
            where T : struct
        {
            using (MemoryStream ms = new MemoryStream())
            {
                DataWriter w = new DataWriter(ms);
                foreach (string line in lines)
                {
                    w.WriteLine(line);
                }

                ms.Position = 0;
                CSVProperties properties = CSVProperties.Default;
                properties.AllowFieldMatching = true;
                ReadTable(table, properties, ms);
            }
        }
        #endregion

        #region static ReadList
        /// <summary>Reads a whole list from the specified csv stream.</summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="properties">Properties of the csv file.</param>
        /// <param name="stream">The stream.</param>
        /// <returns></returns>
        public static List<T> ReadList<T>(CSVProperties properties, Stream stream)
            where T : struct
        {
            RowLayout layout = RowLayout.CreateTyped(typeof(T));
            using (CSVReader reader = new CSVReader(layout, properties, stream))
            {
                return reader.ReadList<T>();
            }
        }

        /// <summary>Reads a whole list from the specified csv file.</summary>
        /// <param name="properties">Properties of the csv file.</param>
        /// <param name="fileName">File name of the csv file.</param>
        public static List<T> ReadList<T>(CSVProperties properties, string fileName)
            where T : struct
        {
            RowLayout layout = RowLayout.CreateTyped(typeof(T));
            using (CSVReader reader = new CSVReader(layout, properties, fileName))
            {
                return reader.ReadList<T>();
            }
        }

        /// <summary>Reads a whole list from the specified csv stream.</summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="stream">The stream.</param>
        /// <returns></returns>
        public static List<T> ReadList<T>(Stream stream)
            where T : struct
        {
            return ReadList<T>(CSVProperties.Default, stream);
        }

        /// <summary>Reads a whole list from the specified csv file.</summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fileName">File name of the csv file.</param>
        /// <returns></returns>
        public static List<T> ReadList<T>(string fileName)
            where T : struct
        {
            return ReadList<T>(CSVProperties.Default, fileName);
        }

        /// <summary>Reads a whole list from the specified csv lines.</summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="lines">The lines.</param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        public static List<T> ReadList<T>(string[] lines)
            where T : struct
        {
            using (MemoryStream ms = new MemoryStream())
            {
                DataWriter w = new DataWriter(ms);
                foreach (string line in lines)
                {
                    w.WriteLine(line);
                }

                ms.Position = 0;
                CSVProperties properties = CSVProperties.Default;
                properties.AllowFieldMatching = true;
                return ReadList<T>(properties, ms);
            }
        }
        #endregion

        #region private implementation

        DataReader m_Reader;
        bool m_CloseBaseStream;
        int m_CurrentRowNumber;
        int[] m_FieldNumberMatching;

        Row ReadRowData()
        {
            if (m_Reader == null)
            {
                throw new ObjectDisposedException("CSVReader");
            }

            string buffer = "";
            try
            {
                if (m_Reader.Available == 0)
                {
                    return null;
                }

                int fieldCount = Layout.FieldCount;
                int fieldNumber = 0;
                Queue<char> ident = new Queue<char>();
                int identInARowCount = 0;

                List<char> currentValue = new List<char>();
                int i = -1;
                object[] result = new object[fieldCount];

                while (fieldNumber < fieldCount)
                {
                    ++i;
                    if ((i == buffer.Length) && (fieldNumber == fieldCount - 1))
                    {
                        break;
                    }

                    while (i >= buffer.Length)
                    {
                        buffer = m_Reader.ReadLine();
                        i = 0;
                    }
                    if (Properties.Separator == buffer[i])
                    {
                        if (ident.Count == 0)
                        {
                            identInARowCount = 0;
                            if (Properties.StringMarker.HasValue)
                            {
                                result[fieldNumber] = Layout.ParseValue(fieldNumber, new string(currentValue.ToArray()), Properties.StringMarker.Value.ToString(), Properties.Culture);
                            }
                            else
                            {
                                result[fieldNumber] = Layout.ParseValue(fieldNumber, new string(currentValue.ToArray()), "", Properties.Culture);
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
                                //escaped char
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
                    throw new InvalidDataException(string.Format("Invalid ident at row {0}!", m_CurrentRowNumber));
                }

                if (Properties.StringMarker.HasValue)
                {
                    result[fieldNumber] = Layout.ParseValue(fieldNumber, new string(currentValue.ToArray()), Properties.StringMarker.Value.ToString(), Properties.Culture);
                }
                else
                {
                    result[fieldNumber] = Layout.ParseValue(fieldNumber, new string(currentValue.ToArray()), "", Properties.Culture);
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
                        throw new InvalidDataException(string.Format("Additional data at end of line in row {0}!", m_CurrentRowNumber));
                    }
                }
                m_CurrentRowNumber++;
                return new Row(result);
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
                throw new InvalidDataException(string.Format("Error while reading row data at row #{0}", m_CurrentRowNumber), ex);
            }
        }
        #endregion

        /// <summary>
        /// Creates a new csv file reader with default properties.
        /// </summary>
        /// <param name="layout">Layout to use when reading from the csv file.</param>
        /// <param name="fileName">Filename to write to.</param>
        public CSVReader(RowLayout layout, string fileName)
            : this(layout, CSVProperties.Default, File.OpenRead(fileName))
        {
            m_CloseBaseStream = true;
        }

        /// <summary>
        /// Creates a new csv file reader with default properties.
        /// </summary>
        /// <param name="layout">Layout to use when reading the csv data.</param>
        /// <param name="stream">Stream to read data from.</param>
        public CSVReader(RowLayout layout, Stream stream)
            : this(layout, CSVProperties.Default, stream)
        {
        }

        /// <summary>
        /// Creates a new csv file reader with the specified properties.
        /// </summary>
        /// <param name="properties">Properties to apply to the reader.</param>
        /// <param name="layout">Layout to use when reading from the csv file.</param>
        /// <param name="fileName">Filename to write to.</param>
        public CSVReader(RowLayout layout, CSVProperties properties, string fileName)
            : this(layout, properties, File.OpenRead(fileName))
        {
            m_CloseBaseStream = true;
        }

        /// <summary>
        /// Creates a new csv file reader with the specified properties.
        /// </summary>
        /// <param name="properties">Properties to apply to the reader.</param>
        /// <param name="layout">Layout to use when reading the csv data.</param>
        /// <param name="stream">Stream to read data from.</param>
        public CSVReader(RowLayout layout, CSVProperties properties, Stream stream)
        {
            Layout = layout;
            BaseStream = stream;
            if (stream == null)
            {
                throw new ArgumentNullException("Stream");
            }

            if (!properties.Valid)
            {
                throw new ArgumentException(string.Format("Invalid Property settings!"));
            }

            Properties = properties;
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
            m_Reader = new DataReader(stream, Properties.Encoding, Properties.NewLineMode);
            if (!Properties.NoHeader)
            {
                string header = m_Reader.ReadLine();
                m_CurrentRowNumber++;

                string[] fields = header.Split(Properties.Separator);

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
                        m_FieldNumberMatching = new int[Layout.FieldCount];
                    }
                }

                int count = Math.Min(Layout.FieldCount, fields.Length);
                for (int i = 0; i < count; i++)
                {
                    string fieldName = fields[i].UnboxText(false);
                    int fieldIndex = Layout.GetFieldIndex(fieldName);
                    if (fieldIndex < 0)
                    {
                        throw new InvalidDataException(string.Format("Error loading CSV Header! Got field name '{0}' instead of '{1}' at type '{2}'", fieldName, Layout.GetProperties(i).Name, Layout.Name));
                    }
                    if (!Properties.AllowFieldMatching)
                    {
                        if (fieldIndex != i)
                        {
                            throw new InvalidDataException(string.Format("Fieldposition of Field '{0}' does not match!", fieldName));
                        }

                        if (!string.Equals(Layout.GetProperties(fieldIndex).Name, fieldName))
                        {
                            throw new InvalidDataException(string.Format("Invalid header value at field number '{0}' name '{1}' expected '{2}'!", i, fieldName, Layout.GetProperties(fieldIndex).Name));
                        }
                    }
                    else
                    {
                        if ((m_FieldNumberMatching == null) && (fieldIndex != i))
                        {
                            m_FieldNumberMatching = new int[Layout.FieldCount];
                        }
                    }
                }

                if (m_FieldNumberMatching != null)
                {
                    int i = 0;
                    for (; i < count; i++)
                    {
                        string fieldName = fields[i].UnboxText(false);
                        m_FieldNumberMatching[i] = Layout.GetFieldIndex(fieldName);
                    }
                    for (; i < Layout.FieldCount; i++)
                    {
                        m_FieldNumberMatching[i] = -1;
                    }
                }
            }
        }

        /// <summary>
        /// Obtains the underlying base stream.
        /// </summary>
        public Stream BaseStream { get; private set; }

        /// <summary>
        /// Obtains the <see cref="CSVProperties"/>.
        /// </summary>
        public readonly CSVProperties Properties;

        /// <summary>
        /// Obtains the row layout.
        /// </summary>
        public RowLayout Layout { get; private set; }

        /// <summary>
        /// Reads a row from the file.
        /// </summary>
        /// <param name="row"></param>
        /// <returns></returns>
        public bool ReadRow(out Row row)
        {
            if (m_Reader == null)
            {
                throw new ObjectDisposedException(nameof(CSVReader));
            }

            row = ReadRowData();
            return row != null;
        }

        /// <summary>
        /// Reads a row from the file.
        /// </summary>
        /// <param name="row"></param>
        /// <returns></returns>
        public bool ReadRow<T>(out T row)
            where T : struct
        {
            if (m_Reader == null)
            {
                throw new ObjectDisposedException(nameof(CSVReader));
            }

            Row currentRow = ReadRowData();
            if (currentRow == null)
            {
                row = default(T);
                return false;
            }
            row = currentRow.GetStruct<T>(Layout);
            return true;
        }

        /// <summary>
        /// Reads the whole file to the specified table.
        /// </summary>
        /// <param name="table"></param>
        public void ReadTable<T>(ITable<T> table)
            where T : struct
        {
            if (m_Reader == null)
            {
                throw new ObjectDisposedException(nameof(CSVReader));
            }

            if (table == null)
            {
                throw new ArgumentNullException("Table");
            }

            Row row;
            while (null != (row = ReadRowData()))
            {
                table.Insert(row.GetStruct<T>(Layout));
            }
        }

        /// <summary>
        /// Reads the whole file to a new list.
        /// </summary>
        /// <returns></returns>
        public List<T> ReadList<T>()
            where T : struct
        {
            if (m_Reader == null)
            {
                throw new ObjectDisposedException(nameof(CSVReader));
            }

            List<T> result = new List<T>();
            Row row;
            while (null != (row = ReadRowData()))
            {
                result.Add(row.GetStruct<T>(Layout));
            }
            return result;
        }

        /// <summary>
        /// Skips a number of rows.
        /// </summary>
        /// <param name="rows"></param>
        /// <returns>if all rows have been skipped.</returns>
        public bool SkipRows(long rows = 0)
        {
            if (m_Reader == null)
            {
                throw new ObjectDisposedException(nameof(CSVReader));
            }

            long c = 0;
            while (c < rows)
            {
                try { m_Reader.ReadLine(); }
                catch (EndOfStreamException) { break; }
                c++;
            }
            return (c == rows);
        }



        /// <summary>
        /// Closes the reader.
        /// </summary>
        public void Close()
        {
            if (m_CloseBaseStream)
            {
                m_Reader?.Close();
            }
            m_Reader = null;
        }

        #region IDisposable Support

        /// <summary>Releases unmanaged and - optionally - managed resources.</summary>
        /// <param name="disposing"><c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources.</param>
        void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (m_CloseBaseStream)
                {
                    if (m_Reader != null)
                    {
                        m_Reader.BaseStream.Dispose();
                    }
                }
                m_Reader = null;
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
