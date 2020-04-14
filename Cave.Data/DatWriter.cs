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
    public sealed class DatWriter : IDisposable
    {
        /// <summary>The current version.</summary>
        public const int CurrentVersion = 4;

        readonly RowLayout layout;
        DataWriter writer;

        #region constructor

        /// <summary>
        /// Initializes a new instance of the <see cref="DatWriter"/> class.
        /// </summary>
        /// <param name="layout">Table layout.</param>
        /// <param name="fileName">Filename to write to.</param>
        public DatWriter(RowLayout layout, string fileName)
            : this(layout, File.Create(fileName))
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DatWriter"/> class.
        /// </summary>
        /// <param name="layout">Table layout.</param>
        /// <param name="stream">Stream to write to.</param>
        public DatWriter(RowLayout layout, Stream stream)
        {
            if (stream == null)
            {
                throw new ArgumentNullException("Stream");
            }

            writer = new DataWriter(stream);
            this.layout = layout;
            WriteFieldDefinition(writer, this.layout, CurrentVersion);
        }

        #endregion

        #region functions

        #region public static functions

        /// <summary>
        /// Creates a new dat file with the specified name and writes the whole table.
        /// </summary>
        /// <param name="fileName">Filename to write to.</param>
        /// <param name="table">Table to write.</param>
        public static void WriteTable(string fileName, ITable table)
        {
            if (table == null)
            {
                throw new ArgumentNullException("table");
            }

            using (var writer = new DatWriter(table.Layout, fileName))
            {
                writer.WriteTable(table);
            }
        }

        #endregion

        #region public functions

        /// <summary>
        /// Writes a row to the file.
        /// </summary>
        /// <param name="row">Row to write.</param>
        public void Write(Row row)
        {
            var data = GetData(row, CurrentVersion);
            WriteData(data);
        }

        /// <summary>
        /// Writes a row to the file.
        /// </summary>
        /// <param name="value">Row to write.</param>
        /// <typeparam name="TStruct">Structure type.</typeparam>
        public void Write<TStruct>(TStruct value)
            where TStruct : struct
        {
            Write(new Row(layout, layout.GetValues(value), false));
        }

        /// <summary>
        /// Writes a number of rows to the file.
        /// </summary>
        /// <param name="table">Table to write.</param>
        public void WriteTable(IEnumerable<Row> table)
        {
            if (table == null)
            {
                throw new ArgumentNullException(nameof(table));
            }

            foreach (Row row in table)
            {
                var data = GetData(row, CurrentVersion);
                WriteData(data);
            }
        }

        /// <summary>
        /// Writes a number of rows to the file.
        /// </summary>
        /// <param name="table">Table to write.</param>
        /// <typeparam name="TStruct">Structure type.</typeparam>
        public void WriteRows<TStruct>(IEnumerable<TStruct> table)
            where TStruct : struct
        {
            if (table == null)
            {
                throw new ArgumentNullException("table");
            }

            foreach (TStruct dataSet in table)
            {
                var row = new Row(layout, layout.GetValues(dataSet), false);
                var data = GetData(row, CurrentVersion);
                WriteData(data);
            }
        }

        /// <summary>
        /// Writes a full table of rows to the file.
        /// </summary>
        /// <param name="table">Table to write.</param>
        public void WriteTable(ITable table)
        {
            if (table == null)
            {
                throw new ArgumentNullException("table");
            }

            foreach (Row row in table.GetRows())
            {
                var data = GetData(row, CurrentVersion);
                WriteData(data);
            }
        }

        /// <summary>
        /// Closes the writer and the stream.
        /// </summary>
        public void Close()
        {
            if (writer != null)
            {
                writer.Close();
                writer = null;
            }
        }

        /// <inheritdoc/>
        public void Dispose() => Close();

        #endregion

        void WriteFieldDefinition(DataWriter writer, RowLayout layout, int version)
        {
            if (version < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(version));
            }

            if (version > 4)
            {
                throw new NotSupportedException("Version not supported!");
            }

            try
            {
                writer.Write("DatTable");
                writer.Write7BitEncoded32(version);
                writer.WritePrefixed(layout.Name);
                writer.Write7BitEncoded32(layout.FieldCount);
                for (var i = 0; i < layout.FieldCount; i++)
                {
                    var field = layout[i];
                    writer.WritePrefixed(field.Name);
                    writer.Write7BitEncoded32((int)field.DataType);
                    writer.Write7BitEncoded32((int)field.Flags);
                    switch (field.DataType)
                    {
                        case DataType.User:
                        case DataType.String:
                            if (version > 2)
                            {
                                writer.Write7BitEncoded32((int)field.StringEncoding);
                            }
                            break;
                        case DataType.DateTime:
                            if (version > 1)
                            {
                                writer.Write7BitEncoded32((int)field.DateTimeKind);
                                writer.Write7BitEncoded32((int)field.DateTimeType);
                            }
                            break;
                        case DataType.TimeSpan:
                            if (version > 3)
                            {
                                writer.Write7BitEncoded32((int)field.DateTimeType);
                            }
                            break;
                    }
                    if ((field.DataType & DataType.MaskRequireValueType) != 0)
                    {
                        var typeName = field.ValueType.AssemblyQualifiedName;
                        var parts = typeName.Split(',');
                        typeName = $"{parts[0]},{parts[1]}";
                        writer.WritePrefixed(typeName);
                    }
                }
                writer.Flush();
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(string.Format("Could not write field definition!"), ex);
            }
        }

        byte[] GetData(Row row, int version)
        {
            if (version < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(version));
            }

            if (version > 4)
            {
                throw new NotSupportedException("Version not supported!");
            }

            using (var buffer = new MemoryStream())
            {
                var writer = new DataWriter(buffer);
                for (var i = 0; i < layout.FieldCount; i++)
                {
                    var fieldProperties = layout[i];
                    switch (fieldProperties.DataType)
                    {
                        case DataType.Binary:
                        {
                            var data = (byte[])row[i];
                            if (version < 3)
                            {
                                if (data == null)
                                {
                                    data = new byte[0];
                                }

                                writer.Write(data.Length);
                                writer.Write(data);
                            }
                            else
                            {
                                writer.WritePrefixed(data);
                            }
                            break;
                        }
                        case DataType.Bool: writer.Write(Convert.ToBoolean(row[i] ?? default(bool))); break;
                        case DataType.TimeSpan: writer.Write(((TimeSpan)(row[i] ?? default(TimeSpan))).Ticks); break;
                        case DataType.DateTime: writer.Write(((DateTime)(row[i] ?? default(DateTime))).Ticks); break;
                        case DataType.Single: writer.Write((float)(row[i] ?? default(float))); break;
                        case DataType.Double: writer.Write((double)(row[i] ?? default(double))); break;
                        case DataType.Int8: writer.Write((sbyte)(row[i] ?? default(sbyte))); break;
                        case DataType.Int16: writer.Write((short)(row[i] ?? default(short))); break;
                        case DataType.UInt8: writer.Write((byte)(row[i] ?? default(byte))); break;
                        case DataType.UInt16: writer.Write((ushort)(row[i] ?? default(ushort))); break;
                        case DataType.Int32:
                            if (version == 1)
                            {
                                writer.Write((int)row[i]);
                                break;
                            }
                            writer.Write7BitEncoded32((int)(row[i] ?? default(int))); break;
                        case DataType.Int64:
                            if (version == 1)
                            {
                                writer.Write((long)row[i]);
                                break;
                            }
                            writer.Write7BitEncoded64((long)(row[i] ?? default(long))); break;
                        case DataType.UInt32:
                            if (version == 1)
                            {
                                writer.Write((uint)row[i]);
                                break;
                            }
                            writer.Write7BitEncoded32((uint)(row[i] ?? default(uint))); break;
                        case DataType.UInt64:
                            if (version == 1)
                            {
                                writer.Write((ulong)row[i]);
                                break;
                            }
                            writer.Write7BitEncoded64((ulong)(row[i] ?? default(ulong))); break;
                        case DataType.Char: writer.Write((char)(row[i] ?? default(char))); break;
                        case DataType.Decimal: writer.Write((decimal)(row[i] ?? default(decimal))); break;

                        case DataType.String:
                        case DataType.User:
                        {
                            var data = row[i];
                            if (data == null)
                            {
                                writer.WritePrefixed((string)null);
                            }
                            else
                            {
                                var text = data.ToString();

                                // check for invalid characters
                                switch (fieldProperties.StringEncoding)
                                {
                                    case StringEncoding.ASCII:
                                        if (!ASCII.IsClean(text))
                                        {
                                            throw new InvalidDataException(string.Format("Invalid character at field {0}", fieldProperties));
                                        }

                                        break;
                                    case StringEncoding.UTF16:
                                        text = Encoding.Unicode.GetString(Encoding.Unicode.GetBytes(text));
                                        break;
                                    case StringEncoding.UTF32:
                                        text = Encoding.Unicode.GetString(Encoding.UTF32.GetBytes(text));
                                        break;
                                    case StringEncoding.UTF8: break;
                                    default: throw new NotImplementedException();
                                }
                                writer.WritePrefixed(text);
                            }
                            break;
                        }

                        case DataType.Enum:
                        {
                            var value = Convert.ToInt64(row[i] ?? 0);
                            writer.Write7BitEncoded64(value);
                            break;
                        }

                        default:
                            throw new NotImplementedException(string.Format("Datatype {0} not implemented!", fieldProperties.DataType));
                    }
                }
                return buffer.ToArray();
            }
        }

        #endregion

        void WriteData(byte[] data)
        {
            var entrySize = data.Length + BitCoder32.GetByteCount7BitEncoded(data.Length + 10);
            var start = writer.BaseStream.Position;
            BitCoder32.Write7BitEncoded(writer, entrySize);
            writer.Write(data);

            var fill = start + entrySize - writer.BaseStream.Position;
            if (fill > 0)
            {
                writer.Write(new byte[fill]);
            }
            else if (fill < 0)
            {
                throw new IOException("Container too small!");
            }
        }
    }
}
