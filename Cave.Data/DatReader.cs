using System;
using System.Collections.Generic;
using System.IO;
using Cave.IO;

namespace Cave.Data
{
    /// <summary>
    /// Provides reading of dat files to a struct / class.
    /// </summary>
    public sealed class DatReader : IDisposable
    {
        DataReader reader;

        #region constructor

        /// <summary>
        /// Initializes a new instance of the <see cref="DatReader"/> class.
        /// </summary>
        /// <param name="fileName">Filename to read from.</param>
        public DatReader(string fileName)
        {
            Stream stream = File.OpenRead(fileName);
            try
            {
                Load(stream);
            }
            catch
            {
                stream.Dispose();
                throw;
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DatReader"/> class.
        /// </summary>
        /// <param name="stream">Stream to read from.</param>
        public DatReader(Stream stream)
        {
            Load(stream);
        }

        #endregion

        /// <summary>
        /// Gets the layout of the table.
        /// </summary>
        public RowLayout Layout { get; private set; }

        /// <summary>
        /// Gets the version the database was created with.
        /// </summary>
        public int Version { get; private set; }

        #region functions

        #region public static functions

        /// <summary>
        /// Reads a whole table from the specified dat file.
        /// </summary>
        /// <param name="table">Table to read the dat file into.</param>
        /// <param name="fileName">File name of the dat file.</param>
        public static void ReadTable(ITable table, string fileName)
        {
            using (var reader = new DatReader(fileName))
            {
                reader.ReadTable(table);
            }
        }

        /// <summary>Reads a whole table from the specified dat file.</summary>
        /// <param name="table">Table to read the dat file into.</param>
        /// <param name="stream">The stream.</param>
        public static void ReadTable(ITable table, Stream stream)
        {
            using (var reader = new DatReader(stream))
            {
                reader.ReadTable(table);
            }
        }

        #endregion

        /// <summary>
        /// Reads a row from the file.
        /// </summary>
        /// <param name="checkLayout">Check layout prior read.</param>
        /// <param name="row">The read row.</param>
        /// <typeparam name="TStruct">Structure type.</typeparam>
        /// <returns>Returns true is the row was read, false otherwise.</returns>
        public bool ReadRow<TStruct>(bool checkLayout, out TStruct row)
            where TStruct : struct
        {
            var layout = RowLayout.CreateTyped(typeof(TStruct));
            if (checkLayout)
            {
                RowLayout.CheckLayout(Layout, layout);
            }

            if (!Layout.IsTyped)
            {
                Layout = layout;
            }

            while (reader.BaseStream.Position < reader.BaseStream.Length)
            {
                Row currentRow = ReadCurrentRow(reader, Version, layout);
                if (currentRow != null)
                {
                    row = currentRow.GetStruct<TStruct>(layout);
                    return true;
                }
            }
            row = default;
            return false;
        }

        /// <summary>
        /// Reads the whole file to the specified table. This does not write transactions and does not clear the table.
        /// If you want to start with a clean table clear it prior using this function.
        /// </summary>
        /// <param name="table">Table to read to.</param>
        public void ReadTable(ITable table)
        {
            if (table == null)
            {
                throw new ArgumentNullException("Table");
            }

            RowLayout.CheckLayout(Layout, table.Layout);
            if (!Layout.IsTyped)
            {
                Layout = table.Layout;
            }

            while (reader.BaseStream.Position < reader.BaseStream.Length)
            {
                Row row = ReadCurrentRow(reader, Version, Layout);
                if (row != null)
                {
                    table.Insert(row);
                }
            }
        }

        /// <summary>
        /// Reads the whole file to a new list.
        /// </summary>
        /// <typeparam name="TStruct">Structure type.</typeparam>
        /// <returns>A new <see cref="List{TStruct}"/>.</returns>
        public List<TStruct> ReadList<TStruct>()
            where TStruct : struct
        {
            var layout = RowLayout.CreateTyped(typeof(TStruct));
            RowLayout.CheckLayout(Layout, layout);
            if (!Layout.IsTyped)
            {
                Layout = layout;
            }

            var result = new List<TStruct>();
            while (reader.BaseStream.Position < reader.BaseStream.Length)
            {
                Row row = ReadCurrentRow(reader, Version, layout);
                if (row != null)
                {
                    result.Add(row.GetStruct<TStruct>(layout));
                }
            }
            return result;
        }

        /// <summary>
        /// Closes the reader.
        /// </summary>
        public void Close()
        {
            if (reader != null)
            {
                reader.Close();
                reader = null;
            }
        }

        /// <summary>
        /// Disposes the base stream.
        /// </summary>
        public void Dispose()
        {
            Close();
        }

        #region internal static functions

        internal static RowLayout LoadFieldDefinition(DataReader reader, out int version)
        {
            DateTimeKind dateTimeKind = DateTimeKind.Unspecified;
            DateTimeType dateTimeType = DateTimeType.Undefined;
            StringEncoding stringEncoding = StringEncoding.UTF8;
            if (reader.ReadString(8) != "DatTable")
            {
                throw new FormatException();
            }

            version = reader.Read7BitEncodedInt32();
            if (version < 1 || version > 4)
            {
                throw new InvalidDataException(string.Format("Unknown Table version!"));
            }

            // read name and create layout
            var layoutName = reader.ReadString();
            var fieldCount = reader.Read7BitEncodedInt32();
            var fields = new FieldProperties[fieldCount];
            for (var i = 0; i < fieldCount; i++)
            {
                var fieldName = reader.ReadString();
                var dataType = (DataType)reader.Read7BitEncodedInt32();
                var fieldFlags = (FieldFlags)reader.Read7BitEncodedInt32();

                DataType databaseDataType = dataType;

                switch (dataType)
                {
                    case DataType.Enum:
                        databaseDataType = DataType.Int64;
                        break;
                    case DataType.User:
                    case DataType.String:
                        databaseDataType = DataType.String;
                        if (version > 2)
                        {
                            stringEncoding = (StringEncoding)reader.Read7BitEncodedInt32();
                        }
                        else
                        {
                            stringEncoding = StringEncoding.UTF8;
                        }
                        break;
                    case DataType.TimeSpan:
                        if (version > 3)
                        {
                            dateTimeType = (DateTimeType)reader.Read7BitEncodedInt32();
                        }
                        break;
                    case DataType.DateTime:
                        if (version > 1)
                        {
                            dateTimeKind = (DateTimeKind)reader.Read7BitEncodedInt32();
                            dateTimeType = (DateTimeType)reader.Read7BitEncodedInt32();
                        }
                        else
                        {
                            dateTimeKind = DateTimeKind.Utc;
                            dateTimeType = DateTimeType.BigIntHumanReadable;
                        }
                        break;
                }

                Type valueType = null;
                if ((dataType & DataType.MaskRequireValueType) != 0)
                {
                    var typeName = reader.ReadString();
                    valueType = AppDom.FindType(typeName: typeName.BeforeFirst(','), assemblyName: typeName.AfterFirst(',').Trim());
                }

                var field = fields[i] = new FieldProperties()
                {
                    Index = i,
                    Flags = fieldFlags,
                    DataType = dataType,
                    ValueType = valueType,
                    Name = fieldName,
                    TypeAtDatabase = databaseDataType,
                    NameAtDatabase = fieldName,
                    DateTimeType = dateTimeType,
                    DateTimeKind = dateTimeKind,
                    StringEncoding = stringEncoding,
                };
                field.Validate();
            }
            return RowLayout.CreateUntyped(layoutName, fields);
        }

        internal static Row ReadCurrentRow(DataReader reader, int version, RowLayout layout)
        {
            if (version < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(version));
            }

            if (version > 4)
            {
                throw new NotSupportedException("Version not supported!");
            }

            var dataStart = reader.BaseStream.Position;
            var dataSize = reader.Read7BitEncodedInt32();

            if (dataSize == 0)
            {
                return null;
            }

            var values = new object[layout.FieldCount];
            for (var i = 0; i < layout.FieldCount; i++)
            {
                var field = layout[i];
                DataType dataType = field.DataType;

                switch (dataType)
                {
                    case DataType.Binary:
                        if (version >= 3)
                        {
                            values[i] = reader.ReadBytes();
                        }
                        else
                        {
                            var size = reader.ReadInt32();
                            values[i] = reader.ReadBytes(size);
                        }
                        break;

                    case DataType.Bool: values[i] = reader.ReadBool(); break;
                    case DataType.DateTime: values[i] = new DateTime(reader.ReadInt64(), field.DateTimeKind); break;
                    case DataType.TimeSpan: values[i] = new TimeSpan(reader.ReadInt64()); break;
                    case DataType.Int8: values[i] = reader.ReadInt8(); break;
                    case DataType.Int16: values[i] = reader.ReadInt16(); break;
                    case DataType.Int32:
                        if (version == 1)
                        {
                            values[i] = reader.ReadInt32();
                            break;
                        }
                        values[i] = reader.Read7BitEncodedInt32(); break;
                    case DataType.Int64:
                        if (version == 1)
                        {
                            values[i] = reader.ReadInt64();
                            break;
                        }
                        values[i] = reader.Read7BitEncodedInt64(); break;
                    case DataType.UInt32:
                        if (version == 1)
                        {
                            values[i] = reader.ReadUInt32();
                            break;
                        }
                        values[i] = reader.Read7BitEncodedUInt32(); break;
                    case DataType.UInt64:
                        if (version == 1)
                        {
                            values[i] = reader.ReadUInt64();
                            break;
                        }
                        values[i] = reader.Read7BitEncodedUInt64(); break;
                    case DataType.UInt8: values[i] = reader.ReadUInt8(); break;
                    case DataType.UInt16: values[i] = reader.ReadUInt16(); break;
                    case DataType.Char: values[i] = reader.ReadChar(); break;
                    case DataType.Single: values[i] = reader.ReadSingle(); break;
                    case DataType.Double: values[i] = reader.ReadDouble(); break;
                    case DataType.Decimal: values[i] = reader.ReadDecimal(); break;
                    case DataType.String: values[i] = reader.ReadString(); break;
                    case DataType.Enum: values[i] = layout.EnumValue(i, reader.Read7BitEncodedInt64()); break;
                    case DataType.User: values[i] = layout.ParseValue(i, reader.ReadString(), null); break;

                    default: throw new NotImplementedException(string.Format("Datatype {0} not implemented!", dataType));
                }
            }
            var skip = dataStart + dataSize - reader.BaseStream.Position;
            if (skip < 0)
            {
                throw new FormatException();
            }

            if (skip > 0)
            {
                reader.BaseStream.Seek(skip, SeekOrigin.Current);
            }

            return new Row(layout, values, false);
        }

        #endregion

        void Load(Stream stream)
        {
            if (stream == null)
            {
                throw new ArgumentNullException("Stream");
            }

            reader = new DataReader(stream);
            Layout = LoadFieldDefinition(reader, out var version);
            Version = version;
        }

        #endregion
    }
}
