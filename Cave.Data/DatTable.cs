using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using Cave.Collections.Generic;
using Cave.IO;

namespace Cave.Data
{
    /// <summary>
    /// Provides a binary dat file database table.
    /// </summary>
    public class DatTable : Table, IDisposable
    {
        /// <summary>The current version.</summary>
        public const int CurrentVersion = 4;

        #region static functionality

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
            if ((version < 1) || (version > 4))
            {
                throw new InvalidDataException(string.Format("Unknown Table version!"));
            }

            // read name and create layout
            var layoutName = reader.ReadString();
            var fields = new List<FieldProperties>();
            var fieldCount = reader.Read7BitEncodedInt32();
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
                    valueType = AppDom.FindType(typeName, AppDom.LoadMode.NoException);
                    if (valueType == null)
                    {
                        throw new ArgumentException(string.Format("Could not load .NET type {0} for DataType {1} at table {2}", typeName, dataType, layoutName));
                    }
                }

                var properties = new FieldProperties(layoutName, fieldFlags, dataType, valueType, 0, fieldName, databaseDataType, dateTimeType, dateTimeKind, stringEncoding, fieldName, null, null, null);
                fields.Add(properties);
            }
            return RowLayout.CreateUntyped(layoutName, fields.ToArray());
        }

        internal static void WriteFieldDefinition(DataWriter writer, RowLayout layout, int version)
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
                    FieldProperties properties = layout.GetProperties(i);
                    writer.WritePrefixed(properties.Name);
                    writer.Write7BitEncoded32((int)properties.DataType);
                    writer.Write7BitEncoded32((int)properties.Flags);
                    switch (properties.DataType)
                    {
                        case DataType.User:
                        case DataType.String:
                            if (version > 2)
                            {
                                writer.Write7BitEncoded32((int)properties.StringEncoding);
                            }
                            break;
                        case DataType.DateTime:
                            if (version > 1)
                            {
                                writer.Write7BitEncoded32((int)properties.DateTimeKind);
                                writer.Write7BitEncoded32((int)properties.DateTimeType);
                            }
                            break;
                        case DataType.TimeSpan:
                            if (version > 3)
                            {
                                writer.Write7BitEncoded32((int)properties.DateTimeType);
                            }
                            break;
                    }
                    if ((properties.DataType & DataType.MaskRequireValueType) != 0)
                    {
                        var typeName = properties.ValueType.AssemblyQualifiedName;
                        typeName = typeName.Substring(0, typeName.IndexOf(","));
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

        internal static byte[] GetData(RowLayout layout, Row row, int version)
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
                    FieldProperties fieldProperties = layout.GetProperties(i);
                    switch (fieldProperties.DataType)
                    {
                        case DataType.Binary:
                        {
                            var data = (byte[])row.GetValue(i);
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
                        case DataType.Bool: writer.Write(Convert.ToBoolean(row.GetValue(i) ?? default(bool))); break;
                        case DataType.TimeSpan: writer.Write(((TimeSpan)(row.GetValue(i) ?? default(TimeSpan))).Ticks); break;
                        case DataType.DateTime: writer.Write(((DateTime)(row.GetValue(i) ?? default(DateTime))).Ticks); break;
                        case DataType.Single: writer.Write((float)(row.GetValue(i) ?? default(float))); break;
                        case DataType.Double: writer.Write((double)(row.GetValue(i) ?? default(double))); break;
                        case DataType.Int8: writer.Write((sbyte)(row.GetValue(i) ?? default(sbyte))); break;
                        case DataType.Int16: writer.Write((short)(row.GetValue(i) ?? default(short))); break;
                        case DataType.UInt8: writer.Write((byte)(row.GetValue(i) ?? default(byte))); break;
                        case DataType.UInt16: writer.Write((ushort)(row.GetValue(i) ?? default(ushort))); break;
                        case DataType.Int32:
                            if (version == 1)
                            {
                                writer.Write((int)row.GetValue(i));
                                break;
                            }
                            writer.Write7BitEncoded32((int)(row.GetValue(i) ?? default(int))); break;
                        case DataType.Int64:
                            if (version == 1)
                            {
                                writer.Write((long)row.GetValue(i));
                                break;
                            }
                            writer.Write7BitEncoded64((long)(row.GetValue(i) ?? default(long))); break;
                        case DataType.UInt32:
                            if (version == 1)
                            {
                                writer.Write((uint)row.GetValue(i));
                                break;
                            }
                            writer.Write7BitEncoded32((uint)(row.GetValue(i) ?? default(uint))); break;
                        case DataType.UInt64:
                            if (version == 1)
                            {
                                writer.Write((ulong)row.GetValue(i));
                                break;
                            }
                            writer.Write7BitEncoded64((ulong)(row.GetValue(i) ?? default(ulong))); break;
                        case DataType.Char: writer.Write((char)(row.GetValue(i) ?? default(char))); break;
                        case DataType.Decimal: writer.Write((decimal)(row.GetValue(i) ?? default(decimal))); break;

                        case DataType.String:
                        case DataType.User:
                        {
                            var data = row.GetValue(i);
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
                            var value = Convert.ToInt64(row.GetValue(i) ?? 0);
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

            var row = new object[layout.FieldCount];
            for (var i = 0; i < layout.FieldCount; i++)
            {
                FieldProperties field = layout.GetProperties(i);
                DataType dataType = field.DataType;

                switch (dataType)
                {
                    case DataType.Binary:
                        if (version >= 3)
                        {
                            row[i] = reader.ReadBytes();
                        }
                        else
                        {
                            var size = reader.ReadInt32();
                            row[i] = reader.ReadBytes(size);
                        }
                        break;

                    case DataType.Bool: row[i] = reader.ReadBool(); break;
                    case DataType.DateTime: row[i] = new DateTime(reader.ReadInt64(), field.DateTimeKind); break;
                    case DataType.TimeSpan: row[i] = new TimeSpan(reader.ReadInt64()); break;
                    case DataType.Int8: row[i] = reader.ReadInt8(); break;
                    case DataType.Int16: row[i] = reader.ReadInt16(); break;
                    case DataType.Int32:
                        if (version == 1)
                        {
                            row[i] = reader.ReadInt32();
                            break;
                        }
                        row[i] = reader.Read7BitEncodedInt32(); break;
                    case DataType.Int64:
                        if (version == 1)
                        {
                            row[i] = reader.ReadInt64();
                            break;
                        }
                        row[i] = reader.Read7BitEncodedInt64(); break;
                    case DataType.UInt32:
                        if (version == 1)
                        {
                            row[i] = reader.ReadUInt32();
                            break;
                        }
                        row[i] = reader.Read7BitEncodedUInt32(); break;
                    case DataType.UInt64:
                        if (version == 1)
                        {
                            row[i] = reader.ReadUInt64();
                            break;
                        }
                        row[i] = reader.Read7BitEncodedUInt64(); break;
                    case DataType.UInt8: row[i] = reader.ReadUInt8(); break;
                    case DataType.UInt16: row[i] = reader.ReadUInt16(); break;
                    case DataType.Char: row[i] = reader.ReadChar(); break;
                    case DataType.Single: row[i] = reader.ReadSingle(); break;
                    case DataType.Double: row[i] = reader.ReadDouble(); break;
                    case DataType.Decimal: row[i] = reader.ReadDecimal(); break;
                    case DataType.String: row[i] = reader.ReadString(); break;
                    case DataType.Enum: row[i] = layout.EnumValue(i, reader.Read7BitEncodedInt64()); break;
                    case DataType.User: row[i] = layout.ParseValue(i, reader.ReadString(), null); break;

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

            return new Row(row);
        }

        static RowLayout GetLayout(string file)
        {
            using (Stream stream = File.Open(file, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
            {
                var reader = new DataReader(stream);
                return LoadFieldDefinition(reader, out var version);
            }
        }
        #endregion

        #region private variables

        long currentLength = 0;
        string dataFile;
        string indexFile;
        long start;

        internal Stream FileStream;
        internal DatIndex Index;

        #endregion

        #region internal implementation
        internal void Recreate()
        {
            Index?.Dispose();
            if (FileStream != null)
            {
                FileStream.Close();
            }

            FileStream = File.Open(dataFile, FileMode.Create, FileAccess.ReadWrite, FileShare.Read);
            WriteFieldDefinition(new DataWriter(FileStream), Layout, Version);
            currentLength = FileStream.Position;
            File.Delete(indexFile);
            Index = new DatIndex(indexFile);
        }

        internal void RebuildIndex()
        {
            FileStream.Position = start;
            Index = new DatIndex(indexFile);
            Trace.TraceWarning(string.Format("Rebuilding index of '{0}' after unclean shutdown...", this));
            var reader = new DataReader(FileStream);
            try
            {
                while (FileStream.Position < FileStream.Length)
                {
                    var current = FileStream.Position;
                    Row row = ReadCurrentRow(reader, Version, Layout);
                    if (row != null)
                    {
                        var entry = new DatEntry(Layout.GetID(row), current, (int)(FileStream.Position - current));
                        Index.Save(entry);
                    }
                    else
                    {
                        var count = 1;
                        while (FileStream.ReadByte() == 0)
                        {
                            count++;
                        }
                        if (FileStream.Position < FileStream.Length)
                        {
                            FileStream.Position--;
                        }

                        var emptyEntry = new DatEntry(0, current, count);
                        Index.Free(emptyEntry);
                    }
                }
                currentLength = FileStream.Position;
                if (FileStream.Length != currentLength)
                {
                    FileStream.SetLength(currentLength);
                }

                Trace.TraceInformation(string.Format("Index of '{0}' rebuilt with {1} entries and {2} free positions.", this, Index.Count, Index.FreeItemCount));
            }
            catch (Exception ex)
            {
                Trace.TraceError("Error while rebuilding index of '{0}'\n{1}", this, ex);
                throw new DataException(string.Format("Database corrupt!"), ex);
            }
        }

        internal Row ReadRow(long id)
        {
            if (!Index.TryGet(id, out DatEntry entry))
            {
                throw new KeyNotFoundException();
            }

            FileStream.Position = entry.BucketPosition;
            Row row = ReadCurrentRow(new DataReader(FileStream), Version, Layout);
            if (row == null)
            {
                throw new DataException(string.Format("Database corrupt!"));
            }

            if (FileStream.Position == entry.BucketPosition + entry.BucketLength)
            {
                return row;
            }

            throw new DataException(string.Format("Index corrupt!"));
        }

        internal long WriteEntry(Row row, DatEntry entry = default(DatEntry))
        {
            var id = Layout.GetID(row);
            var data = GetData(Layout, row, Version);
            var minSize = data.Length + BitCoder32.GetByteCount7BitEncoded(data.Length + 10);

            // reuse old entry ?
            if (entry.ID != 0)
            {
                if (entry.BucketLength >= minSize)
                {
                    // write entry
                    FileStream.Position = entry.BucketPosition;
                    var writer = new DataWriter(FileStream);
                    BitCoder32.Write7BitEncoded(writer, entry.BucketLength);
                    writer.Write(data);
                    var fill = entry.BucketPosition + entry.BucketLength - FileStream.Position;
                    if (fill > 0)
                    {
                        FileStream.Write(new byte[fill], 0, (int)fill);
                    }
                    else if (fill < 0)
                    {
                        throw new DataException(string.Format("Database corrupt!"));
                    }

                    return id;
                }

                // no reuse -> release old
                Index.Free(entry);
            }

            // find free entry
            entry = Index.GetFree(id, minSize);
            if (entry.ID == 0)
            {
                // create new
                entry = new DatEntry(id, currentLength, minSize);
                currentLength += entry.BucketLength;
            }
            Index.Save(entry);
            {
                // write entry
                FileStream.Position = entry.BucketPosition;
                var writer = new DataWriter(FileStream);
                BitCoder32.Write7BitEncoded(writer, entry.BucketLength);
                writer.Write(data);
                var fill = entry.BucketPosition + entry.BucketLength - FileStream.Position;
                if (fill > 0)
                {
                    FileStream.Write(new byte[fill], 0, (int)fill);
                }
                else if (fill < 0)
                {
                    throw new DataException(string.Format("Database corrupt!"));
                }
            }
            IncreaseSequenceNumber();
            return id;
        }
        #endregion

        /// <summary>
        /// Version of the database file.
        /// </summary>
        public int Version { get; private set; }

        #region constructor / destructor

        /// <summary>
        /// Creates a new <see cref="DatTable{T}"/> instance.
        /// </summary>
        /// <param name="database">Database this table belongs to.</param>
        /// <param name="file">File to use.</param>
        public DatTable(DatDatabase database, string file)
            : this(database, file, GetLayout(file))
        {
        }

        /// <summary>
        /// Creates a new <see cref="DatTable{T}"/> instance.
        /// </summary>
        /// <param name="database">Database this table belongs to.</param>
        /// <param name="file">File to use.</param>
        /// <param name="layout">The layout of the table.</param>
        public DatTable(DatDatabase database, string file, RowLayout layout)
            : base(database, layout)
        {
            dataFile = file;
            indexFile = dataFile + ".idx";
            var createNew = !File.Exists(dataFile);

            if (createNew)
            {
                if (Layout == null)
                {
                    throw new ArgumentNullException("Layout", "Layout of table is unknown!");
                }

                Version = CurrentVersion;
                Recreate();
                return;
            }

            DateTime indexWriteTime = File.GetLastWriteTime(indexFile);
            DateTime fileWriteTime = File.GetLastWriteTime(dataFile);
            FileStream = File.Open(dataFile, FileMode.Open, FileAccess.ReadWrite, FileShare.None);

            var reader = new DataReader(FileStream);
            RowLayout fileLayout = LoadFieldDefinition(reader, out var version);
            Storage.CheckLayout(Layout, fileLayout);
            Version = version;
            start = FileStream.Position;

            var rebuildIndex = indexWriteTime + TimeSpan.FromSeconds(1) < fileWriteTime;
            if (!rebuildIndex)
            {
                Index = new DatIndex(indexFile);
                return;
            }

            RebuildIndex();
        }

        /// <summary>
        /// Frees all resources
        /// </summary>
        ~DatTable()
        {
            Dispose(false);
        }
        #endregion

        #region Table member

        /// <summary>Searches the table for rows with given field value combinations.</summary>
        /// <param name="search">The search to run.</param>
        /// <param name="resultOption">Options for the search and the result set.</param>
        /// <returns>Returns the ID of the row found or -1.</returns>
        public override IList<long> FindRows(Search search = default(Search), ResultOption resultOption = default(ResultOption))
        {
            if (search == null)
            {
                search = Search.None;
            }

            search.LoadLayout(Layout);

            if (resultOption != null)
            {
                var table = new MemoryTable(Layout);
                var found = FindFirst(r => search.Check(r), out Row row);
                while (found)
                {
                    table.Insert(row);
                    found = FindNext(r => search.Check(r), out row);
                }
                return MemoryTable.FindRows(Layout, null, table, search, resultOption, true);
            }
            else
            {
                var ids = new List<long>();
                var found = FindFirst(r => search.Check(r), out Row row);
                while (found)
                {
                    ids.Add(row.GetID(Layout.IDFieldIndex));
                    found = FindNext(r => search.Check(r), out row);
                }
                return ids;
            }
        }

        /// <summary>
        /// This function does a lookup on the ids of the table and returns the row with the n-th ID where n is the specified index.
        /// Note that indices may change on each update, insert, delete and sorting is not garanteed!.
        /// <param name="index">The index of the row to be fetched</param>
        /// </summary>
        /// <returns>Returns the row.</returns>
        public override Row GetRowAt(int index)
        {
            var id = this.Index.IDs.ElementAt(index);
            return GetRow(id);
        }

        /// <summary>
        /// Gets a row from the table.
        /// </summary>
        /// <param name="id">The ID of the row to be fetched.</param>
        /// <returns>Returns the row.</returns>
        public override Row GetRow(long id)
        {
            return ReadRow(id);
        }

        /// <summary>
        /// Checks a specified ID for existance.
        /// </summary>
        /// <param name="id">The dataset ID to look for.</param>
        /// <returns>Returns whether the dataset exists or not.</returns>
        public override bool Exist(long id)
        {
            return Index.TryGet(id, out DatEntry e);
        }

        /// <summary>
        /// Inserts a row to the table. If an ID &lt;= 0 is specified an automatically generated ID will be used to add the dataset.
        /// </summary>
        /// <param name="row">The row to insert. If an ID &lt;= 0 is specified an automatically generated ID will be used to add the dataset.</param>
        /// <returns>Returns the ID of the inserted dataset.</returns>
        public override long Insert(Row row)
        {
            var id = Layout.GetID(row);
            if (id <= 0)
            {
                id = Index.GetNextFreeID();
                row = row.SetID(Layout.IDFieldIndex, id);
                return WriteEntry(row);
            }
            else
            {
                if (Index.TryGet(id, out DatEntry entry))
                {
                    throw new ArgumentException("ID already present!");
                }

                return WriteEntry(row, entry);
            }
        }

        /// <summary>
        /// Replaces a row at the table. The ID has to be given. This inserts (if the row does not exist) or updates (if it exists) the row.
        /// </summary>
        /// <param name="row">The row to replace (valid ID needed).</param>
        public override void Replace(Row row)
        {
            var id = Layout.GetID(row);
            if (id <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(row), "Row ID is invalid!");
            }

            Index.TryGet(id, out DatEntry entry);
            WriteEntry(row, entry);
        }

        /// <summary>
        /// Updates a row to the table. The row must exist already!.
        /// </summary>
        /// <param name="row">The row to update.</param>
        public override void Update(Row row)
        {
            var id = Layout.GetID(row);
            if (id <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(row), "Row ID is invalid!");
            }

            if (!Index.TryGet(id, out DatEntry entry))
            {
                throw new KeyNotFoundException();
            }

            WriteEntry(row, entry);
        }

        /// <summary>
        /// Removes a row from the table.
        /// </summary>
        /// <param name="id">The dataset ID to remove.</param>
        public override void Delete(long id)
        {
            if (!Index.TryGet(id, out DatEntry entry))
            {
                throw new KeyNotFoundException();
            }

            FileStream.Position = entry.BucketPosition;
            FileStream.Write(new byte[entry.BucketLength], 0, entry.BucketLength);
            FileStream.Flush();
            Index.Free(entry);
        }

        /// <summary>Removes all rows from the table matching the specified search.</summary>
        /// <param name="search">The Search used to identify rows for removal.</param>
        /// <returns>Returns the number of dataset deleted.</returns>
        public override int TryDelete(Search search)
        {
            var ids = FindRows(search);
            Delete(ids);
            return ids.Count;
        }

        /// <summary>
        /// Clears all rows of the table.
        /// </summary>
        public override void Clear()
        {
            Recreate();
        }

        /// <summary>
        /// Gets the RowCount.
        /// </summary>
        public override long RowCount => Index == null ? 0 : Index.Count;

        /// <summary>
        /// Gets the next used ID at the table (positive values are valid, negative ones are invalid, 0 is not defined!).
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public override long GetNextUsedID(long id)
        {
            return Index.GetNextUsedID(id);
        }

        /// <summary>
        /// Gets the next free ID at the table.
        /// </summary>
        /// <returns></returns>
        public override long GetNextFreeID()
        {
            return Index.GetNextFreeID();
        }

        /// <summary>
        /// searches for a specified databaset (first occurence).
        /// </summary>
        /// <param name="match"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        public bool FindFirst(Predicate<Row> match, out Row result)
        {
            FileStream.Position = start;
            return FindNext(match, out result);
        }

        /// <summary>
        /// searches for a specified databaset (next occurence after <see cref="FindFirst"/>).
        /// </summary>
        /// <param name="match"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        public bool FindNext(Predicate<Row> match, out Row result)
        {
            if (match == null)
            {
                throw new ArgumentNullException("match");
            }

            while (FileStream.Position < FileStream.Length)
            {
                result = ReadCurrentRow(new DataReader(FileStream), Version, Layout);
                if (result != null)
                {
                    if (match(result))
                    {
                        return true;
                    }
                }
            }
            result = null;
            return false;
        }

        /// <summary>
        /// Gets an array containing all rows of the table.
        /// </summary>
        /// <returns></returns>
        public override IList<Row> GetRows()
        {
            var result = new List<Row>((int)RowCount);
            FileStream.Position = start;
            var reader = new DataReader(FileStream);
            while (FileStream.Position < FileStream.Length)
            {
                Row row = ReadCurrentRow(reader, Version, Layout);
                if (row != null)
                {
                    result.Add(row);
                }
            }
            return result;
        }

        /// <summary>Obtains the rows with the given ids.</summary>
        /// <param name="ids">IDs of the rows to fetch from the table.</param>
        /// <returns>Returns the rows.</returns>
        public override IList<Row> GetRows(IEnumerable<long> ids)
        {
            var rows = new List<Row>();
            foreach (var id in ids.AsSet())
            {
                rows.Add(GetRow(id));
            }
            return rows;
        }
        #endregion

        #region IDisposable Member

        /// <summary>
        /// Frees all used resources.
        /// </summary>
        /// <param name="disposing"></param>
        /*protected virtual*/
        void Dispose(bool disposing)
        {
            if (disposing)
            {
                FileStream?.Close();
                FileStream = null;
                Index?.Dispose();
                Index = null;
            }
        }

        /// <summary>
        /// Frees all used resources.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        #endregion
    }
}
