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

using Cave.Collections.Generic;
using Cave.IO;
using Cave.Text;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;

namespace Cave.Data
{
    /// <summary>
    /// Provides a binary dat file database table
    /// </summary>
    public class DatTable : Table, IDisposable
    {
        /// <summary>The current version</summary>
        public const int CurrentVersion = 3;

        #region private variables
        internal Stream m_FileStream;
        internal DatIndex m_Index;
        long m_CurrentLength = 0;
        string m_DataFile;
        string m_IndexFile;
        long m_Start;
        #endregion

        #region static internal functionality

        static internal RowLayout LoadFieldDefinition(DataReader reader, out int version)
        {
            DateTimeKind dateTimeKind = DateTimeKind.Unspecified;
            DateTimeType dateTimeType = DateTimeType.Undefined;
            StringEncoding stringEncoding = StringEncoding.UTF8;
            if (reader.ReadString(8) != "DatTable")
            {
                throw new FormatException();
            }

            version = reader.Read7BitEncodedInt32();
            if ((version < 1) || (version > 3))
            {
                throw new InvalidDataException(string.Format("Unknown Table version!"));
            }
            //read name and create layout
            string layoutName = reader.ReadString();
            List<FieldProperties> fields = new List<FieldProperties>();
            int fieldCount = reader.Read7BitEncodedInt32();
            for (int i = 0; i < fieldCount; i++)
            {
                string fieldName = reader.ReadString();
                DataType dataType = (DataType)reader.Read7BitEncodedInt32();
                FieldFlags fieldFlags = (FieldFlags)reader.Read7BitEncodedInt32();

                switch (dataType)
                {
                    case DataType.User:
                    case DataType.String:
                        if (version > 2)
                        {
                            stringEncoding = (StringEncoding)reader.Read7BitEncodedInt32();
                        }
                        else
                        {
                            stringEncoding = StringEncoding.UTF8;
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
                    string typeName = reader.ReadString();
                    valueType = AppDom.FindType(typeName, AppDom.LoadMode.NoException);
                    if (valueType == null)
                    {
                        throw new ArgumentException(string.Format("Could not load .NET type {0} for DataType {1} at table {2}", typeName, dataType, layoutName));
                    }
                }

                DataType databaseDataType = dataType;
                if (dataType == DataType.Enum)
                {
                    databaseDataType = DataType.Int64;
                }

                if (dataType == DataType.User)
                {
                    databaseDataType = DataType.String;
                }

                FieldProperties properties = new FieldProperties(layoutName, fieldFlags, dataType, valueType, 0, fieldName, databaseDataType, dateTimeType, dateTimeKind, stringEncoding, fieldName, null, null, null);
                fields.Add(properties);
            }
            return RowLayout.CreateUntyped(layoutName, fields.ToArray());
        }

        static internal void WriteFieldDefinition(DataWriter writer, RowLayout layout, int version)
        {
            if (version < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(version));
            }

            if (version > 3)
            {
                throw new NotSupportedException("Version not supported!");
            }

            try
            {
                writer.Write("DatTable");
                writer.Write7BitEncoded32(version);
                writer.WritePrefixed(layout.Name);
                writer.Write7BitEncoded32(layout.FieldCount);
                for (int i = 0; i < layout.FieldCount; i++)
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
                            writer.Write7BitEncoded32((int)properties.DateTimeKind);
                            writer.Write7BitEncoded32((int)properties.DateTimeType);
                            break;
                    }
                    if ((properties.DataType & DataType.MaskRequireValueType) != 0)
                    {
                        string typeName = properties.ValueType.AssemblyQualifiedName;
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

        static internal byte[] GetData(RowLayout layout, Row row, int version)
        {
            if (version < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(version));
            }

            if (version > 3)
            {
                throw new NotSupportedException("Version not supported!");
            }

            using (MemoryStream buffer = new MemoryStream())
            {
                DataWriter writer = new DataWriter(buffer);
				for (int i = 0; i < layout.FieldCount; i++)
				{
					FieldProperties fieldProperties = layout.GetProperties(i);
					switch (fieldProperties.DataType)
					{
						case DataType.Binary:
						{
							byte[] data = (byte[])row.GetValue(i);
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
						case DataType.Bool: writer.Write((bool)row.GetValue(i)); break;
						case DataType.TimeSpan: writer.Write(((TimeSpan)row.GetValue(i)).Ticks); break;
						case DataType.DateTime: writer.Write(((DateTime)row.GetValue(i)).Ticks); break;
						case DataType.Single: writer.Write((float)row.GetValue(i)); break;
						case DataType.Double: writer.Write((double)row.GetValue(i)); break;
						case DataType.Int8: writer.Write((sbyte)row.GetValue(i)); break;
						case DataType.Int16: writer.Write((short)row.GetValue(i)); break;
						case DataType.UInt8: writer.Write((byte)row.GetValue(i)); break;
						case DataType.UInt16: writer.Write((ushort)row.GetValue(i)); break;
						case DataType.Int32: if (version == 1) { writer.Write((int)row.GetValue(i)); break; } writer.Write7BitEncoded32((int)row.GetValue(i)); break;
						case DataType.Int64: if (version == 1) { writer.Write((long)row.GetValue(i)); break; } writer.Write7BitEncoded64((long)row.GetValue(i)); break;
						case DataType.UInt32: if (version == 1) { writer.Write((uint)row.GetValue(i)); break; } writer.Write7BitEncoded32((uint)row.GetValue(i)); break;
						case DataType.UInt64: if (version == 1) { writer.Write((ulong)row.GetValue(i)); break; } writer.Write7BitEncoded64((ulong)row.GetValue(i)); break;
						case DataType.Char: writer.Write((char)row.GetValue(i)); break;
						case DataType.Decimal: writer.Write((decimal)row.GetValue(i)); break;

						case DataType.String:
						case DataType.User:
						{
							object data = row.GetValue(i);
							if (data == null)
							{
								writer.WritePrefixed((string)null);
							}
							else
							{
								string text = data.ToString();
								//check for invalid characters
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
							long value = Convert.ToInt64(row.GetValue(i));
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

        static internal Row ReadCurrentRow(DataReader reader, int version, RowLayout layout)
        {
            if (version < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(version));
            }

            if (version > 3)
            {
                throw new NotSupportedException("Version not supported!");
            }

            long dataStart = reader.BaseStream.Position;
            int dataSize = reader.Read7BitEncodedInt32();

            if (dataSize == 0)
            {
                return null;
            }

            object[] row = new object[layout.FieldCount];
            for (int i = 0; i < layout.FieldCount; i++)
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
                            int size = reader.ReadInt32();
                            row[i] = reader.ReadBytes(size);
                        }
                        break;

                    case DataType.Bool: row[i] = reader.ReadBool(); break;
                    case DataType.DateTime: row[i] = new DateTime(reader.ReadInt64(), field.DateTimeKind); break;
                    case DataType.TimeSpan: row[i] = new TimeSpan(reader.ReadInt64()); break;
                    case DataType.Int8: row[i] = reader.ReadInt8(); break;
                    case DataType.Int16: row[i] = reader.ReadInt16(); break;
                    case DataType.Int32: if (version == 1) { row[i] = reader.ReadInt32(); break; } row[i] = reader.Read7BitEncodedInt32(); break;
                    case DataType.Int64: if (version == 1) { row[i] = reader.ReadInt64(); break; } row[i] = reader.Read7BitEncodedInt64(); break;
                    case DataType.UInt32: if (version == 1) { row[i] = reader.ReadUInt32(); break; } row[i] = reader.Read7BitEncodedUInt32(); break;
                    case DataType.UInt64: if (version == 1) { row[i] = reader.ReadUInt64(); break; } row[i] = reader.Read7BitEncodedUInt64(); break;
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
            long skip = (dataStart + dataSize) - reader.BaseStream.Position;
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
        #endregion

        #region internal implementation
        internal void Recreate()
        {
            m_Index?.Dispose();
            if (m_FileStream != null)
            {
                m_FileStream.Close();
            }

            m_FileStream = File.Open(m_DataFile, FileMode.Create, FileAccess.ReadWrite, FileShare.Read);
            WriteFieldDefinition(new DataWriter(m_FileStream), Layout, Version);
            m_CurrentLength = m_FileStream.Position;
			File.Delete(m_IndexFile);
            m_Index = new DatIndex(m_IndexFile);
        }

        internal void RebuildIndex()
        {
            m_FileStream.Position = m_Start;
            m_Index = new DatIndex(m_IndexFile);
            Trace.TraceWarning(string.Format("Rebuilding index of '{0}' after unclean shutdown...", this));
            DataReader reader = new DataReader(m_FileStream);
            try
            {
                while (m_FileStream.Position < m_FileStream.Length)
                {
                    long current = m_FileStream.Position;
                    Row row = ReadCurrentRow(reader, Version, Layout);
                    if (row != null)
                    {
                        DatEntry entry = new DatEntry(Layout.GetID(row), current, (int)(m_FileStream.Position - current));
                        m_Index.Save(entry);
                    }
                    else
                    {
                        int count = 1;
                        while (m_FileStream.ReadByte() == 0)
                        {
                            count++;
                        }
                        if (m_FileStream.Position < m_FileStream.Length)
                        {
                            m_FileStream.Position--;
                        }

                        DatEntry emptyEntry = new DatEntry(0, current, count);
                        m_Index.Free(emptyEntry);
                    }
                }
                m_CurrentLength = m_FileStream.Position;
                if (m_FileStream.Length != m_CurrentLength)
                {
                    m_FileStream.SetLength(m_CurrentLength);
                }

                Trace.TraceInformation(string.Format("Index of '{0}' rebuilt with {1} entries and {2} free positions.", this, m_Index.Count, m_Index.FreeItemCount));
            }
            catch (Exception ex)
            {
                Trace.TraceError("Error while rebuilding index of '{0}'\n{1}", this, ex);
                throw new DataException(string.Format("Database corrupt!"), ex);
            }
        }

        internal Row ReadRow(long id)
        {
            if (!m_Index.TryGet(id, out DatEntry entry))
            {
                throw new KeyNotFoundException();
            }

            m_FileStream.Position = entry.BucketPosition;
            Row row = ReadCurrentRow(new DataReader(m_FileStream), Version, Layout);
            if (row == null)
            {
                throw new DataException(string.Format("Database corrupt!"));
            }

            if (m_FileStream.Position == entry.BucketPosition + entry.BucketLength)
            {
                return row;
            }

            throw new DataException(string.Format("Index corrupt!"));
        }

        internal long WriteEntry(Row row, DatEntry entry = default(DatEntry))
        {
            long id = Layout.GetID(row);
            byte[] data = GetData(Layout, row, Version);
            int minSize = data.Length + BitCoder32.GetByteCount7BitEncoded(data.Length + 10);

            //reuse old entry ?
            if (entry.ID != 0)
            {
                if (entry.BucketLength >= minSize)
                {
                    //write entry
                    m_FileStream.Position = entry.BucketPosition;
                    DataWriter writer = new DataWriter(m_FileStream);
                    BitCoder32.Write7BitEncoded(writer, entry.BucketLength);
                    writer.Write(data);
                    long fill = (entry.BucketPosition + entry.BucketLength) - m_FileStream.Position;
                    if (fill > 0)
                    {
                        m_FileStream.Write(new byte[fill], 0, (int)fill);
                    }
                    else if (fill < 0)
                    {
                        throw new DataException(string.Format("Database corrupt!"));
                    }

                    return id;
                }
                //no reuse -> release old
                m_Index.Free(entry);
            }

            //find free entry
            entry = m_Index.GetFree(id, minSize);
            if (entry.ID == 0)
            {
                //create new
                entry = new DatEntry(id, m_CurrentLength, minSize);
                m_CurrentLength += entry.BucketLength;
            }
            m_Index.Save(entry);
            {
                //write entry
                m_FileStream.Position = entry.BucketPosition;
                DataWriter writer = new DataWriter(m_FileStream);
                BitCoder32.Write7BitEncoded(writer, entry.BucketLength);
                writer.Write(data);
                long fill = (entry.BucketPosition + entry.BucketLength) - m_FileStream.Position;
                if (fill > 0)
                {
                    m_FileStream.Write(new byte[fill], 0, (int)fill);
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
        /// Version of the database file
        /// </summary>
        public int Version { get; private set; }

        #region constructor / destructor

        static RowLayout GetLayout(string file)
        {
            using (Stream stream = File.Open(file, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
            {
                DataReader reader = new DataReader(stream);
                return LoadFieldDefinition(reader, out int version);
            }
        }

        /// <summary>
        /// Creates a new <see cref="DatTable{T}"/> instance
        /// </summary>
        /// <param name="database">Database this table belongs to</param>
        /// <param name="file">File to use</param>
        public DatTable(DatDatabase database, string file)
            : this(database, file, GetLayout(file))
        {
        }

        /// <summary>
        /// Creates a new <see cref="DatTable{T}"/> instance
        /// </summary>
        /// <param name="database">Database this table belongs to</param>
        /// <param name="file">File to use</param>
        /// <param name="layout">The layout of the table</param>
        public DatTable(DatDatabase database, string file, RowLayout layout)
            : base(database, layout)
        {
            m_DataFile = file;
            m_IndexFile = m_DataFile + ".idx";
            bool createNew = !File.Exists(m_DataFile);

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

            DateTime indexWriteTime = File.GetLastWriteTime(m_IndexFile);
            DateTime fileWriteTime = File.GetLastWriteTime(m_DataFile);
            m_FileStream = File.Open(m_DataFile, FileMode.Open, FileAccess.ReadWrite, FileShare.None);

            DataReader reader = new DataReader(m_FileStream);
            RowLayout fileLayout = LoadFieldDefinition(reader, out int version);
            Storage.CheckLayout(Layout, fileLayout);
            Version = version;
            m_Start = m_FileStream.Position;

            bool rebuildIndex = (indexWriteTime + TimeSpan.FromSeconds(1) < fileWriteTime);
            if (!rebuildIndex)
            {
                m_Index = new DatIndex(m_IndexFile);
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
        /// <param name="search">The search to run</param>
        /// <param name="resultOption">Options for the search and the result set</param>
        /// <returns>Returns the ID of the row found or -1</returns>
        public override List<long> FindRows(Search search = default(Search), ResultOption resultOption = default(ResultOption))
        {
            if (search == null)
            {
                search = Search.None;
            }

            search.LoadLayout(Layout);
            SortedDictionary<long, Row> rows = new SortedDictionary<long, Row>();
            bool found = FindFirst(r => search.Check(r), out Row row);
            while (found)
            {
                rows.Add(row.GetID(Layout.IDFieldIndex), row);
                found = FindNext(r => search.Check(r), out row);
            }
            return MemoryTable.FindRows(Layout, null, rows, search, resultOption, true);
        }

        /// <summary>
        /// This function does a lookup on the ids of the table and returns the row with the n-th ID where n is the specified index.
        /// Note that indices may change on each update, insert, delete and sorting is not garanteed!
        /// <param name="index">The index of the row to be fetched</param>
        /// </summary>
        /// <returns>Returns the row</returns>
        public override Row GetRowAt(int index)
        {
            long id = m_Index.IDs.ElementAt(index);
            return GetRow(id);
        }

        /// <summary>
        /// Obtains a row from the table
        /// </summary>
        /// <param name="id">The ID of the row to be fetched</param>
        /// <returns>Returns the row</returns>
        public override Row GetRow(long id)
        {
            return ReadRow(id);
        }

        /// <summary>
        /// Checks a specified ID for existance
        /// </summary>
        /// <param name="id">The dataset ID to look for</param>
        /// <returns>Returns whether the dataset exists or not</returns>
        public override bool Exist(long id)
        {
            return m_Index.TryGet(id, out DatEntry e);
        }

        /// <summary>
        /// Inserts a row to the table. If an ID &lt;= 0 is specified an automatically generated ID will be used to add the dataset.
        /// </summary>
        /// <param name="row">The row to insert. If an ID &lt;= 0 is specified an automatically generated ID will be used to add the dataset.</param>
        /// <returns>Returns the ID of the inserted dataset</returns>
        public override long Insert(Row row)
        {
            long id = Layout.GetID(row);
            if (id <= 0)
            {
                id = m_Index.GetNextFreeID();
                row = row.SetID(Layout.IDFieldIndex, id);
                return WriteEntry(row);
            }
            else
            {
                if (m_Index.TryGet(id, out DatEntry entry))
                {
                    throw new ArgumentException("ID already present!");
                }

                return WriteEntry(row, entry);
            }
        }

        /// <summary>
        /// Replaces a row at the table. The ID has to be given. This inserts (if the row does not exist) or updates (if it exists) the row.
        /// </summary>
        /// <param name="row">The row to replace (valid ID needed)</param>
        public override void Replace(Row row)
        {
            long id = Layout.GetID(row);
            if (id <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(row), "Row ID is invalid!");
            }

            m_Index.TryGet(id, out DatEntry entry);
            WriteEntry(row, entry);
        }

        /// <summary>
        /// Updates a row to the table. The row must exist already!
        /// </summary>
        /// <param name="row">The row to update</param>
        public override void Update(Row row)
        {
            long id = Layout.GetID(row);
            if (id <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(row), "Row ID is invalid!");
            }

            if (!m_Index.TryGet(id, out DatEntry entry))
            {
                throw new KeyNotFoundException();
            }

            WriteEntry(row, entry);
        }

        /// <summary>
        /// Removes a row from the table.
        /// </summary>
        /// <param name="id">The dataset ID to remove</param>
        public override void Delete(long id)
        {
            if (!m_Index.TryGet(id, out DatEntry entry))
            {
                throw new KeyNotFoundException();
            }

            m_FileStream.Position = entry.BucketPosition;
            m_FileStream.Write(new byte[entry.BucketLength], 0, entry.BucketLength);
            m_FileStream.Flush();
            m_Index.Free(entry);
        }

		/// <summary>Removes all rows from the table matching the specified search.</summary>
		/// <param name="search">The Search used to identify rows for removal</param>
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
        /// Obtains the RowCount
        /// </summary>
        public override long RowCount
        {
            get
            {
                return m_Index == null ? 0 : m_Index.Count;
            }
        }

        /// <summary>
        /// Obtains the next used ID at the table (positive values are valid, negative ones are invalid, 0 is not defined!)
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public override long GetNextUsedID(long id)
        {
            return m_Index.GetNextUsedID(id);
        }

        /// <summary>
        /// Obtains the next free ID at the table
        /// </summary>
        /// <returns></returns>
        public override long GetNextFreeID()
        {
            return m_Index.GetNextFreeID();
        }

        /// <summary>
        /// searches for a specified databaset (first occurence)
        /// </summary>
        /// <param name="match"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        public bool FindFirst(Predicate<Row> match, out Row result)
        {
            m_FileStream.Position = m_Start;
            return FindNext(match, out result);
        }

        /// <summary>
        /// searches for a specified databaset (next occurence after <see cref="FindFirst"/>)
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

            while (m_FileStream.Position < m_FileStream.Length)
            {
                result = ReadCurrentRow(new DataReader(m_FileStream), Version, Layout);
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
        /// Obtains an array containing all rows of the table
        /// </summary>
        /// <returns></returns>
        public override List<Row> GetRows()
        {
            List<Row> result = new List<Row>((int)RowCount);
            m_FileStream.Position = m_Start;
            DataReader reader  = new DataReader(m_FileStream);
            while (m_FileStream.Position < m_FileStream.Length)
            {
                Row row = ReadCurrentRow(reader, Version, Layout);
                if (row != null)
                {
                    result.Add(row);
                }
            }
            return result;
        }

        /// <summary>Obtains the rows with the given ids</summary>
        /// <param name="ids">IDs of the rows to fetch from the table</param>
        /// <returns>Returns the rows</returns>
        public override List<Row> GetRows(IEnumerable<long> ids)
        {
            List<Row> rows = new List<Row>();
            foreach (long id in ids.AsSet())
            {
                rows.Add(GetRow(id));
            }
            return rows;
        }
        #endregion

        #region IDisposable Member
        /// <summary>
        /// Frees all used resources
        /// </summary>
        /// <param name="disposing"></param>
        /*protected virtual*/
        void Dispose(bool disposing)
        {
            if (disposing)
            {
                m_FileStream?.Close();
                m_FileStream = null;
                m_Index?.Dispose();
                m_Index = null;
            }
        }

        /// <summary>
        /// Frees all used resources
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        #endregion
    }

    /// <summary>
    /// Provides a binary dat file database table
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class DatTable<T> : DatTable, ITable<T> where T : struct
    {
        #region constructor
        /// <summary>
        /// Creates a new <see cref="DatTable{T}"/> instance
        /// </summary>
        /// <param name="database">Database this table belongs to</param>
        /// <param name="file">Filename of the table file</param>
        /// <param name="layout">Layout and name of the table</param>
        public DatTable(DatDatabase database, RowLayout layout, string file)
            : base(database, file, layout)
        {
        }
        #endregion

        #region additional functionality

        /// <summary>
        /// searches for a specified databaset (first occurence)
        /// </summary>
        /// <param name="match"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        public bool FindFirst(Predicate<T> match, out T result)
        {
            m_FileStream.Position = 0;
            return FindNext(match, out result);
        }

        /// <summary>
        /// searches for a specified databaset (next occurence after <see cref="FindFirst"/>)
        /// </summary>
        /// <param name="match"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        public bool FindNext(Predicate<T> match, out T result)
        {
            if (match == null)
            {
                throw new ArgumentNullException("match");
            }

            while (m_FileStream.Position < m_FileStream.Length)
            {
                Row row = ReadCurrentRow(new DataReader(m_FileStream), Version, Layout);
                if (row != null)
                {
                    T value = row.GetStruct<T>(Layout);
                    if (match(value))
                    {
                        result = value;
                        return true;
                    }
                }
            }
            result = new T();
            return false;
        }

        #endregion

        #region ITable<T> Member

        #region implemented non virtual
        /// <summary>
        /// Obtains the row struct with the specified index.
        /// This allows a memorytable to be used as virtual list for listviews, ...
        /// Note that indices may change on each update, insert, delete and sorting is not garanteed!
        /// </summary>
        /// <param name="index">The rows index (0..RowCount-1)</param>
        /// <returns></returns>
        /// <exception cref="IndexOutOfRangeException"></exception>
        public T GetStructAt(int index)
        {
            return GetRowAt(index).GetStruct<T>(Layout);
        }

        /// <summary>
        /// Inserts rows into the table using a transaction. 
        /// </summary>
        /// <param name="rows">The rows to insert.</param>
        public void Insert(IEnumerable<T> rows)
        {
            if (rows == null)
            {
                throw new ArgumentNullException("Rows");
            }

            TransactionLog<T> l = new TransactionLog<T>();
            foreach (T r in rows) { l.AddInserted(r); }
            Commit(l);
        }

        /// <summary>
        /// Updates rows at the table. The rows must exist already!
        /// </summary>
        /// <param name="rows">The rows to update</param>
        public void Update(IEnumerable<T> rows)
        {
            if (rows == null)
            {
                throw new ArgumentNullException("Rows");
            }

            TransactionLog<T> l = new TransactionLog<T>();
            foreach (T r in rows) { l.AddUpdated(r); }
            Commit(l);
        }

        /// <summary>
        /// Replaces rows at the table. This inserts (if the row does not exist) or updates (if it exists) each row.
        /// </summary>
        /// <param name="rows">The rows to replace (valid ID needed)</param>
        public void Replace(IEnumerable<T> rows)
        {
            if (rows == null)
            {
                throw new ArgumentNullException("Rows");
            }

            TransactionLog<T> l = new TransactionLog<T>();
            foreach (T i in rows) { l.AddReplaced(i); }
            Commit(l);
        }

        /// <summary>Tries to get the (unique) row with the given fieldvalue.</summary>
        /// <param name="search">The search.</param>
        /// <param name="row">The row.</param>
        /// <returns>Returns true on success, false otherwise</returns>
        public bool TryGetStruct(Search search, out T row)
        {
            long id = FindRow(search);
            if (id > -1)
            {
                row = GetStruct(id);
                return true;
            }
            row = new T();
            return false;
        }

        #endregion

        #region implemented virtual
        /// <summary>
        /// Obtains a row from the table
        /// </summary>
        /// <param name="id">The ID of the row to be fetched</param>
        /// <returns>Returns the row</returns>
        public virtual T GetStruct(long id)
        {
            return GetRow(id).GetStruct<T>(Layout);
        }

        /// <summary>
        /// Searches the table for a single row with specified search.
        /// </summary>
        /// <param name="search">The search to run</param>
        /// <param name="resultOption">Options for the search and the result set</param>
        /// <returns>Returns the row found</returns>
        public virtual T GetStruct(Search search = default(Search), ResultOption resultOption = default(ResultOption))
        {
            long id = FindRow(search, resultOption);
            if (id <= 0)
            {
                throw new DataException(string.Format("Dataset could not be found!"));
            }

            return GetStruct(id);
        }

        /// <summary>
        /// Searches the table for rows with specified field value combinations.
        /// </summary>
        /// <param name="search">The search to run</param>
        /// <param name="resultOption">Options for the search and the result set</param>
        /// <returns>Returns the rows found</returns>
        public virtual List<T> GetStructs(Search search = default(Search), ResultOption resultOption = default(ResultOption))
        {
            return ToStructs<T>(Layout, GetRows(search, resultOption));
        }

        /// <summary>
        /// Obtains the rows with the specified ids
        /// </summary>
        /// <param name="ids">IDs of the rows to fetch from the table</param>
        /// <returns>Returns the rows</returns>
        public virtual List<T> GetStructs(IEnumerable<long> ids)
        {
            return ToStructs<T>(Layout, GetRows(ids));
        }

        /// <summary>
        /// Inserts a row to the table. If an ID <![CDATA[<=]]> 0 is specified an automatically generated ID will be used to add the dataset.
        /// </summary>
        /// <param name="row">The row to insert</param>
        /// <returns>Returns the ID of the inserted dataset</returns>
        public virtual long Insert(T row)
        {
            return Insert(Row.Create(Layout, row));
        }

        /// <summary>
        /// Updates a row to the table. The row must exist already!
        /// </summary>
        /// <param name="row">The row to update</param>
        public virtual void Update(T row)
        {
            Update(Row.Create(Layout, row));
        }

        /// <summary>
        /// Replaces a row at the table. The ID has to be given. This inserts (if the row does not exist) or updates (if it exists) the row.
        /// </summary>
        /// <param name="row">The row to replace (valid ID needed)</param>
        public virtual void Replace(T row)
        {
            Replace(Row.Create(Layout, row));
        }

        /// <summary>
        /// Checks whether a row is present unchanged at the database and removes it.
        /// (Use Delete(ID) to delete a DataSet without any checks)
        /// </summary>
        /// <param name="row"></param>
        public virtual void Delete(T row)
        {
            long id = Layout.GetID(row);
            T data = GetStruct(id);
            if (!data.Equals(row))
            {
                throw new DataException(string.Format("Row does not match row at database!"));
            }

            Delete(id);
        }

        /// <summary>
        /// Provides access to the row with the specified ID
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public virtual T this[long id]
        {
            get
            {
                return GetStruct(id);
            }
            set
            {
                long i = Layout.GetID(value);
                if (i != id)
                {
                    throw new ArgumentException(string.Format("ID mismatch!"));
                }

                Replace(value);
            }
        }

        /// <summary>
        /// Copies all rows to a specified array
        /// </summary>
        /// <param name="rowArray"></param>
        /// <param name="startIndex"></param>
        public void CopyTo(T[] rowArray, int startIndex)
        {
            GetStructs().CopyTo(rowArray, startIndex);
        }

        #endregion

        #endregion
    }
}
