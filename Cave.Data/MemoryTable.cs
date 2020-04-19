using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Cave.Collections.Generic;

namespace Cave.Data
{
    /// <summary>
    /// Provides a table stored completely in memory.
    /// </summary>
    [DebuggerDisplay("{Name}")]
    public class MemoryTable : Table
    {
        #region fields

        /// <summary>Gets a value indicating whether this instance is readonly.</summary>
        bool isReadonly;

        /// <summary>The rows (id, row) dictionary.</summary>
        Dictionary<Identifier, object[]> rows = new Dictionary<Identifier, object[]>();

        /// <summary>The indices for fast lookups.</summary>
        FieldIndex[] indices;

        /// <summary>The memory table options.</summary>
        MemoryTableOptions memoryTableOptions;

        #endregion

        #region constructors

        /// <summary>
        /// Initializes a new instance of the <see cref="MemoryTable"/> class.
        /// </summary>
        protected MemoryTable()
        {
        }

        #endregion

        #region properties

        #region IsReadonly

        /// <summary>Gets or sets a value indicating whether this instance is readonly.</summary>
        /// <value><c>true</c> if this instance is readonly; otherwise, <c>false</c>.</value>
        /// <exception cref="ReadOnlyException">Table {0} is readonly!.</exception>
        /// <remarks>
        /// If the table is not readonly this can be set to readonly. Once set to readonly a reset is not possible.
        /// But you can recreate a writeable table by using a new <see cref="MemoryTable" /> and the <see cref="LoadTable(ITable, Search, ProgressCallback, object)" /> function.
        /// </remarks>
        public bool IsReadonly
        {
            get => isReadonly;
            set
            {
                if (isReadonly)
                {
                    throw new ReadOnlyException();
                }

                isReadonly = value;
            }
        }
        #endregion

        #region RowCount

        /// <inheritdoc/>
        public override long RowCount => rows.Count;

        #endregion

        #endregion

        #region functions

        #region public static Create()

        /// <summary>
        /// Initializes a new instance of the <see cref="MemoryTable"/> class.
        /// </summary>
        /// <param name="type">Table layout.</param>
        /// <returns>Returns a new <see cref="MemoryTable"/> instance.</returns>
        public static MemoryTable Create(Type type)
        {
            return Create(RowLayout.CreateTyped(type));
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MemoryTable"/> class.
        /// </summary>
        /// <param name="layout">Table layout.</param>
        /// <returns>Returns a new <see cref="MemoryTable"/> instance.</returns>
        public static MemoryTable Create(RowLayout layout)
        {
            return Create(MemoryDatabase.Default, layout);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MemoryTable"/> class.
        /// </summary>
        /// <param name="database">The database the.</param>
        /// <param name="layout">The layout of the table.</param>
        /// <param name="options">The options.</param>
        /// <returns>Returns a new <see cref="MemoryTable"/> instance.</returns>
        public static MemoryTable Create(IDatabase database, RowLayout layout, MemoryTableOptions options = 0)
        {
            var result = new MemoryTable
            {
                memoryTableOptions = options,
            };
            result.Connect(database, default, layout);
            return result;
        }

        #endregion

        #region public static CreateIndex()

        /// <summary>Creates a memory index for the specified layout.</summary>
        /// <param name="layout">The layout.</param>
        /// <param name="options">The options.</param>
        /// <returns>Returns a field index array or null.</returns>
        public static FieldIndex[] CreateIndex(RowLayout layout, MemoryTableOptions options = 0)
        {
            if ((options & MemoryTableOptions.DisableIndex) == 0)
            {
                var indexCount = 0;
                var indices = new FieldIndex[layout.FieldCount];
                for (var i = 0; i < indices.Length; i++)
                {
                    var field = layout[i];
                    if ((field.Flags & (FieldFlags.Index | FieldFlags.ID)) != 0)
                    {
                        indices[i] = new FieldIndex();
                        indexCount++;
                    }
                }
                if (indexCount > 0)
                {
                    return indices;
                }
            }
            return null;
        }

        #endregion

        #region Initialize()

        /// <inheritdoc/>
        public override void Connect(IDatabase database, TableFlags flags, RowLayout layout)
        {
            base.Connect(database, flags, layout);
            indices = CreateIndex(Layout, memoryTableOptions);
        }

        #endregion

        #region Load Table

        /// <summary>Replaces all data present with the data at the given table.</summary>
        /// <param name="table">The table to load.</param>
        /// <param name="search">The search.</param>
        /// <param name="callback">The callback.</param>
        /// <param name="userItem">The user item.</param>
        /// <exception cref="ArgumentNullException">Table.</exception>
        /// <exception cref="ReadOnlyException">Table {0} is readonly!.</exception>
        public void LoadTable(ITable table, Search search = null, ProgressCallback callback = null, object userItem = null)
        {
            Trace.TraceInformation("Loading table {0}", table);
            if (table == null)
            {
                throw new ArgumentNullException("Table");
            }

            Storage.CheckLayout(Layout, table.Layout);
            if (search == null)
            {
                search = Search.None;
            }

            Clear();
            int offset = 0;
            var rowCount = table.RowCount;

            if (rowCount == 0)
            {
                return;
            }

            while (true)
            {
                var rows = table.GetRows(search, ResultOption.Limit(Storage.TransactionRowCount) + ResultOption.Offset(offset));

                var nextOffset = offset + rows.Count;
                Insert(rows);
                if (callback != null)
                {
                    var e = new ProgressEventArgs(userItem, RowCount, rows.Count, rowCount, true);
                    callback.Invoke(this, e);
                    if (e.Break)
                    {
                        break;
                    }
                }
                else
                {
                    var progress = RowCount * 100f / rowCount;
                    Trace.TraceInformation(string.Format("Loaded {0} rows from table {1} starting with offset {2} to {3} ({4:N}%)", rows.Count, table, offset, nextOffset, progress));
                }
                offset = nextOffset;

                if (rows.Count < Storage.TransactionRowCount)
                {
                    if (rowCount != RowCount)
                    {
                        throw new Exception();
                    }
                    break;
                }
            }
        }

        #endregion

        #region SetRows

        /// <summary>Replaces the whole data at the table with the specified one.</summary>
        /// <param name="rows">The rows to insert.</param>
        /// <exception cref="ReadOnlyException">Table {0} is readonly!.</exception>
        /// <exception cref="ArgumentNullException">rows.</exception>
        public void SetRows(IEnumerable<Row> rows)
        {
            if (isReadonly)
            {
                throw new ReadOnlyException(string.Format("Table {0} is readonly!", this));
            }

            if (rows == null)
            {
                throw new ArgumentNullException(nameof(rows));
            }

            Clear();
            foreach (Row row in rows)
            {
                Insert(row);
            }
        }

        #endregion

        #region GetRowAt

        /// <summary>
        /// This function does a lookup on the ids of the table and returns the row with the n-th ID where n is the given index.
        /// Note that indices may change on each update, insert, delete and sorting is not garanteed!.
        /// </summary>
        /// <param name="index">The index of the row to be fetched.</param>
        /// <returns>Returns the row.</returns>
        public override Row GetRowAt(int index) => new Row(Layout, rows.Values.ElementAt(index), true);

        #endregion

        #region Exist

        /// <inheritdoc/>
        public override bool Exist(Search search)
        {
            search.LoadLayout(Layout);
            return rows.Values.Any(row => search.Check(row));
        }

        /// <inheritdoc/>
        public override bool Exist(Row row) => Exist(new Identifier(row, Layout));

        #endregion

        #region Replace

        /// <inheritdoc/>
        public override void Replace(Row row)
        {
            var id = new Identifier(row, Layout);
            if (Exist(id))
            {
                Update(row, id);
            }
            else
            {
                Insert(row, id);
            }
        }

        /// <inheritdoc/>
        public override void Replace(IEnumerable<Row> rows)
        {
            foreach (var row in rows)
            {
                Replace(row);
            }
        }

        #endregion

        #region Insert

        /// <inheritdoc/>
        public override Row Insert(Row row)
        {
            var id = new Identifier(row, Layout);
            return Insert(row, id);
        }

        /// <inheritdoc/>
        public override void Insert(IEnumerable<Row> rows)
        {
            foreach (var row in rows)
            {
                Insert(row);
            }
        }

        #endregion

        #region Update

        /// <inheritdoc/>
        public override void Update(Row row)
        {
            var id = new Identifier(row, Layout);
            Update(row, id);
        }

        /// <inheritdoc/>
        public override void Update(IEnumerable<Row> rows)
        {
            foreach (var row in rows)
            {
                Update(row);
            }
        }

        #endregion

        #region Delete

        /// <inheritdoc/>
        public override void Delete(Row row)
        {
            if (isReadonly)
            {
                throw new ReadOnlyException(string.Format("Table {0} is readonly!", this));
            }

            if (Database.Storage.LogVerboseMessages)
            {
                Trace.TraceInformation("Delete {0} at {1}", row, this);
            }

            var id = new Identifier(row, Layout);
            if (!rows.Remove(id))
            {
                throw new ArgumentException(string.Format("Row {0} not found at table {1}!", row, Name));
            }

            if (indices != null)
            {
                for (var i = 0; i < Layout.FieldCount; i++)
                {
                    FieldIndex index = indices[i];
                    if (index == null)
                    {
                        continue;
                    }

                    index.Delete(row.Values, i);
#if DEBUG
                    if (index.Count != RowCount)
                    {
                        throw new Exception(string.Format("BFDE: Operation: {0}, index.Count {1}, RowCount {2}", "Delete", index.Count, RowCount));
                    }
#endif
                }
            }
            IncreaseSequenceNumber();
        }

        /// <inheritdoc/>
        public override void Delete(IEnumerable<Row> rows)
        {
            foreach (var row in rows)
            {
                Delete(row);
            }
        }

        /// <summary>Removes all rows from the table matching the specified search.</summary>
        /// <param name="search">The Search used to identify rows for removal.</param>
        /// <returns>Returns the number of dataset deleted.</returns>
        public override int TryDelete(Search search)
        {
            if (search == null)
            {
                search = Search.None;
            }

            var rows = search.Scan(null, Layout, indices, this);
            var count = 0;
            foreach (var row in rows)
            {
                Delete(row);
                count++;
            }
            return count;
        }

        #endregion

        #region Clear

        /// <inheritdoc/>
        public override void Clear()
        {
            if (isReadonly)
            {
                throw new ReadOnlyException(string.Format("Table {0} is readonly!", this));
            }

            if (Database.Storage.LogVerboseMessages)
            {
                Trace.TraceInformation("Clear {0}", this);
            }

            rows = new Dictionary<Identifier, object[]>();
            indices = CreateIndex(Layout, memoryTableOptions);
            IncreaseSequenceNumber();
        }

        #endregion

        #region Count

        /// <inheritdoc/>
        public override long Count(Search search = null, ResultOption resultOption = null) => GetRows(search, resultOption, false).Count;

        #endregion

        #region GetRows()

        /// <inheritdoc/>
        public override IList<Row> GetRows() => rows.Values.Select(r => new Row(Layout, r, true)).ToList();

        #endregion

        #region GetRows

        /// <inheritdoc/>
        public override IList<Row> GetRows(Search search = null, ResultOption resultOption = null) => GetRows(search, resultOption, false);

        #endregion

        #region GetRow

        /// <inheritdoc/>
        public override Row GetRow(Search search = null, ResultOption resultOption = null) => GetRows(search, resultOption).Single();

        #endregion

        #region additional functionality

        /// <summary>
        /// Checks an identififer for existance.
        /// </summary>
        /// <param name="id">The identifier.</param>
        /// <returns>True if the id is found at the table, false otherwise.</returns>
        public bool Exist(Identifier id) => rows.ContainsKey(id);

        /// <summary>
        /// Gets a row from the table.
        /// </summary>
        /// <param name="id">The identifier of the row to be fetched.</param>
        /// <returns>Returns the row.</returns>
        public Row GetRow(Identifier id)
        {
            return new Row(Layout, rows[id], true);
        }

        /// <summary>Obtains the rows with the given ids.</summary>
        /// <param name="ids">Identifiers of the rows to fetch from the table.</param>
        /// <returns>Returns the rows.</returns>
        public IList<Row> GetRows(IEnumerable<Identifier> ids)
        {
            var result = new List<Row>();
            foreach (var id in ids)
            {
                result.Add(new Row(Layout, rows[id], true));
            }
            return result;
        }

        /// <summary>Searches the table for rows with given field value combinations.</summary>
        /// <param name="search">The search.</param>
        /// <param name="resultOption">The result option.</param>
        /// <param name="skipSearch">if set to <c>true</c> [skip search].</param>
        /// <returns>A list of rows matching the specified criteria.</returns>
        /// <exception cref="ArgumentException">Field '{Parameter}' is not present!.</exception>
        /// <exception cref="InvalidOperationException">Cannot set two different limits or offsets!.</exception>
        /// <exception cref="NotSupportedException">Option {option.Mode} is not supported!.</exception>
        IList<Row> GetRows(Search search, ResultOption resultOption, bool skipSearch = false)
        {
            if (resultOption == null)
            {
                resultOption = ResultOption.None;
            }

            if (search == null || search.Mode == SearchMode.None)
            {
                skipSearch = true;
            }
            else
            {
                search.LoadLayout(Layout);
            }

            var grouping = new List<int>();
            var sorting = new Set<int, ResultOptionMode>();
            var limit = -1;
            var offset = -1;

            if (resultOption != null)
            {
                foreach (ResultOption option in resultOption.ToArray())
                {
                    switch (option.Mode)
                    {
                        case ResultOptionMode.None: break;

                        case ResultOptionMode.Group:
                        {
                            var fieldIndex = Layout.GetFieldIndex(option.Parameter, true);
                            grouping.Add(fieldIndex);
                        }
                        break;

                        case ResultOptionMode.SortAsc:
                        case ResultOptionMode.SortDesc:
                        {
                            var fieldIndex = Layout.GetFieldIndex(option.Parameter, true);
                            sorting.Add(fieldIndex, option.Mode);
                        }
                        break;

                        case ResultOptionMode.Limit:
                        {
                            if (limit >= 0)
                            {
                                throw new InvalidOperationException("Cannot set two different limits!");
                            }

                            limit = Math.Abs(int.Parse(option.Parameter));
                            break;
                        }

                        case ResultOptionMode.Offset:
                        {
                            if (offset >= 0)
                            {
                                throw new InvalidOperationException("Cannot set two different offsets!");
                            }

                            offset = Math.Abs(int.Parse(option.Parameter));
                            break;
                        }

                        default: throw new NotSupportedException($"Option {option.Mode} is not supported!");
                    }
                }
            }
            IEnumerable<Row> result;
            if (skipSearch)
            {
                result = rows.Values.Select(r => new Row(Layout, r, true));
            }
            else
            {
                // simple ungrouped search
                result = search.Scan(null, Layout, indices, this);
            }

            // group by ?
            if (grouping.Count > 0)
            {
                var groupedRows = new List<Row>();
                var groupedKeys = new Set<Identifier>();
                foreach (var row in result)
                {
                    var key = new Identifier(row, grouping);
                    if (groupedKeys.Include(key))
                    {
                        groupedRows.Add(row);
                    }
                }
                result = groupedRows;
            }

            List<Row> sorted = null;
            if (sorting.Count > 0)
            {
                sorted = result.ToList();
                if (sorting.Count > 1)
                {
                    sorting.Reverse();
                }

                foreach (ItemPair<int, ResultOptionMode> sort in sorting)
                {
                    var sorter = new TableSorter(Layout[sort.A], sort.B);
                    sorted.Sort(sorter);
                }
            }
            else
            {
                // no sort
                sorted = result.AsList();
            }

            if (offset > -1 || limit > -1)
            {
                if (offset < 0)
                {
                    offset = 0;
                }

                if (offset >= sorted.Count)
                {
                    return new Row[0];
                }

                if (limit < 0)
                {
                    return sorted.SubRange(offset).ToList();
                }

                limit = Math.Min(limit, sorted.Count - offset);
                return limit <= 0 ? new Row[0] : (IList<Row>)sorted.GetRange(offset, limit);
            }
            return sorted;
        }
        #endregion

        #region private functions

        Row Insert(Row row, Identifier id)
        {
            if (isReadonly)
            {
                throw new ReadOnlyException(string.Format("Table {0} is readonly!", this));
            }

            if (Database.Storage.LogVerboseMessages)
            {
                Trace.TraceInformation("Insert {0} at {1}", row, this);
            }

            var autoinc = Layout.Identifier.Where(f => f.Flags.HasFlag(FieldFlags.AutoIncrement));
            if (autoinc.Any())
            {
                GetAutoIncrement(ref row, ref id, autoinc);
            }

            var values = row.CopyValues();
            rows.Add(id, values);
            if (indices != null)
            {
                for (var i = 0; i < Layout.FieldCount; i++)
                {
                    FieldIndex index = indices[i];
                    if (index == null)
                    {
                        continue;
                    }

                    index.Add(values, i);
#if DEBUG
                    if (index.Count != RowCount)
                    {
                        throw new Exception(string.Format("BFDE: Operation: {0}, index.Count {1}, RowCount {2}", "Add", index.Count, RowCount));
                    }
#endif
                }
            }
            IncreaseSequenceNumber();
            return row;
        }

        void GetAutoIncrement(ref Row row, ref Identifier id, IEnumerable<IFieldProperties> autoinc)
        {
            var values = row.CopyValues();
            foreach (var field in autoinc)
            {
                var value = values[field.Index];
                switch (field.DataType)
                {
                    default:
                        throw new NotSupportedException($"Autoincrement field {field} not supported!");
                    case DataType.DateTime:
                        if (value == null || (DateTime)value == default(DateTime))
                        {
                            value = DateTime.UtcNow;
                        }
                        break;
                    case DataType.User:
                        if (field.ValueType == typeof(Guid))
                        {
                            if (value == null || (Guid)value == default(Guid))
                            {
                                value = Guid.NewGuid();
                            }
                        }
                        else
                        {
                            throw new NotSupportedException($"Autoincrement field {field} not supported!");
                        }
                        break;
                    case DataType.Int8:
                        if (value == null || (sbyte)value == default(sbyte))
                        {
                            value = (Maximum<sbyte>(field.Name) ?? 0) + 1;
                        }
                        break;
                    case DataType.UInt8:
                        if (value == null || (byte)value == default(byte))
                        {
                            value = (Maximum<byte>(field.Name) ?? 0) + 1;
                        }
                        break;
                    case DataType.Int16:
                        if (value == null || (short)value == default(short))
                        {
                            value = (Maximum<short>(field.Name) ?? 0) + 1;
                        }
                        break;
                    case DataType.UInt16:
                        if (value == null || (ushort)value == default(ushort))
                        {
                            value = (Maximum<ushort>(field.Name) ?? 0) + 1;
                        }
                        break;
                    case DataType.Int32:
                        if (value == null || (int)value == default(int))
                        {
                            value = (Maximum<int>(field.Name) ?? 0) + 1;
                        }
                        break;
                    case DataType.UInt32:
                        if (value == null || (uint)value == default(uint))
                        {
                            value = (Maximum<uint>(field.Name) ?? 0) + 1;
                        }
                        break;
                    case DataType.Int64:
                        if (value == null || (long)value == default(long))
                        {
                            value = (Maximum<long>(field.Name) ?? 0) + 1;
                        }
                        break;
                    case DataType.UInt64:
                        if (value == null || (ulong)value == default(ulong))
                        {
                            value = (Maximum<ulong>(field.Name) ?? 0) + 1;
                        }
                        break;
                }
                values[field.Index] = value;
            }

            var newRow = new Row(Layout, values, false);
            var newId = new Identifier(newRow, Layout);
            if (Exist(newId))
            {
                throw new InvalidDataException("Could not create autoincrement identifier!");
            }
            id = newId;
            row = newRow;
        }

        void Update(Row row, Identifier id)
        {
            object[] values = row.CopyValues();
            if (isReadonly)
            {
                throw new ReadOnlyException(string.Format("Table {0} is readonly!", this));
            }

            if (Database.Storage.LogVerboseMessages)
            {
                Trace.TraceInformation("Update {0} ID {1} at {2}", values, id, this);
            }

            if (!rows.TryGetValue(id, out object[] oldValues))
            {
                throw new KeyNotFoundException("ID not present!");
            }

            rows[id] = values;
            if (indices != null)
            {
                for (var i = 0; i < Layout.FieldCount; i++)
                {
                    FieldIndex index = indices[i];
                    if (index == null)
                    {
                        continue;
                    }

                    index.Replace(oldValues, values, i);
#if DEBUG
                    if (index.Count != RowCount)
                    {
                        throw new Exception(string.Format("BFDE: Operation: {0}, index.Count {1}, RowCount {2}", "Replace", index.Count, RowCount));
                    }
#endif
                }
            }
            IncreaseSequenceNumber();
        }

        #endregion

        #endregion
    }
}
