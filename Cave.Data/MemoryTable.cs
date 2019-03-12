using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Cave.Collections.Generic;
using Cave.Compression.Tar;

namespace Cave.Data
{
    /// <summary>
    /// Provides a table stored completely in memory.
    /// </summary>
    [DebuggerDisplay("{Name}")]
    public class MemoryTable : Table, IMemoryTable
    {
        #region static class

        /// <summary>Searches the table for rows with given field value combinations.</summary>
        /// <param name="layout">The layout.</param>
        /// <param name="indices">The indices.</param>
        /// <param name="table">The table.</param>
        /// <param name="search">The search.</param>
        /// <param name="resultOption">The result option.</param>
        /// <param name="skipSearch">if set to <c>true</c> [skip search].</param>
        /// <returns></returns>
        /// <exception cref="ArgumentException">
        /// </exception>
        /// <exception cref="InvalidOperationException">
        /// </exception>
        /// <exception cref="NotSupportedException"></exception>
        internal static IList<long> FindRows(RowLayout layout, IFieldIndex[] indices, ITable table, Search search, ResultOption resultOption, bool skipSearch = false)
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
                search.LoadLayout(layout);
            }

            var grouping = new List<int>();
            var sorting = new Set<int, ResultOptionMode>();

            IEnumerable<long> sortedIDs = null;
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
                            var fieldIndex = layout.GetFieldIndex(option.Parameter);
                            if (fieldIndex < 0)
                            {
                                throw new ArgumentException(string.Format("Field '{0}' is not present!", resultOption.Parameter));
                            }

                            grouping.Add(fieldIndex);
                        }
                        break;

                        case ResultOptionMode.SortAsc:
                        case ResultOptionMode.SortDesc:
                        {
                            var fieldIndex = layout.GetFieldIndex(option.Parameter);
                            if (fieldIndex < 0)
                            {
                                throw new ArgumentException(string.Format("Field '{0}' is not present!", option.Parameter));
                            }

                            if (fieldIndex == layout.IDFieldIndex)
                            {
                                sorting.Add(layout.GetFieldIndex(option.Parameter), option.Mode);
                                if (sorting.Count == 1)
                                {
                                    sortedIDs = table.SortedIDs;
                                    if (option.Mode == ResultOptionMode.SortDesc)
                                    {
                                        sortedIDs = sortedIDs.Reverse();
                                    }
                                }
                                else
                                {
                                    sortedIDs = null;
                                }
                            }
                            else
                            {
                                sorting.Add(layout.GetFieldIndex(option.Parameter), option.Mode);
                                IFieldIndex index = indices?[fieldIndex];
                                if (index != null)
                                {
                                    if (sorting.Count == 1)
                                    {
                                        sortedIDs = index.SortedIDs;
                                        if (option.Mode == ResultOptionMode.SortDesc)
                                        {
                                            sortedIDs = sortedIDs.Reverse();
                                        }
                                    }
                                    else
                                    {
                                        sortedIDs = null;
                                    }
                                }
                                else
                                {
                                    sortedIDs = null;
#if DEBUG
                                    Debug.WriteLine(string.Format("Sorting of <yellow>{0}<default> requires slow result sort. Think about adding an index to field <yellow>{1}<default>!", resultOption, layout.GetProperties(fieldIndex)));
#endif
                                }
                            }
                        }
                        break;

                        case ResultOptionMode.Limit:
                        {
                            if (limit >= 0)
                            {
                                throw new InvalidOperationException(string.Format("Cannot set two different limits!"));
                            }

                            limit = Math.Abs(int.Parse(option.Parameter));
                            break;
                        }

                        case ResultOptionMode.Offset:
                        {
                            if (offset >= 0)
                            {
                                throw new InvalidOperationException(string.Format("Cannot set two different offsets!"));
                            }

                            offset = Math.Abs(int.Parse(option.Parameter));
                            break;
                        }

                        default: throw new NotSupportedException(string.Format("Option {0} is not supported!", option.Mode));
                    }
                }
            }

            IEnumerable<long> result;
            if (skipSearch)
            {
                result = sortedIDs ?? table.IDs;
            }
            else
            {
                // simple ungrouped search
                result = search.Scan(null, layout, indices, table);
            }

            // group by ?
            if (grouping.Count > 0)
            {
                var groupedIDs = new Set<long>();
                foreach (var groupField in grouping)
                {
                    var groupedValues = new Set<object>();
                    foreach (var id in result)
                    {
                        var val = table.GetRow(id).GetValue(groupField);
                        if (groupedValues.Contains(val))
                        {
                            continue;
                        }

                        groupedValues.Add(val);
                        groupedIDs.Include(id);
                    }
                }
                result = groupedIDs.ToList();
            }

            List<long> sorted = null;

            // Sort by presorted ids
            if (sortedIDs != null)
            {
                // no sort if we return only presorted ids
                if (skipSearch)
                {
                    sorted = result.AsList();
                }
                else
                {
                    IItemSet<long> resultIDs = result.AsSet();
                    sorted = new List<long>();
                    foreach (var id in sortedIDs)
                    {
                        if (resultIDs.Contains(id))
                        {
                            sorted.Add(id);
                        }
                    }
                }
            }

            // Manual sort
            else if (sorting.Count > 0)
            {
                sorted = result.ToList();
                if (sorting.Count > 1)
                {
                    sorting.Reverse();
                }

                foreach (ItemPair<int, ResultOptionMode> sort in sorting)
                {
                    var sorter = new TableSorter(table, sort.A, sort.B);
                    sorted.Sort(sorter);
                }
            }

            // no sort
            else
            {
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
                    return new List<long>();
                }

                if (limit < 0)
                {
                    return sorted.SubRange(offset).ToList();
                }

                limit = Math.Min(limit, sorted.Count - offset);
                return limit <= 0 ? new List<long>() : sorted.GetRange(offset, limit);
            }
            return sorted;
        }
        #endregion

        #region private variables

        /// <summary>Gets a value indicating whether this instance is readonly.</summary>
        bool isReadonly;

        /// <summary>provides the next free id.</summary>
        long nextFreeID = 1;

        /// <summary>The rows (id, row) dictionary.</summary>
        FakeSortedDictionary<long, Row> items;

        /// <summary>The indices for fast lookups.</summary>
        FieldIndex[] indices;

        /// <summary>The memory table options.</summary>
        MemoryTableOptions memoryTableOptions;

        #endregion

        #region constructors

        /// <summary>
        /// Creates an empty unbound memory table (within a new memory storage).
        /// This is used by temporary tables and query results.
        /// </summary>
        public MemoryTable(RowLayout layout)
            : this(MemoryDatabase.Default, layout)
        {
        }

        /// <summary>Creates a new empty MemoryTable for the specified database with the specified name.</summary>
        /// <param name="database">The database the.</param>
        /// <param name="layout">The layout of the table.</param>
        /// <param name="options">The options.</param>
        public MemoryTable(IDatabase database, RowLayout layout, MemoryTableOptions options = 0)
            : base(database, layout)
        {
            items = new FakeSortedDictionary<long, Row>();
            memoryTableOptions = options;
            indices = CreateIndex(Layout, memoryTableOptions);
        }

        #endregion

        #region IsReadonly

        /// <summary>Gets a value indicating whether this instance is readonly.</summary>
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

            lock (this)
            {
                Clear();
                long startID = 0;
                var rowCount = table.RowCount;
                while (true)
                {
                    var rows = table.GetRows(search & Search.FieldGreater(table.Layout.IDField.Name, startID), ResultOption.SortAscending("ID") + ResultOption.Limit(CaveSystemData.TransactionRowCount));
                    if (rows.Count == 0)
                    {
                        break;
                    }

                    var endID = Layout.GetID(rows[rows.Count - 1]);
                    Insert(rows, false);
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
                        Trace.TraceInformation(string.Format("Loaded {0} rows from table {1} starting with ID {2} to {3} ({4:N}%)", rows.Count, table, startID, endID, progress));
                    }
                    startID = endID;
                }
            }
        }

        #endregion

        #region SetRows

        /// <summary>Replaces the whole data at the table with the specified one without writing transactions.</summary>
        /// <param name="rows"></param>
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
            foreach (Row item in rows)
            {
                Insert(item, false);
            }
        }

        #endregion

        #region GetRow

        /// <summary>
        /// This function does a lookup on the ids of the table and returns the row with the n-th ID where n is the given index.
        /// Note that indices may change on each update, insert, delete and sorting is not garanteed!.
        /// <param name="index">The index of the row to be fetched</param>
        /// </summary>
        /// <returns>Returns the row.</returns>
        public override Row GetRowAt(int index)
        {
            var id = items.SortedKeys.ElementAt(index);
            if (Database.Storage.LogVerboseMessages)
            {
                Trace.TraceInformation("IndexOf {0} is ID {1} at {2}", index, id, this);
            }

            return items[id];
        }

        /// <summary>
        /// Gets a row from the table.
        /// </summary>
        /// <param name="id">The ID of the row to be fetched.</param>
        /// <returns>Returns the row.</returns>
        public override Row GetRow(long id)
        {
            return items[id];
        }

        /// <summary>
        /// Gets an array containing all rows of the memory table.
        /// </summary>
        /// <returns></returns>
        public override IList<Row> GetRows()
        {
            return items.Values;
        }

        /// <summary>Obtains the rows with the given ids.</summary>
        /// <param name="ids">IDs of the rows to fetch from the table.</param>
        /// <returns>Returns the rows.</returns>
        public override IList<Row> GetRows(IEnumerable<long> ids)
        {
            var rows = new List<Row>();
            foreach (var id in ids)
            {
                rows.Add(items[id]);
            }
            return rows;
        }

        #endregion

        #region Exist

        /// <summary>
        /// Checks a given ID for existance.
        /// </summary>
        /// <param name="id">The dataset ID to look for.</param>
        /// <returns>Returns whether the dataset exists or not.</returns>
        public override bool Exist(long id)
        {
            return items.ContainsKey(id);
        }

        #endregion

        #region Replace

        /// <summary>
        /// Replaces a row at the table. The ID has to be given. This inserts (if the row does not exist) or updates (if it exists) the row.
        /// </summary>
        /// <param name="row">The row to replace (valid ID needed).</param>
        /// <exception cref="ReadOnlyException">Table {0} is readonly!.</exception>
        public override void Replace(Row row)
        {
            Replace(row, TransactionLog != null);
        }

        /// <summary>
        /// Replaces a row at the table. The ID has to be given. This inserts (if the row does not exist) or updates (if it exists) the row.
        /// </summary>
        /// <param name="row">The row to replace (valid ID needed).</param>
        /// <param name="writeTransaction">If true a transaction is generated at the <see cref="TransactionLog"/>.</param>
        /// <exception cref="ReadOnlyException">Table {0} is readonly!.</exception>
        public void Replace(Row row, bool writeTransaction)
        {
            var id = Layout.GetID(row);
            if (id <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(row), "Invalid ID!");
            }

            if (Exist(id))
            {
                Update(row, writeTransaction);
            }
            else
            {
                id = Insert(row, writeTransaction);
            }
            if (writeTransaction)
            {
                TransactionLog?.AddReplaced(id, row.SetID(Layout.IDFieldIndex, id));
            }
        }

        /// <summary>
        /// Replaces a row at the table. The ID has to be given. This inserts (if the row does not exist) or updates (if it exists) the row.
        /// </summary>
        /// <param name="rows">The rows to replace (valid ID needed).</param>
        /// <param name="writeTransaction">If true a transaction is generated at the <see cref="TransactionLog"/>.</param>
        /// <exception cref="ReadOnlyException">Table {0} is readonly!.</exception>
        public void Replace(IEnumerable<Row> rows, bool writeTransaction)
        {
            foreach (Row row in rows)
            {
                Replace(row, writeTransaction);
            }
        }

        #endregion

        #region Insert

        /// <summary>
        /// Inserts a row to the table. If an ID &lt; 0 is given an automatically generated ID will be used to add the dataset.
        /// </summary>
        /// <param name="row">The row to insert. If an ID &lt;= 0 is given an automatically generated ID will be used to add the dataset.</param>
        /// <returns>Returns the ID of the inserted dataset.</returns>
        /// <exception cref="ReadOnlyException">Table {0} is readonly!.</exception>
        public override long Insert(Row row)
        {
            return Insert(row, TransactionLog != null);
        }

        /// <summary>
        /// Inserts a row to the table. If an ID &lt; 0 is given an automatically generated ID will be used to add the dataset.
        /// </summary>
        /// <param name="row">The row to insert. If an ID &lt;= 0 is given an automatically generated ID will be used to add the dataset.</param>
        /// <param name="writeTransaction">If true a transaction is generated at the <see cref="TransactionLog"/>.</param>
        /// <returns>Returns the ID of the inserted dataset.</returns>
        /// <exception cref="ReadOnlyException">Table {0} is readonly!.</exception>
        public long Insert(Row row, bool writeTransaction)
        {
            if (isReadonly)
            {
                throw new ReadOnlyException(string.Format("Table {0} is readonly!", this));
            }

            var id = Layout.GetID(row);
            if (Database.Storage.LogVerboseMessages)
            {
                Trace.TraceInformation("Insert {0} ID {1} at {2}", row, id, this);
            }

            if (id <= 0)
            {
                id = nextFreeID++;
                row = row.SetID(Layout.IDFieldIndex, id);
                items.Add(id, row);
            }
            else
            {
                items.Add(id, row);
                if (nextFreeID <= id)
                {
                    nextFreeID = id + 1;
                }
            }
            if (writeTransaction)
            {
                TransactionLog?.AddInserted(id, row);
            }
            if (indices != null)
            {
                for (var i = 0; i < FieldCount; i++)
                {
                    FieldIndex index = indices[i];
                    if (index == null)
                    {
                        continue;
                    }

                    index.Add(id, row.GetValue(i));
#if DEBUG
                    if (index.Count != RowCount)
                    {
                        throw new Exception(string.Format("BFDE: Operation: {0}, index.Count {1}, RowCount {2}", "Add", index.Count, RowCount));
                    }
#endif
                }
            }
            IncreaseSequenceNumber();
            return id;
        }

        /// <summary>
        /// Inserts rows into the table using a transaction.
        /// </summary>
        /// <param name="rows">The rows to insert.</param>
        /// <param name="writeTransaction">If true a transaction is generated at the <see cref="TransactionLog"/>.</param>
        /// <exception cref="ReadOnlyException">Table {0} is readonly!.</exception>
        public void Insert(IEnumerable<Row> rows, bool writeTransaction)
        {
            if (rows == null)
            {
                throw new ArgumentNullException("Rows");
            }

            foreach (Row row in rows)
            {
                Insert(row, writeTransaction);
            }
        }

        #endregion

        #region Update

        /// <summary>
        /// Updates a row to the table. The row must exist already!.
        /// </summary>
        /// <param name="row">The row to update.</param>
        /// <exception cref="ReadOnlyException">Table {0} is readonly!.</exception>
        public override void Update(Row row)
        {
            Update(row, TransactionLog != null);
        }

        /// <summary>
        /// Updates a row to the table. The row must exist already!.
        /// </summary>
        /// <param name="row">The row to update.</param>
        /// <param name="writeTransaction">If true a transaction is generated at the <see cref="TransactionLog"/>.</param>
        /// <exception cref="ReadOnlyException">Table {0} is readonly!.</exception>
        public void Update(Row row, bool writeTransaction)
        {
            if (isReadonly)
            {
                throw new ReadOnlyException(string.Format("Table {0} is readonly!", this));
            }

            var id = Layout.GetID(row);
            if (Database.Storage.LogVerboseMessages)
            {
                Trace.TraceInformation("Update {0} ID {1} at {2}", row, id, this);
            }

            if (id <= 0)
            {
                throw new ArgumentException("Row ID is out of range", nameof(row));
            }

            if (!items.TryGetValue(id, out Row oldRow))
            {
                throw new KeyNotFoundException("ID not present!");
            }

            items[id] = row;
            if (writeTransaction)
            {
                TransactionLog?.AddUpdated(id, row);
            }
            if (indices != null)
            {
                for (var i = 0; i < FieldCount; i++)
                {
                    FieldIndex index = indices[i];
                    if (index == null)
                    {
                        continue;
                    }

                    index.Replace(id, oldRow.GetValue(i), row.GetValue(i));
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

        /// <summary>
        /// Updates rows at the table. The rows must exist already!.
        /// </summary>
        /// <param name="rows">The rows to update.</param>
        /// <param name="writeTransaction">If true a transaction is generated at the <see cref="TransactionLog"/>.</param>
        /// <exception cref="ReadOnlyException">Table {0} is readonly!.</exception>
        public void Update(IEnumerable<Row> rows, bool writeTransaction)
        {
            if (rows == null)
            {
                throw new ArgumentNullException("Rows");
            }

            foreach (Row r in rows)
            {
                Update(r, writeTransaction);
            }
        }

        #endregion

        #region Delete

        /// <summary>
        /// Removes a row from the table.
        /// </summary>
        /// <param name="id">The dataset ID to remove.</param>
        /// <exception cref="ReadOnlyException">Table {0} is readonly!.</exception>
        public override void Delete(long id)
        {
            Delete(id, TransactionLog != null);
        }

        /// <summary>
        /// Removes a row from the table.
        /// </summary>
        /// <param name="id">The dataset ID to remove.</param>
        /// <param name="writeTransaction">If true a transaction is generated at the <see cref="TransactionLog"/>.</param>
        /// <exception cref="ReadOnlyException">Table {0} is readonly!.</exception>
        public void Delete(long id, bool writeTransaction = true)
        {
            if (isReadonly)
            {
                throw new ReadOnlyException(string.Format("Table {0} is readonly!", this));
            }

            if (Database.Storage.LogVerboseMessages)
            {
                Trace.TraceInformation("Delete {0} at {1}", id, this);
            }

            Row row = null;
            if (indices != null)
            {
                row = GetRow(id);
            }

            if (!items.Remove(id))
            {
                throw new ArgumentException(string.Format("ID {0} not found at table {1}!", id, Name));
            }

            if (indices != null)
            {
                for (var i = 0; i < FieldCount; i++)
                {
                    FieldIndex index = indices[i];
                    if (index == null)
                    {
                        continue;
                    }

                    index.Delete(id, row.GetValue(i));
#if DEBUG
                    if (index.Count != RowCount)
                    {
                        throw new Exception(string.Format("BFDE: Operation: {0}, index.Count {1}, RowCount {2}", "Delete", index.Count, RowCount));
                    }
#endif
                }
            }
            if (writeTransaction)
            {
                TransactionLog?.AddDeleted(id);
            }
            IncreaseSequenceNumber();
        }

        /// <summary>Removes a row from the table.</summary>
        /// <param name="ids">The ids.</param>
        /// <param name="writeTransaction">If true a transaction is generated at the <see cref="TransactionLog" />.</param>
        /// <exception cref="ReadOnlyException">Table {0} is readonly!.</exception>
        public void Delete(IEnumerable<long> ids, bool writeTransaction = true)
        {
            foreach (var id in ids)
            {
                Delete(id, writeTransaction);
            }
        }

        /// <summary>Removes all rows from the table matching the specified search.</summary>
        /// <param name="search">The Search used to identify rows for removal.</param>
        /// <returns>Returns the number of dataset deleted.</returns>
        public override int TryDelete(Search search)
        {
            return TryDelete(search, TransactionLog != null);
        }

        /// <summary>Removes all rows from the table matching the specified search.</summary>
        /// <param name="search">The Search used to identify rows for removal.</param>
        /// <param name="writeTransaction">if set to <c>true</c> [write transaction].</param>
        /// <returns>Returns the number of dataset deleted.</returns>
        public int TryDelete(Search search, bool writeTransaction)
        {
            if (search == null)
            {
                search = Search.None;
            }

            IEnumerable<long> ids = search.Scan(null, Layout, indices, this);
            var count = 0;
            foreach (var id in ids)
            {
                Delete(id, writeTransaction);
                count++;
            }
            return count;
        }

        #endregion

        #region Clear

        /// <summary>
        /// Clears all rows of the table (this operation will not write anything to the transaction log).
        /// </summary>
        /// <exception cref="ReadOnlyException">Table {0} is readonly!.</exception>
        public override void Clear()
        {
            Clear(true);
        }

        /// <summary>
        /// Clears all rows of the table (this operation will not write anything to the transaction log).
        /// </summary>
        /// <param name="resetIDs">if set to <c>true</c> [the next insert will get id 1].</param>
        /// <exception cref="ReadOnlyException">Table {0} is readonly!.</exception>
        public void Clear(bool resetIDs)
        {
            if (isReadonly)
            {
                throw new ReadOnlyException(string.Format("Table {0} is readonly!", this));
            }

            if (resetIDs)
            {
                nextFreeID = 1;
            }

            if (Database.Storage.LogVerboseMessages)
            {
                Trace.TraceInformation("Clear {0}", this);
            }

            items = new FakeSortedDictionary<long, Row>();
            indices = CreateIndex(Layout, memoryTableOptions);
            IncreaseSequenceNumber();
        }

        #endregion

        #region FindRows

        /// <summary>
        /// Searches the table for rows with given field value combinations.
        /// </summary>
        /// <param name="search">The search to run.</param>
        /// <param name="resultOption">Options for the search and the result set.</param>
        /// <returns>Returns the ID of the row found or -1.</returns>
        public override IList<long> FindRows(Search search = default(Search), ResultOption resultOption = default(ResultOption))
        {
            return FindRows(Layout, indices, this, search, resultOption);
        }

        #endregion

        #region Properties

        /// <summary>
        /// Gets the RowCount.
        /// </summary>
        public override long RowCount => items.Count;

        #region IDs

        /// <inheritdoc />
        public override IList<long> IDs => items.UnsortedKeys.ToList();

        /// <inheritdoc />
        public override IList<long> SortedIDs => items.SortedKeys;

        #endregion

        /// <summary>
        /// Gets/sets the transaction log used to store all changes. The user has to create it, dequeue the items and
        /// dispose it after usage!.
        /// </summary>
        public TransactionLog TransactionLog { get; set; }

        #endregion

        #region Used / Free IDs

        /// <summary>
        /// Gets the next used ID at the table (positive values are valid, negative ones are invalid, 0 is not defined!).
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public override long GetNextUsedID(long id)
        {
            foreach (var index in items.SortedKeys)
            {
                if (index > id)
                {
                    return index;
                }
            }
            return -1;
        }

        /// <summary>
        /// Gets the next free ID at the table.
        /// </summary>
        /// <returns></returns>
        public override long GetNextFreeID()
        {
            return nextFreeID;
        }

        /// <summary>
        /// Tries to set the next free id used at inserts.
        /// </summary>
        /// <param name="id"></param>
        /// <exception cref="ReadOnlyException">Table {0} is readonly!.</exception>
        public void SetNextFreeID(long id)
        {
            if (isReadonly)
            {
                throw new ReadOnlyException(string.Format("Table {0} is readonly!", this));
            }

            if (id < nextFreeID)
            {
                throw new InvalidOperationException(string.Format("Cannot set NextFreeID to a value in range of existant IDs!"));
            }

            nextFreeID = id;
        }

        #endregion

        /// <summary>Saves the table to a tar file.</summary>
        /// <param name="file">The file.</param>
        public void SaveTo(TarWriter file)
        {
            using (var stream = new MemoryStream())
            {
                var writer = new DatWriter(Layout, stream);
                writer.WriteTable(this);
                stream.Position = 0;
                file.AddFile(Database.Name + "/" + Name, stream, (int)stream.Length);
            }
        }
    }
}
