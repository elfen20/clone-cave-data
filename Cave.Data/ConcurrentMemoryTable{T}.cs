using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Threading;
using Cave.Collections.Generic;

namespace Cave.Data
{
    /// <summary>
    /// Provides a thread safe table stored completely in memory.
    /// </summary>
    [DebuggerDisplay("{Name}")]
    public class ConcurrentMemoryTable<T> : IMemoryTable<T>
        where T : struct
    {
        #region static class

        /// <summary>
        /// Converts the typed instance to an untyped one.
        /// </summary>
        /// <param name="table"></param>
        /// <returns></returns>
        public static implicit operator ConcurrentMemoryTable(ConcurrentMemoryTable<T> table)
        {
            return new ConcurrentMemoryTable(table);
        }
        #endregion

        /// <summary>Gets or sets the maximum wait time (milliseconds) while waiting for a write lock.</summary>
        /// <value>The maximum wait time (milliseconds) while waiting for a write lock.</value>
        /// <remarks>
        /// By default the maximum wait time is set to 100 milliseconds in release builds.
        /// Disabling the maximum wait time results in writing operations blocked forever if there are no gaps between reading operations.
        /// </remarks>
        public int MaxWaitTime { get; set; } = 100;

        #region private class
        MemoryTable<T> table;
        int readLock;
        object writeLock = new object();

        #region ReadLocked
        internal void ReadLocked(Action action)
        {
            lock (this)
            {
                Interlocked.Increment(ref readLock);
            }

            try
            {
                action();
            }
            finally
            {
                Interlocked.Decrement(ref readLock);
            }
        }

        internal TResult ReadLocked<TResult>(Func<TResult> func)
        {
            lock (this)
            {
                Interlocked.Increment(ref readLock);
            }

            try
            {
                return func();
            }
            finally
            {
                Interlocked.Decrement(ref readLock);
            }
        }
        #endregion

        #region WriteLocked
        void WriteLocked(Action action)
        {
            var wait = MaxWaitTime;
            lock (writeLock)
            {
                Monitor.Enter(this);
                if (readLock < 0)
                {
                    throw new Exception("Fatal readlock underflow, deadlock imminent!");
                }

                while (readLock > 0)
                {
                    if (wait > 0)
                    {
                        Stopwatch watch = null;
                        if (wait > 0)
                        {
                            watch = new Stopwatch();
                            watch.Start();
                        }

                        // spin until noone is reading anymore or spincount is reached
                        while (readLock > 0 && watch.ElapsedMilliseconds < wait)
                        {
                            Monitor.Exit(this);
                            Thread.Sleep(1);
                            Monitor.Enter(this);
                        }

                        // if spinning completed and we are still waiting on readers, keep lock until all readers are finished
                        while (readLock > 0)
                        {
                            Thread.Sleep(0);
                        }
                    }
                    else
                    {
                        // spin until noone is reading anymore, this may wait forever if there is no gap between reading processes
                        while (readLock > 0)
                        {
                            Monitor.Exit(this);
                            Thread.Sleep(1);
                            Monitor.Enter(this);
                        }
                    }
                }

                // write
                try
                {
                    action.Invoke();
                }
                finally
                {
                    Monitor.Exit(this);
                }
            }
        }
        #endregion
        #endregion

        #region constructors

        /// <summary>
        /// Initializes a new instance of the <see cref="ConcurrentMemoryTable{T}"/> class.
        /// </summary>
        public ConcurrentMemoryTable(MemoryTableOptions options = 0, string nameOverride = null)
        {
            table = new MemoryTable<T>(MemoryDatabase.Default, RowLayout.CreateTyped(typeof(T), nameOverride), options);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ConcurrentMemoryTable{T}"/> class.
        /// </summary>
        public ConcurrentMemoryTable(string nameOverride, MemoryTableOptions options = 0)
        {
            table = new MemoryTable<T>(MemoryDatabase.Default, RowLayout.CreateTyped(typeof(T), nameOverride), options);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ConcurrentMemoryTable{T}"/> class.
        /// </summary>
        /// <param name="table"></param>
        /// <exception cref="ReadOnlyException">Table {0} is readonly!.</exception>
        public ConcurrentMemoryTable(MemoryTable<T> table)
        {
            if (table.IsReadonly)
            {
                throw new ReadOnlyException(string.Format("Table {0} is readonly!", this));
            }

            this.table = table;
        }
        #endregion

        #region ITable<T> members

        /// <summary>
        /// Gets a row from the table.
        /// </summary>
        /// <param name="id">The ID of the row to be fetched.</param>
        /// <returns>Returns the row.</returns>
        public T GetStruct(long id)
        {
            return ReadLocked(() => table.GetStruct(id));
        }

        /// <summary>Calculates the sum of the specified field name for all matching rows.</summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="search">The search.</param>
        /// <returns></returns>
        public double Sum(string fieldName, Search search = null)
        {
            return ReadLocked(() => table.Sum(fieldName, search));
        }

        /// <summary>
        /// Inserts a row into the table. If an ID &lt;= 0 is given an automatically generated ID will be used to add the dataset.
        /// </summary>
        /// <param name="row">The row to insert. If an ID &lt;= 0 is given an automatically generated ID will be used to add the dataset.</param>
        /// <returns>Returns the ID of the inserted dataset.</returns>
        public long Insert(T row)
        {
            long id = 0;
            WriteLocked(() =>
            {
                id = table.Insert(row);
            });
            return id;
        }

        /// <summary>
        /// Inserts rows into the table using a transaction.
        /// </summary>
        /// <param name="rows">The rows to insert.</param>
        public void Insert(IEnumerable<T> rows)
        {
            WriteLocked(() =>
            {
                table.Insert(rows);
            });
        }

        /// <summary>Inserts rows into the table using a transaction.</summary>
        /// <param name="rows">The rows to insert.</param>
        /// <param name="writeTransaction">if set to <c>true</c> [write transaction].</param>
        public void Insert(IEnumerable<T> rows, bool writeTransaction)
        {
            WriteLocked(() =>
            {
                table.Insert(rows, writeTransaction);
            });
        }

        /// <summary>
        /// Updates a row at the table. The row must exist already!.
        /// </summary>
        /// <param name="row">The row to update.</param>
        public void Update(T row)
        {
            WriteLocked(() =>
            {
                table.Update(row);
            });
        }

        /// <summary>
        /// Updates rows at the table. The rows must exist already!.
        /// </summary>
        /// <param name="rows">The rows to update.</param>
        public void Update(IEnumerable<T> rows)
        {
            WriteLocked(() =>
            {
                table.Update(rows);
            });
        }

        /// <summary>Updates rows at the table. The rows must exist already!.</summary>
        /// <param name="rows">The rows to update.</param>
        /// <param name="writeTransaction">if set to <c>true</c> [write transaction].</param>
        public void Update(IEnumerable<T> rows, bool writeTransaction = true)
        {
            WriteLocked(() =>
            {
                table.Update(rows, writeTransaction);
            });
        }

        /// <summary>
        /// Replaces a row at the table. The ID has to be given. This inserts (if the row does not exist) or updates (if it exists) the row.
        /// </summary>
        /// <param name="row">The row to replace (valid ID needed).</param>
        public void Replace(T row)
        {
            WriteLocked(() =>
            {
                table.Replace(row);
            });
        }

        /// <summary>
        /// Replaces rows at the table. This inserts (if the row does not exist) or updates (if it exists) each row.
        /// </summary>
        /// <param name="rows">The rows to replace (valid ID needed).</param>
        public void Replace(IEnumerable<T> rows)
        {
            WriteLocked(() =>
            {
                table.Replace(rows);
            });
        }

        /// <summary>
        /// Replaces rows at the table. This inserts (if the row does not exist) or updates (if it exists) each row.
        /// </summary>
        /// <param name="rows">The rows to replace (valid ID needed).</param>
        /// <param name="writeTransaction">if set to <c>true</c> [write transaction].</param>
        public void Replace(IEnumerable<T> rows, bool writeTransaction)
        {
            WriteLocked(() =>
            {
                table.Replace(rows, writeTransaction);
            });
        }

        /// <summary>
        /// Searches the table for a single row with given search.
        /// </summary>
        /// <param name="search">The search to run.</param>
        /// <returns>Returns the row found.</returns>
        public T GetStruct(Search search)
        {
            return ReadLocked(() => table.GetStruct(search));
        }

        /// <summary>
        /// Searches the table for a single row with given search.
        /// </summary>
        /// <param name="search">The search to run.</param>
        /// <param name="resultOption">Options for the search and the result set.</param>
        /// <returns>Returns the row found.</returns>
        public T GetStruct(Search search = default(Search), ResultOption resultOption = default(ResultOption))
        {
            return ReadLocked(() => table.GetStruct(search, resultOption));
        }

        /// <summary>
        /// Searches the table for rows with given search.
        /// </summary>
        /// <param name="search">The search to run.</param>
        /// <returns>Returns the rows found.</returns>
        public IList<T> GetStructs(Search search)
        {
            return ReadLocked(() => table.GetStructs(search));
        }

        /// <summary>
        /// Searches the table for rows with given search.
        /// </summary>
        /// <param name="search">The search to run.</param>
        /// <param name="resultOption">Options for the search and the result set.</param>
        /// <returns>Returns the rows found.</returns>
        public IList<T> GetStructs(Search search = default(Search), ResultOption resultOption = default(ResultOption))
        {
            return ReadLocked(() => table.GetStructs(search, resultOption));
        }

        /// <summary>
        /// Gets the row with the specified ID.
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public T this[long id] => ReadLocked(() => table[id]);

        /// <summary>
        /// Gets the rows with the given ids.
        /// </summary>
        /// <param name="ids">IDs of the rows to fetch from the table.</param>
        /// <returns>Returns the rows.</returns>
        public virtual IList<T> GetStructs(IEnumerable<long> ids)
        {
            return ReadLocked(() => table.GetStructs(ids));
        }

        /// <summary>
        /// The storage engine the database belongs to.
        /// </summary>
        public IStorage Storage => table.Storage;

        /// <summary>
        /// Gets the database the table belongs to.
        /// </summary>
        public IDatabase Database => table.Database;

        /// <summary>
        /// Gets the name of the table.
        /// </summary>
        public string Name => table.Name;

        /// <summary>
        /// Gets the RowLayout of the table.
        /// </summary>
        public RowLayout Layout => table.Layout;

        /// <summary>
        /// Counts the results of a given search.
        /// </summary>
        /// <param name="search">The search to run.</param>
        /// <param name="resultOption">Options for the search and the result set.</param>
        /// <returns>Returns the number of rows found matching the criteria given.</returns>
        public virtual long Count(Search search = default(Search), ResultOption resultOption = default(ResultOption))
        {
            return ReadLocked(() => table.Count(search, resultOption));
        }

        /// <summary>
        /// Gets the RowCount.
        /// </summary>
        public long RowCount => ReadLocked(() => table.RowCount);

        /// <summary>
        /// Clears all rows of the table using the <see cref="Clear(bool)"/> [resetIDs = true] function.
        /// </summary>
        public void Clear()
        {
            Clear(true);
        }

        /// <summary>
        /// Clears all rows of the table (this operation will not write anything to the transaction log).
        /// </summary>
        /// <param name="resetIDs">if set to <c>true</c> [the next insert will get id 1].</param>
        public virtual void Clear(bool resetIDs)
        {
            WriteLocked(() =>
            {
                table.Clear(resetIDs);
            });
        }

        /// <summary>
        /// Gets a row from the table.
        /// </summary>
        /// <param name="id">The ID of the row to be fetched.</param>
        /// <returns>Returns the row.</returns>
        public Row GetRow(long id)
        {
            return ReadLocked(() => table.GetRow(id));
        }

        /// <summary>
        /// This function does a lookup on the ids of the table and returns the row with the n-th ID where n is the given index.
        /// Note that indices may change on each update, insert, delete and sorting is not garanteed!.
        /// <param name="index">The index of the row to be fetched</param>
        /// </summary>
        /// <returns>Returns the row.</returns>
        public Row GetRowAt(int index)
        {
            return ReadLocked(() => table.GetRowAt(index));
        }

        /// <summary>
        /// Sets the specified value to the specified fieldname on all rows.
        /// </summary>
        /// <param name="field">The fields name.</param>
        /// <param name="value">The value to set.</param>
        public void SetValue(string field, object value)
        {
            WriteLocked(() =>
            {
                table.SetValue(field, value);
            });
        }

        /// <summary>
        /// Checks a given ID for existance.
        /// </summary>
        /// <param name="id">The dataset ID to look for.</param>
        /// <returns>Returns whether the dataset exists or not.</returns>
        public bool Exist(long id)
        {
            return ReadLocked(() => table.Exist(id));
        }

        /// <summary>Checks a given search for any datasets matching.</summary>
        /// <param name="search"></param>
        /// <returns></returns>
        public bool Exist(Search search)
        {
            return ReadLocked(() => table.Exist(search));
        }

        /// <summary>
        /// Inserts a row into the table. If an ID &lt;= 0 is given an automatically generated ID will be used to add the dataset.
        /// </summary>
        /// <param name="row">The row to insert. If an ID &lt;= 0 is given an automatically generated ID will be used to add the dataset.</param>
        /// <returns>Returns the ID of the inserted dataset.</returns>
        public long Insert(Row row)
        {
            long id = 0;
            WriteLocked(() =>
            {
                id = table.Insert(row);
            });
            return id;
        }

        /// <summary>
        /// Inserts rows into the table using a transaction.
        /// </summary>
        /// <param name="rows">The rows to insert.</param>
        public void Insert(IEnumerable<Row> rows)
        {
            WriteLocked(() =>
            {
                table.Insert(rows);
            });
        }

        /// <summary>Inserts rows into the table using a transaction.</summary>
        /// <param name="rows">The rows to insert.</param>
        /// <param name="writeTransaction">If true a transaction is generated at the <see cref="TransactionLog" />.</param>
        public void Insert(IEnumerable<Row> rows, bool writeTransaction)
        {
            WriteLocked(() =>
            {
                table.Insert(rows, writeTransaction);
            });
        }

        /// <summary>
        /// Updates a row at the table. The row must exist already!.
        /// </summary>
        /// <param name="row">The row to update.</param>
        /// <returns>Returns the ID of the dataset.</returns>
        public void Update(Row row)
        {
            WriteLocked(() =>
            {
                table.Update(row);
            });
        }

        /// <summary>
        /// Updates rows at the table using a transaction.
        /// </summary>
        /// <param name="rows">The rows to insert.</param>
        public void Update(IEnumerable<Row> rows)
        {
            WriteLocked(() =>
            {
                table.Update(rows);
            });
        }

        /// <summary>Updates rows at the table using a transaction.</summary>
        /// <param name="rows">The rows to insert.</param>
        /// <param name="writeTransaction">If true a transaction is generated at the <see cref="TransactionLog" />.</param>
        public void Update(IEnumerable<Row> rows, bool writeTransaction)
        {
            WriteLocked(() =>
            {
                table.Update(rows, writeTransaction);
            });
        }

        /// <summary>
        /// Removes a row from the table.
        /// </summary>
        /// <param name="id">The dataset ID to remove.</param>
        public void Delete(long id)
        {
            WriteLocked(() =>
            {
                table.Delete(id);
            });
        }

        /// <summary>
        /// Removes rows from the table.
        /// </summary>
        /// <param name="ids">The dataset IDs to remove.</param>
        public void Delete(IEnumerable<long> ids)
        {
            WriteLocked(() =>
            {
                table.Delete(ids);
            });
        }

        /// <summary>Removes all rows from the table matching the specified search.</summary>
        /// <param name="search">The Search used to identify rows for removal.</param>
        /// <returns>Returns the number of dataset deleted.</returns>
        public int TryDelete(Search search)
        {
            var result = 0;
            WriteLocked(() =>
            {
                result = table.TryDelete(search);
            });
            return result;
        }

        /// <summary>Removes all rows from the table matching the specified search.</summary>
        /// <param name="search">The Search used to identify rows for removal.</param>
        /// <param name="writeTransaction">If true a transaction is generated at the <see cref="TransactionLog" />.</param>
        /// <returns>Returns the number of dataset deleted.</returns>
        public int TryDelete(Search search, bool writeTransaction)
        {
            var result = 0;
            WriteLocked(() =>
            {
                result = table.TryDelete(search, writeTransaction);
            });
            return result;
        }

        /// <summary>
        /// Replaces rows at the table. This inserts (if the row does not exist) or updates (if it exists) each row.
        /// </summary>
        /// <param name="rows">The rows to replace (valid ID needed).</param>
        public void Replace(IEnumerable<Row> rows)
        {
            WriteLocked(() =>
            {
                table.Replace(rows);
            });
        }

        /// <summary>
        /// Replaces rows at the table. This inserts (if the row does not exist) or updates (if it exists) each row.
        /// </summary>
        /// <param name="rows">The rows to replace (valid ID needed).</param>
        /// <param name="writeTransaction">If true a transaction is generated at the <see cref="TransactionLog" />.</param>
        public void Replace(IEnumerable<Row> rows, bool writeTransaction)
        {
            WriteLocked(() =>
            {
                table.Replace(rows, writeTransaction);
            });
        }

        /// <summary>Searches the table for a row with given field value combinations.</summary>
        /// <param name="search">The search to run.</param>
        /// <param name="resultOption">Options for the search and the result set.</param>
        /// <returns>Returns the ID of the row found or -1.</returns>
        public long FindRow(Search search = default(Search), ResultOption resultOption = default(ResultOption))
        {
            return ReadLocked(() => table.FindRow(search, resultOption));
        }

        /// <summary>
        /// Searches the table for rows with given field value combinations.
        /// </summary>
        /// <param name="search">The search to run.</param>
        /// <param name="resultOption">Options for the search and the result set.</param>
        /// <returns>Returns the IDs of the rows found.</returns>
        public IList<long> FindRows(Search search = default(Search), ResultOption resultOption = default(ResultOption))
        {
            return ReadLocked(() => table.FindRows(search, resultOption));
        }

        /// <summary>
        /// Searches the table for rows with given field value combinations.
        /// </summary>
        /// <param name="search">The search to run.</param>
        /// <returns>Returns the IDs of the rows found.</returns>
        public IList<long> FindRows(Search search)
        {
            return ReadLocked(() => table.FindRows(search));
        }

        /// <summary>
        /// Searches the table for a single row with given search.
        /// </summary>
        /// <param name="search">The search to run.</param>
        /// <param name="resultOption">Options for the search and the result set.</param>
        /// <returns>Returns the row found.</returns>
        public Row GetRow(Search search = default(Search), ResultOption resultOption = default(ResultOption))
        {
            return ReadLocked(() => table.GetRow(search, resultOption));
        }

        /// <summary>
        /// Searches the table for rows with given search.
        /// </summary>
        /// <param name="search">The search to run.</param>
        /// <param name="resultOption">Options for the search and the result set.</param>
        /// <returns>Returns the rows found.</returns>
        public IList<Row> GetRows(Search search = default(Search), ResultOption resultOption = default(ResultOption))
        {
            return ReadLocked(() => table.GetRows(search, resultOption));
        }

        /// <summary>
        /// Gets the next used ID at the table (positive values are valid, negative ones are invalid, 0 is not defined!).
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public long GetNextUsedID(long id)
        {
            return ReadLocked(() => table.GetNextUsedID(id));
        }

        /// <summary>
        /// Gets the next free ID at the table.
        /// </summary>
        /// <returns></returns>
        public long GetNextFreeID()
        {
            return ReadLocked(() => table.GetNextFreeID());
        }

        /// <summary>
        /// Gets the rows with the given ids.
        /// </summary>
        /// <param name="ids">IDs of the rows to fetch from the table.</param>
        /// <returns>Returns the rows.</returns>
        public virtual IList<Row> GetRows(IEnumerable<long> ids)
        {
            return ReadLocked(() => table.GetRows(ids));
        }

        /// <summary>
        /// Gets an array with all rows.
        /// </summary>
        /// <returns></returns>
        public IList<Row> GetRows()
        {
            return ReadLocked(() => table.GetRows());
        }

        /// <summary>Obtains all different field values of a given field.</summary>
        /// <typeparam name="TValue">The type of the value.</typeparam>
        /// <param name="field">The field.</param>
        /// <param name="includeNull">allow null value to be added to the results.</param>
        /// <param name="ids">The ids.</param>
        /// <returns></returns>
        public IItemSet<TValue> GetValues<TValue>(string field, bool includeNull = false, IEnumerable<long> ids = null)
        {
            return ReadLocked(() => table.GetValues<TValue>(field, includeNull, ids));
        }

        /// <summary>Commits a whole TransactionLog to the table.</summary>
        /// <param name="transactions">The transaction log to read.</param>
        /// <param name="flags">The flags to use.</param>
        /// <param name="count">Number of transactions to combine at one write.</param>
        /// <returns>Returns the number of transactions done or -1 if unknown.</returns>
        public int Commit(TransactionLog transactions, TransactionFlags flags = TransactionFlags.Default, int count = -1)
        {
            WriteLocked(() =>
            {
                count = table.Commit(transactions, flags, count);
            });
            return count;
        }

        /// <summary>Tries to get the (unique) row with the given fieldvalue.</summary>
        /// <param name="search">The search.</param>
        /// <param name="row">The row.</param>
        /// <returns>Returns true on success, false otherwise.</returns>
        public bool TryGetStruct(Search search, out T row)
        {
            var ids = FindRows(search);
            if (ids.Count == 1)
            {
                row = GetStruct(ids[0]);
                return true;
            }
            row = default(T);
            return false;
        }

        #endregion

        #region MemoryTable<T> additions

        /// <summary>Gets the sequence number.</summary>
        /// <value>The sequence number.</value>
        public int SequenceNumber => table.SequenceNumber;

        /// <summary>Gets a value indicating whether this instance is readonly.</summary>
        /// <value><c>true</c> if this instance is readonly; otherwise, <c>false</c>.</value>
        public bool IsReadonly => table.IsReadonly;

        #region LoadTable

        /// <summary>Replaces all data present with the data at the given table.</summary>
        /// <param name="table">The table to load.</param>
        /// <param name="search">The search.</param>
        /// <param name="callback">The callback.</param>
        /// <param name="userItem">The user item.</param>
        /// <exception cref="ArgumentNullException">Table.</exception>
        public void LoadTable(ITable table, Search search = null, ProgressCallback callback = null, object userItem = null)
        {
            if (table == null)
            {
                throw new ArgumentNullException(nameof(table));
            }

            WriteLocked(() =>
            {
                this.table.LoadTable(table, search, callback, userItem);
            });
        }
        #endregion

        #region SetStructs

        /// <summary>
        /// Replaces the whole data at the table with the specified one without writing transactions.
        /// </summary>
        /// <param name="items"></param>
        public void SetStructs(IEnumerable<T> items)
        {
            WriteLocked(() =>
            {
                table.SetStructs(items);
            });
        }
        #endregion

        #region SetRows

        /// <summary>
        /// Replaces the whole data at the table with the specified one without writing transactions.
        /// </summary>
        /// <param name="rows"></param>
        public void SetRows(IEnumerable<Row> rows)
        {
            WriteLocked(() =>
            {
                table.SetRows(rows);
            });
        }
        #endregion

        #region IDs

        /// <inheritdoc/>
        public IList<long> IDs => ReadLocked(() => table.IDs);

        /// <inheritdoc/>
        public IList<long> SortedIDs => ReadLocked(() => table.SortedIDs);

        #endregion

        #region TransactionLog

        /// <summary>
        /// Gets/sets the transaction log used to store all changes. The user has to create it, dequeue the items and
        /// dispose it after usage!.
        /// </summary>
        public virtual TransactionLog TransactionLog
        {
            // no need to lock anything here, transaction log is already thread safe
            get => table.TransactionLog;
            set => table.TransactionLog = value;
        }
        #endregion

        #region GetStructAt

        /// <summary>
        /// Gets the row struct with the given index.
        /// This allows a memorytable to be used as virtual list for listviews, ...
        /// Note that indices will change on each update, insert, delete and sorting is not garanteed!.
        /// </summary>
        /// <param name="index">The rows index (0..RowCount-1).</param>
        /// <returns></returns>
        /// <exception cref="IndexOutOfRangeException"></exception>
        public virtual T GetStructAt(int index)
        {
            return ReadLocked(() => table.GetStructAt(index));
        }
        #endregion

        #region Insert

        /// <summary>Inserts the specified row.</summary>
        /// <param name="row">The row.</param>
        /// <param name="writeTransaction">if set to <c>true</c> [write transaction].</param>
        /// <returns></returns>
        public long Insert(T row, bool writeTransaction)
        {
            long id = 0;
            WriteLocked(() =>
            {
                id = table.Insert(row, writeTransaction);
            });
            return id;
        }

        /// <summary>
        /// Inserts a row to the table. If an ID &lt; 0 is given an automatically generated ID will be used to add the dataset.
        /// </summary>
        /// <param name="row">The row to insert. If an ID &lt;= 0 is given an automatically generated ID will be used to add the dataset.</param>
        /// <param name="writeTransaction">If true a transaction is generated at the <see cref="TransactionLog" />.</param>
        /// <returns>Returns the ID of the inserted dataset.</returns>
        public long Insert(Row row, bool writeTransaction)
        {
            long id = 0;
            WriteLocked(() =>
            {
                id = table.Insert(row, writeTransaction);
            });
            return id;
        }
        #endregion

        #region Replace

        /// <summary>Replaces the specified row.</summary>
        /// <param name="row">The row.</param>
        /// <param name="writeTransaction">if set to <c>true</c> [write transaction].</param>
        public void Replace(T row, bool writeTransaction)
        {
            WriteLocked(() =>
            {
                table.Replace(row, writeTransaction);
            });
        }

        /// <summary>
        /// Replaces a row at the table. The ID has to be given. This inserts (if the row does not exist) or updates (if it exists) the row.
        /// </summary>
        /// <param name="row">The row to replace (valid ID needed).</param>
        public void Replace(Row row)
        {
            WriteLocked(() =>
            {
                table.Replace(row);
            });
        }

        /// <summary>
        /// Replaces a row at the table. The ID has to be given. This inserts (if the row does not exist) or updates (if it exists) the row.
        /// </summary>
        /// <param name="row">The row to replace (valid ID needed).</param>
        /// <param name="writeTransaction">If true a transaction is generated at the <see cref="TransactionLog" />.</param>
        public void Replace(Row row, bool writeTransaction)
        {
            WriteLocked(() =>
            {
                table.Replace(row, writeTransaction);
            });
        }
        #endregion

        #region Update

        /// <summary>Updates the specified row.</summary>
        /// <param name="row">The row.</param>
        /// <param name="writeTransaction">if set to <c>true</c> [write transaction].</param>
        /// <returns></returns>
        public void Update(T row, bool writeTransaction)
        {
            WriteLocked(() =>
            {
                table.Update(row, writeTransaction);
            });
        }

        /// <summary>Updates a row to the table. The row must exist already!.</summary>
        /// <param name="row">The row to update.</param>
        /// <param name="writeTransaction">If true a transaction is generated at the <see cref="TransactionLog" />.</param>
        /// <returns></returns>
        public void Update(Row row, bool writeTransaction)
        {
            WriteLocked(() =>
            {
                table.Update(row, writeTransaction);
            });
        }
        #endregion

        #region Delete

        /// <summary>Removes a row from the table.</summary>
        /// <param name="id">The dataset ID to remove.</param>
        /// <param name="writeTransaction">If true a transaction is generated at the <see cref="TransactionLog" />.</param>
        public void Delete(long id, bool writeTransaction = true)
        {
            WriteLocked(() =>
            {
                table.Delete(id, writeTransaction);
            });
        }

        /// <summary>Removes a row from the table.</summary>
        /// <param name="ids">The ids.</param>
        /// <param name="writeTransaction">If true a transaction is generated at the <see cref="TransactionLog" />.</param>
        public void Delete(IEnumerable<long> ids, bool writeTransaction = true)
        {
            WriteLocked(() =>
            {
                table.Delete(ids, writeTransaction);
            });
        }
        #endregion

        #endregion

        #region ToString and eXtended Text

        /// <summary>Returns a <see cref="string" /> that represents this instance.</summary>
        /// <returns>A <see cref="string" /> that represents this instance.</returns>
        public override string ToString()
        {
            return $"Table {Database.Name}.{Name} [{RowCount} Rows]";
        }
        #endregion
    }
}
