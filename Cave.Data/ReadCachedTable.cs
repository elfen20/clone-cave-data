using System;
using System.Collections.Generic;

namespace Cave.Data
{
    /// <summary>
    /// Provides a read cache for table operations.
    /// </summary>
    public class ReadCachedTable : Table, IReadCachedTable
    {
        /// <summary>
        /// Provides access to the underlying base table.
        /// </summary>
        readonly ITable m_BaseTable;

        /// <summary>
        /// Provides access to the memory cache table.
        /// </summary>
        MemoryTable m_CacheTable;
        DateTime m_LastUpdate;
        readonly Search m_Search = Search.None;
        readonly ResultOption m_ResultOption = ResultOption.None;

        /// <summary>
        /// Obtains the current cache generation (this will increase on each update).
        /// </summary>
        public int Generation { get; private set; }

        /// <summary>
        /// Obtains the DateTime value of the last full update.
        /// </summary>
        public DateTime LastUpdate { get { lock (this) { return m_LastUpdate; } } }

        /// <summary>
        /// Updates the whole table.
        /// </summary>
        public void UpdateCache()
        {
            MemoryTable newCache = new MemoryTable(m_BaseTable.Layout);
            List<Row> rows = m_BaseTable.GetRows(m_Search, m_ResultOption);
            newCache.SetRows(rows);
            lock (this)
            {
                m_CacheTable = newCache;
                Generation++;
                m_LastUpdate = DateTime.UtcNow;
            }
        }

        /// <summary>
        /// Creates a read cache for the specified table.
        /// </summary>
        /// <param name="source">Source database table.</param>
        /// <param name="search">The search to use.</param>
        /// <param name="resultOption">The result options to use.</param>
        public ReadCachedTable(ITable source, Search search = default(Search), ResultOption resultOption = default(ResultOption))
            : base(source.Database, source.Layout)
        {
            if (search == null)
            {
                search = Search.None;
            }

            if (resultOption == null)
            {
                resultOption = ResultOption.None;
            }

            m_BaseTable = source;
            m_Search = search;
            m_ResultOption = resultOption;
            UpdateCache();
        }

        /// <summary>
        /// Obtains the (cached) row count for the specified search.
        /// </summary>
        /// <param name="search">The search to run.</param>
        /// <param name="resultOption">Options for the search and the result set.</param>
        /// <returns></returns>
        public override long Count(Search search = default(Search), ResultOption resultOption = default(ResultOption))
        {
            lock (this)
            {
                return m_CacheTable.Count(search, resultOption);
            }
        }

        /// <summary>
        /// Obtains the (cached) row count.
        /// </summary>
        public override long RowCount
        {
            get
            {
                lock (this)
                {
                    return m_CacheTable.RowCount;
                }
            }
        }

        /// <summary>
        /// Clears all rows at the table.
        /// </summary>
        public override void Clear()
        {
            lock (this)
            {
                m_BaseTable.Clear();
                m_CacheTable = new MemoryTable(m_BaseTable.Layout);
                Generation++;
            }
        }

        /// <summary>Searches the table for rows with given field value combinations.</summary>
        /// <param name="search">The search to run.</param>
        /// <param name="resultOption">Options for the search and the result set.</param>
        /// <returns>Returns the ID of the row found or -1.</returns>
        public override List<long> FindRows(Search search = default(Search), ResultOption resultOption = default(ResultOption))
        {
            lock (this)
            {
                return m_CacheTable.FindRows(search, resultOption);
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
            lock (this)
            {
                return m_CacheTable.GetRowAt(index);
            }
        }

        /// <summary>
        /// Obtains the row with the specified ID.
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public override Row GetRow(long id)
        {
            lock (this)
            {
                return m_CacheTable.GetRow(id);
            }
        }

        /// <summary>
        /// Checks whether a row with the specified ID is present at the cache or not.
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public override bool Exist(long id)
        {
            lock (this)
            {
                return m_CacheTable.Exist(id);
            }
        }

        /// <summary>
        /// Inserts a row at the table and cache. If an ID &lt;= 0 is specified an automatically generated ID will be used to add the dataset.
        /// </summary>
        /// <param name="row">The row to insert. If an ID &lt;= 0 is specified an automatically generated ID will be used to add the dataset.</param>
        /// <returns>Returns the ID of the inserted dataset.</returns>
        public override long Insert(Row row)
        {
            lock (this)
            {
                long id = m_BaseTable.Insert(row);
                if (Layout.GetID(row) != id)
                {
                    row = row.SetID(Layout.IDFieldIndex, id);
                }
                m_CacheTable.Insert(row);
                return id;
            }
        }

        /// <summary>
        /// Replaces a row at the table. The ID has to be given. This inserts (if the row does not exist) or updates (if it exists) the row.
        /// </summary>
        /// <param name="row">The row to replace (valid ID needed).</param>
        public override void Replace(Row row)
        {
            lock (this)
            {
                m_BaseTable.Replace(row);
                m_CacheTable.Replace(row);
            }
        }

        /// <summary>
        /// Updates a row at the table and cache. The row must exist already!.
        /// </summary>
        /// <param name="row">The row to update.</param>
        /// <returns>Returns the ID of the dataset.</returns>
        public override void Update(Row row)
        {
            lock (this)
            {
                m_BaseTable.Update(row);
                m_CacheTable.Update(row);
            }
        }

        /// <summary>
        /// Removes a row from the table and cache.
        /// </summary>
        /// <param name="id">The dataset ID to remove.</param>
        public override void Delete(long id)
        {
            lock (this)
            {
                m_CacheTable.Delete(id);
                m_BaseTable.Delete(id);
            }
        }

        /// <summary>
        /// Removes all rows from the table matching the specified search.
        /// </summary>
        /// <param name="search">The Search used to identify rows for removal.</param>
        public override int TryDelete(Search search)
        {
            lock (this)
            {
                m_CacheTable.TryDelete(search);
                return m_BaseTable.TryDelete(search);
            }
        }

        /// <summary>
        /// Obtains the next used ID.
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public override long GetNextUsedID(long id)
        {
            lock (this)
            {
                return m_CacheTable.GetNextUsedID(id);
            }
        }

        /// <summary>
        /// Obtains the next free ID.
        /// </summary>
        /// <returns></returns>
        public override long GetNextFreeID()
        {
            lock (this)
            {
                return m_BaseTable.GetNextFreeID();
            }
        }

        /// <summary>
        /// Obtains an array containing all rows of the table.
        /// </summary>
        /// <returns></returns>
        public override List<Row> GetRows()
        {
            lock (this)
            {
                return m_CacheTable.GetRows();
            }
        }

        /// <summary>Obtains the rows with the given ids.</summary>
        /// <param name="ids">IDs of the rows to fetch from the table.</param>
        /// <returns>Returns the rows.</returns>
        public override List<Row> GetRows(IEnumerable<long> ids)
        {
            lock (this)
            {
                return m_CacheTable.GetRows(ids);
            }
        }

        /// <summary>Commits a whole TransactionLog to the table.</summary>
        /// <param name="transactionLog">The transaction log to read.</param>
        /// <param name="flags">The flags to use.</param>
        /// <param name="count">Number of transactions to combine at one write.</param>
        /// <returns>Returns the number of transactions done or -1 if unknown.</returns>
        public override int Commit(TransactionLog transactionLog, TransactionFlags flags = TransactionFlags.Default, int count = -1)
        {
            lock (this)
            {
                int result = m_BaseTable.Commit(transactionLog, flags, count);
                UpdateCache();
                return result;
            }
        }
    }

    /// <summary>
    /// Provides a read cache for table operations.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class ReadCachedTable<T> : Table<T>, IReadCachedTable<T>
        where T : struct
    {
        /// <summary>
        /// Provides access to the underlying base table.
        /// </summary>
        readonly ITable<T> m_BaseTable;

        /// <summary>
        /// Provides access to the memory cache table.
        /// </summary>
        MemoryTable<T> m_CacheTable;

        readonly Search m_Search = Search.None;
        readonly ResultOption m_ResultOption = ResultOption.None;

        /// <summary>
        /// Obtains the current cache generation (this will increase on each update).
        /// </summary>
        public int Generation { get; private set; }

        /// <summary>
        /// Obtains the DateTime value of the last full update.
        /// </summary>
        public DateTime LastUpdate { get; private set; }

        /// <summary>
        /// Updates the whole table.
        /// </summary>
        public void UpdateCache()
        {
            MemoryTable<T> newCache = new MemoryTable<T>();
            List<Row> rows = m_BaseTable.GetRows(m_Search, m_ResultOption);
            newCache.SetRows(rows);
            lock (this)
            {
                m_CacheTable = newCache;
                LastUpdate = DateTime.UtcNow;
                Generation += 1;
            }
        }

        /// <summary>
        /// Creates a read cache for the specified table.
        /// </summary>
        /// <param name="source">Source database table.</param>
        /// <param name="search">The search to use.</param>
        /// <param name="resultOption">The result options to use.</param>
        public ReadCachedTable(ITable<T> source, Search search = default(Search), ResultOption resultOption = default(ResultOption))
            : base(source.Database, source.Layout)
        {
            if (search == null)
            {
                search = Search.None;
            }

            if (resultOption == null)
            {
                resultOption = ResultOption.None;
            }

            m_BaseTable = source;
            m_Search = search;
            m_ResultOption = resultOption;
            UpdateCache();
        }

        /// <summary>
        /// Obtains the (cached) row count for the specified search.
        /// </summary>
        /// <param name="search">The search to run.</param>
        /// <param name="resultOption">Options for the search and the result set.</param>
        /// <returns></returns>
        public override long Count(Search search = default(Search), ResultOption resultOption = default(ResultOption))
        {
            lock (this)
            {
                return m_CacheTable.Count(search, resultOption);
            }
        }

        /// <summary>
        /// Obtains the (cached) row count.
        /// </summary>
        public override long RowCount
        {
            get
            {
                lock (this)
                {
                    return m_CacheTable.RowCount;
                }
            }
        }

        /// <summary>
        /// Clears the whole table.
        /// </summary>
        public override void Clear()
        {
            lock (this)
            {
                m_BaseTable.Clear();
                m_CacheTable = new MemoryTable<T>();
                Generation++;
                LastUpdate = DateTime.UtcNow;
            }
        }

        /// <summary>Searches the table for rows with given field value combinations.</summary>
        /// <param name="search">The search to run.</param>
        /// <param name="resultOption">Options for the search and the result set.</param>
        /// <returns>Returns the ID of the row found or -1.</returns>
        public override List<long> FindRows(Search search = default(Search), ResultOption resultOption = default(ResultOption))
        {
            lock (this)
            {
                return m_CacheTable.FindRows(search, resultOption);
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
            lock (this)
            {
                return m_CacheTable.GetRowAt(index);
            }
        }

        /// <summary>
        /// Obtains the row with the specified ID.
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public override Row GetRow(long id)
        {
            lock (this)
            {
                return m_CacheTable.GetRow(id);
            }
        }

        /// <summary>
        /// Checks whether a row with the specified ID is present at the cache or not.
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public override bool Exist(long id)
        {
            lock (this)
            {
                return m_CacheTable.Exist(id);
            }
        }

        /// <summary>
        /// Inserts a row at the table and cache. If an ID &lt;= 0 is specified an automatically generated ID will be used to add the dataset.
        /// </summary>
        /// <param name="row">The row to insert. If an ID &lt;= 0 is specified an automatically generated ID will be used to add the dataset.</param>
        /// <returns>Returns the ID of the inserted dataset.</returns>
        public override long Insert(Row row)
        {
            lock (this)
            {
                long id = m_BaseTable.Insert(row);
                if (id != Layout.GetID(row))
                {
                    row = row.SetID(Layout.IDFieldIndex, id);
                }

                m_CacheTable.Insert(row);
                return id;
            }
        }

        /// <summary>
        /// Replaces a row at the table. The ID has to be given. This inserts (if the row does not exist) or updates (if it exists) the row.
        /// </summary>
        /// <param name="row">The row to replace (valid ID needed).</param>
        public override void Replace(Row row)
        {
            lock (this)
            {
                m_BaseTable.Replace(row);
                m_CacheTable.Replace(row);
            }
        }

        /// <summary>
        /// Updates a row at the table and cache. The row must exist already!.
        /// </summary>
        /// <param name="row">The row to update.</param>
        /// <returns>Returns the ID of the dataset.</returns>
        public override void Update(Row row)
        {
            lock (this)
            {
                m_BaseTable.Update(row);
                m_CacheTable.Update(row);
            }
        }

        /// <summary>
        /// Removes a row from the table and cache.
        /// </summary>
        /// <param name="id">The dataset ID to remove.</param>
        public override void Delete(long id)
        {
            lock (this)
            {
                m_CacheTable.Delete(id);
                m_BaseTable.Delete(id);
            }
        }

        /// <summary>
        /// Removes all rows from the table matching the specified search.
        /// </summary>
        /// <param name="search">The Search used to identify rows for removal.</param>
        public override int TryDelete(Search search)
        {
            lock (this)
            {
                m_CacheTable.TryDelete(search);
                return m_BaseTable.TryDelete(search);
            }
        }

        /// <summary>
        /// Obtains the next used ID.
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public override long GetNextUsedID(long id)
        {
            lock (this)
            {
                return m_CacheTable.GetNextUsedID(id);
            }
        }

        /// <summary>
        /// Obtains the next free ID.
        /// </summary>
        /// <returns></returns>
        public override long GetNextFreeID()
        {
            lock (this)
            {
                return m_BaseTable.GetNextFreeID();
            }
        }

        /// <summary>
        /// Obtains the row struct with the specified index. 
        /// This allows the cache to be used as virtual list for listviews, ...
        /// Note that indices will change on each update!.
        /// </summary>
        /// <param name="index">The rows index (0..RowCount-1).</param>
        /// <returns></returns>
        public override T GetStructAt(int index)
        {
            lock (this)
            {
                return m_CacheTable.GetStructAt(index);
            }
        }

        /// <summary>
        /// Obtains an array containing all rows of the table.
        /// </summary>
        /// <returns></returns>
        public override List<Row> GetRows()
        {
            lock (this)
            {
                return m_CacheTable.GetRows();
            }
        }

        /// <summary>Obtains the rows with the given ids.</summary>
        /// <param name="ids">IDs of the rows to fetch from the table.</param>
        /// <returns>Returns the rows.</returns>
        public override List<Row> GetRows(IEnumerable<long> ids)
        {
            lock (this)
            {
                return m_CacheTable.GetRows(ids);
            }
        }

        /// <summary>Obtains a row from the table.</summary>
        /// <param name="id">The ID of the row to be fetched.</param>
        /// <returns>Returns the row.</returns>
        public override T GetStruct(long id)
        {
            lock (this)
            {
                return m_CacheTable.GetStruct(id);
            }
        }

        /// <summary>Obtains the rows with the given ids.</summary>
        /// <param name="ids">IDs of the rows to fetch from the table.</param>
        /// <returns>Returns the rows.</returns>
        public override List<T> GetStructs(IEnumerable<long> ids)
        {
            lock (this)
            {
                return m_CacheTable.GetStructs(ids);
            }
        }

        /// <summary>Commits a whole TransactionLog to the table.</summary>
        /// <param name="transactionLog">The transaction log to read.</param>
        /// <param name="flags">The flags to use.</param>
        /// <param name="count">Number of transactions to combine at one write.</param>
        /// <returns>Returns the number of transactions done or -1 if unknown.</returns>
        public override int Commit(TransactionLog transactionLog, TransactionFlags flags = TransactionFlags.Default, int count = -1)
        {
            lock (this)
            {
                int result = m_BaseTable.Commit(transactionLog, flags, count);
                UpdateCache();
                return result;
            }
        }
    }
}
