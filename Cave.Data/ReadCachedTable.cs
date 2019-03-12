using System;
using System.Collections.Generic;

namespace Cave.Data
{
    /// <summary>
    /// Provides a read cache for table operations.
    /// </summary>
    public class ReadCachedTable : Table, IReadCachedTable
    {
        MemoryTable cacheTable;
        DateTime lastUpdate;

        /// <summary>
        /// Gets the current cache generation (this will increase on each update).
        /// </summary>
        public int Generation { get; private set; }

        /// <summary>
        /// The underlying table.
        /// </summary>
        public ITable BaseTable { get; }

        /// <summary>
        /// The search to use when updating the cache.
        /// </summary>
        public Search Search { get; } = Search.None;

        /// <summary>
        /// The result options to use when updating the cache.
        /// </summary>
        public ResultOption ResultOption { get; } = ResultOption.None;

        /// <summary>
        /// Gets the DateTime value of the last full update.
        /// </summary>
        public DateTime LastUpdate
        {
            get
            {
                lock (this)
                {
                    return lastUpdate;
                }
            }
        }

        /// <summary>
        /// Updates the whole table.
        /// </summary>
        public void UpdateCache()
        {
            var newCache = new MemoryTable(BaseTable.Layout);
            var rows = BaseTable.GetRows(Search, ResultOption);
            newCache.SetRows(rows);
            lock (this)
            {
                cacheTable = newCache;
                Generation++;
                lastUpdate = DateTime.UtcNow;
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

            BaseTable = source;
            Search = search;
            ResultOption = resultOption;
            UpdateCache();
        }

        /// <summary>
        /// Gets the (cached) row count for the specified search.
        /// </summary>
        /// <param name="search">The search to run.</param>
        /// <param name="resultOption">Options for the search and the result set.</param>
        /// <returns></returns>
        public override long Count(Search search = default(Search), ResultOption resultOption = default(ResultOption))
        {
            lock (this)
            {
                return cacheTable.Count(search, resultOption);
            }
        }

        /// <summary>
        /// Gets the (cached) row count.
        /// </summary>
        public override long RowCount
        {
            get
            {
                lock (this)
                {
                    return cacheTable.RowCount;
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
                BaseTable.Clear();
                cacheTable = new MemoryTable(BaseTable.Layout);
                Generation++;
            }
        }

        /// <summary>Searches the table for rows with given field value combinations.</summary>
        /// <param name="search">The search to run.</param>
        /// <param name="resultOption">Options for the search and the result set.</param>
        /// <returns>Returns the ID of the row found or -1.</returns>
        public override IList<long> FindRows(Search search = default(Search), ResultOption resultOption = default(ResultOption))
        {
            lock (this)
            {
                return cacheTable.FindRows(search, resultOption);
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
                return cacheTable.GetRowAt(index);
            }
        }

        /// <summary>
        /// Gets the row with the specified ID.
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public override Row GetRow(long id)
        {
            lock (this)
            {
                return cacheTable.GetRow(id);
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
                return cacheTable.Exist(id);
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
                var id = BaseTable.Insert(row);
                if (Layout.GetID(row) != id)
                {
                    row = row.SetID(Layout.IDFieldIndex, id);
                }
                cacheTable.Insert(row);
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
                BaseTable.Replace(row);
                cacheTable.Replace(row);
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
                BaseTable.Update(row);
                cacheTable.Update(row);
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
                cacheTable.Delete(id);
                BaseTable.Delete(id);
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
                cacheTable.TryDelete(search);
                return BaseTable.TryDelete(search);
            }
        }

        /// <summary>
        /// Gets the next used ID.
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public override long GetNextUsedID(long id)
        {
            lock (this)
            {
                return cacheTable.GetNextUsedID(id);
            }
        }

        /// <summary>
        /// Gets the next free ID.
        /// </summary>
        /// <returns></returns>
        public override long GetNextFreeID()
        {
            lock (this)
            {
                return BaseTable.GetNextFreeID();
            }
        }

        /// <summary>
        /// Gets an array containing all rows of the table.
        /// </summary>
        /// <returns></returns>
        public override IList<Row> GetRows()
        {
            lock (this)
            {
                return cacheTable.GetRows();
            }
        }

        /// <summary>Obtains the rows with the given ids.</summary>
        /// <param name="ids">IDs of the rows to fetch from the table.</param>
        /// <returns>Returns the rows.</returns>
        public override IList<Row> GetRows(IEnumerable<long> ids)
        {
            lock (this)
            {
                return cacheTable.GetRows(ids);
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
                var result = BaseTable.Commit(transactionLog, flags, count);
                UpdateCache();
                return result;
            }
        }
    }
}
