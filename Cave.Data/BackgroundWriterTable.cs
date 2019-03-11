using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Cave.Data
{
    /// <summary>
    /// Creates a <see cref="SynchronizedMemoryTable"/> with a background <see cref="TableWriter"/> instance.
    /// </summary>
    public sealed class BackgroundWriterTable : SynchronizedMemoryTable, ICachedTable
    {
        /// <summary>The base table.</summary>
        public ITable BaseTable { get; private set; }

        /// <summary>The table writer.</summary>
        public TableWriter TableWriter { get; private set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="BackgroundWriterTable" /> class.
        /// </summary>
        /// <param name="table">Table to load.</param>
        public BackgroundWriterTable(ITable table)
            : base(table.Layout)
        {
            BaseTable = table;
            LoadTable(table);
            base.TransactionLog = new TransactionLog(table.Layout);
            TableWriter = new TableWriter(table, base.TransactionLog);
        }

        /// <summary>Reloads this instance from the database.</summary>
        public void Reload()
        {
            lock (this)
            {
                TableWriter.Flush();
                LoadTable(BaseTable);
            }
        }

        /// <summary>
        /// Gets the transaction log used to store all changes.
        /// </summary>
        /// <exception cref="NotSupportedException">Changing the transactionlog is not supported!.</exception>
        public override TransactionLog TransactionLog
        {
            get => base.TransactionLog;
            set => throw new NotSupportedException("Changing the transactionlog is not supported!");
        }

        /// <summary>
        /// Inserts a row into the table. If an ID &lt;= 0 is specified an automatically generated ID will be used to add the dataset.
        /// </summary>
        /// <param name="row">The row to insert. If an ID &lt;= 0 is specified an automatically generated ID will be used to add the dataset.</param>
        /// <returns>Returns the ID of the inserted dataset.</returns>
        public Task InsertAsync(Row row)
        {
            return Task.Factory.StartNew((r) => { Insert((Row)r); }, row);
        }

        /// <summary>
        /// Inserts rows into the table using a transaction.
        /// </summary>
        /// <param name="rows">The rows to insert.</param>
        public Task InsertAsync(IEnumerable<Row> rows)
        {
            return Task.Factory.StartNew((r) => { Insert((Row)r); }, rows);
        }

        /// <summary>
        /// Updates a row at the table. The row must exist already!.
        /// </summary>
        /// <param name="row">The row to update.</param>
        public Task UpdateAsync(Row row)
        {
            return Task.Factory.StartNew((r) => { Update((Row)r); }, row);
        }

        /// <summary>
        /// Updates rows at the table. The rows must exist already!.
        /// </summary>
        /// <param name="rows">The rows to update.</param>
        public Task UpdateAsync(IEnumerable<Row> rows)
        {
            return Task.Factory.StartNew((r) => { Update((Row)r); }, rows);
        }

        /// <summary>
        /// Replaces a row at the table. The ID has to be given. This inserts (if the row does not exist) or updates (if it exists) the row.
        /// </summary>
        /// <param name="row">The row to replace (valid ID needed).</param>
        public Task ReplaceAsync(Row row)
        {
            return Task.Factory.StartNew((r) => { Replace((Row)r); }, row);
        }

        /// <summary>
        /// Replaces rows at the table. This inserts (if the row does not exist) or updates (if it exists) each row.
        /// </summary>
        /// <param name="rows">The rows to replace (valid ID needed).</param>
        public Task ReplaceAsync(IEnumerable<Row> rows)
        {
            return Task.Factory.StartNew((r) => { Replace((Row)r); }, rows);
        }

        /// <summary>
        /// Clears all rows of the table.
        /// </summary>
        /// <param name="resetIDs">if set to <c>true</c> [the next insert will get id 1].</param>
        public override void Clear(bool resetIDs)
        {
            lock (this)
            {
                BaseTable.Clear();
                base.Clear(resetIDs);
            }
        }

        /// <summary>Flushes all changes done to this instance.</summary>
        public void Flush()
        {
            TableWriter.Flush();
        }

        /// <summary>Closes this instance after flushing all data.</summary>
        public void Close()
        {
            TableWriter.Close();
        }
    }

    /// <summary>
    /// Creates a <see cref="SynchronizedMemoryTable{T}"/> with a background <see cref="TableWriter"/> instance.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public sealed class BackgroundWriterTable<T> : SynchronizedMemoryTable<T>, ICachedTable<T>
        where T : struct
    {
        /// <summary>The base table.</summary>
        public ITable BaseTable { get; private set; }

        /// <summary>Gets the name of the log source.</summary>
        /// <value>The name of the log source.</value>
        public override string LogSourceName => "BackgroundWriterTable <" + Name + ">";

        /// <summary>The table writer.</summary>
        public TableWriter TableWriter { get; private set; }

        /// <summary>Gets the asynchronous exception.</summary>
        /// <value>The asynchronous exception.</value>
        public Exception AsyncException { get; private set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="BackgroundWriterTable{T}"/> class.
        /// </summary>
        public BackgroundWriterTable(ITable<T> table)
            : base()
        {
            BaseTable = table;
            LoadTable(table);
            base.TransactionLog = new TransactionLog(table.Layout);
            TableWriter = new TableWriter(table, base.TransactionLog);
        }

        /// <summary>Reloads this instance from the database.</summary>
        public void Reload()
        {
            lock (this)
            {
                TableWriter.Flush();
                LoadTable(BaseTable);
            }
        }

        /// <summary>
        /// Gets the transaction log used to store all changes.
        /// </summary>
        /// <exception cref="NotSupportedException">Changing the transactionlog is not supported!.</exception>
        public override TransactionLog TransactionLog
        {
            get => base.TransactionLog;
            set => throw new NotSupportedException("Changing the transactionlog is not supported!");
        }

        /// <summary>
        /// Clears all rows of the table.
        /// </summary>
        /// <param name="resetIDs">if set to <c>true</c> [the next insert will get id 1].</param>
        public override void Clear(bool resetIDs)
        {
            lock (this)
            {
                BaseTable.Clear();
                base.Clear(resetIDs);
            }
        }

        /// <summary>
        /// Obtains the row struct with the specified index.
        /// This allows a memorytable to be used as virtual list for listviews, ...
        /// Note that indices may change on each update, insert, delete and sorting is not garanteed!.
        /// </summary>
        /// <param name="index">The rows index (0..RowCount-1).</param>
        /// <returns></returns>
        /// <exception cref="IndexOutOfRangeException"></exception>
        public override T GetStructAt(int index)
        {
            return GetRowAt(index).GetStruct<T>(Layout);
        }

        /// <summary>
        /// Inserts a row into the table. If an ID &lt;= 0 is specified an automatically generated ID will be used to add the dataset.
        /// </summary>
        /// <param name="row">The row to insert. If an ID &lt;= 0 is specified an automatically generated ID will be used to add the dataset.</param>
        /// <returns>Returns the ID of the inserted dataset.</returns>
        public Task InsertAsync(T row)
        {
            return Task.Factory.StartNew((r) => { Insert((T)r); }, row);
        }

        /// <summary>
        /// Inserts rows into the table using a transaction.
        /// </summary>
        /// <param name="rows">The rows to insert.</param>
        public Task InsertAsync(IEnumerable<T> rows)
        {
            return Task.Factory.StartNew((r) => { Insert((T)r); }, rows);
        }

        /// <summary>
        /// Updates a row at the table. The row must exist already!.
        /// </summary>
        /// <param name="row">The row to update.</param>
        public Task UpdateAsync(T row)
        {
            return Task.Factory.StartNew((r) => { Update((T)r); }, row);
        }

        /// <summary>
        /// Updates rows at the table. The rows must exist already!.
        /// </summary>
        /// <param name="rows">The rows to update.</param>
        public Task UpdateAsync(IEnumerable<T> rows)
        {
            return Task.Factory.StartNew((r) => { Update((T)r); }, rows);
        }

        /// <summary>
        /// Replaces a row at the table. The ID has to be given. This inserts (if the row does not exist) or updates (if it exists) the row.
        /// </summary>
        /// <param name="row">The row to replace (valid ID needed).</param>
        public Task ReplaceAsync(T row)
        {
            return Task.Factory.StartNew((r) => { Replace((T)r); }, row);
        }

        /// <summary>
        /// Replaces rows at the table. This inserts (if the row does not exist) or updates (if it exists) each row.
        /// </summary>
        /// <param name="rows">The rows to replace (valid ID needed).</param>
        public Task ReplaceAsync(IEnumerable<T> rows)
        {
            return Task.Factory.StartNew((r) => { Replace((T)r); }, rows);
        }

        /// <summary>Flushes all changes done to this instance.</summary>
        public void Flush()
        {
            TableWriter.Flush();
        }

        /// <summary>Closes this instance after flushing all data.</summary>
        public void Close()
        {
            TableWriter.Close();
        }
    }
}
