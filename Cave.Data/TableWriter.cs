using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Cave.Data
{
    /// <summary>
    /// Provides an asynchronous table writer.
    /// </summary>
    public class TableWriter : IDisposable
    {
        /// <summary>Gets the name of the log source.</summary>
        /// <value>The name of the log source.</value>
        public string LogSourceName => "TableWriter <" + Table.Name + ">";

        /// <summary>logs verbose messages.</summary>
        public bool LogVerboseMessages { get; set; }

        bool m_Flushing;
        bool m_Disposed;
        Task m_Task;
        bool m_Exit;
        long m_WrittenCount;
        long? m_LastFlush;
        TimeSpan maxSeenDelay = new TimeSpan(TimeSpan.TicksPerMinute);

        /// <summary>The table this instance writes to.</summary>
        public ITable Table { get; private set; }

        /// <summary>The transaction log.</summary>
        public TransactionLog TransactionLog { get; private set; }

        /// <summary>Gets or sets the transaction flags.</summary>
        /// <value>The transaction flags.</value>
        public TransactionFlags TransactionFlags { get; set; }

        /// <summary>
        /// Obtains the RowLayout of the table.
        /// </summary>
        public RowLayout Layout => Table.Layout;

        /// <summary>
        /// Gets / sets the cache flush treshold. This is the number of datasets the CachedTable will store before triggering a threshold violation and causing it to be flushed to the database.
        /// Set this to -1 to disable the treshold.
        /// </summary>
        public int CacheFlushTreshold { get; set; } = 1000;

        /// <summary>
        /// Gets / sets the minimum cache flush wait time. This is the time in milliseconds the Writer will wait before starting any flush to the database. 
        /// Set this to TimeSpan.Zero to disable the minimum wait time (the background thread will no longer sleep whenever anythis can be written).
        /// </summary>
        public int CacheFlushMinWaitTime { get; set; } = 1000;

        /// <summary>
        /// Gets / sets the maximum cache flush wait time. This is the time in milliseconds the Writer will wait before starting a forced flush to the database. 
        /// Set this to TimeSpan.Zero to disable the maximum wait time (the background thread will wait until a treshold violation occurs and the CacheFlushWaitTime is exceeded).
        /// </summary>
        public int CacheFlushMaxWaitTime { get; set; } = 60000;

        /// <summary>
        /// Gets / sets the number of transactions flushed per round (values &lt;= 0 will use the database default transaction count).
        /// </summary>
        public int FlushCount { get; set; } = 1000;

        /// <summary>
        /// Obtains the (local) date time of the last flush.
        /// </summary>
        public DateTime LastFlush => new DateTime((long)m_LastFlush);

        void UncatchedFlush()
        {
            if (m_Disposed)
            {
                throw new ObjectDisposedException(LogSourceName);
            }

            long written = Table.Commit(TransactionLog, TransactionFlags, FlushCount);
            if (written > 0)
            {
                Interlocked.Add(ref m_WrittenCount, written);

                TimeSpan delay = TimeSpan.Zero;
                Transaction t = TransactionLog.Peek();
                if (t != null)
                {
                    delay = DateTime.UtcNow - t.Created;
                    if (delay > maxSeenDelay)
                    {
                        maxSeenDelay = delay;
                        Trace.TraceWarning($"Delay <red>{0}<default>!", maxSeenDelay);
                    }
                }
                if (LogVerboseMessages)
                {
                    Trace.TraceWarning("Flushed {0} datasets to <green>{1}<default> (current delay <magenta>{1}<default>)", written, Table.Name, delay);
                }
            }
        }

        /// <summary>
        /// Flushes the data to the database and catches any errors.
        /// </summary>
        bool CatchedFlush()
        {
            try
            {
                UncatchedFlush();
                m_LastFlush = DateTime.UtcNow.Ticks;
                return true;
            }
            catch (Exception ex)
            {
                Debug.WriteLine(string.Format("Error while flushing cache to table {0}\n{1}", Layout, ex));
                return false;
            }
        }

        void CheckError()
        {
            Task task = m_Task;
            if (task != null && task.IsFaulted)
            {
                throw task.Exception;
            }
        }

        /// <summary>
        /// Checks whether the <see cref="CacheFlushTreshold"/> is exceeded or not. If a threshold violation is detected, the table is flushed to the database.
        /// </summary>
        void Worker()
        {
#if DEBUG
            Thread.CurrentThread.Name = "TableWriter " + Table;
#endif
            long nextMessage = 0;
            DateTime nextMaxWaitTimeExceeded = DateTime.UtcNow.AddMilliseconds(CacheFlushMaxWaitTime);

            while (!m_Exit)
            {
                //wait until at least one transaction is available
                while (!m_Exit && TransactionLog.Count == 0)
                {
                    nextMaxWaitTimeExceeded = DateTime.UtcNow.AddMilliseconds(CacheFlushMaxWaitTime);
                    if (LogVerboseMessages)
                    {
                        if (DateTime.UtcNow.Ticks > nextMessage)
                        {
                            Trace.TraceInformation(ToString());
                            nextMessage = DateTime.UtcNow.Ticks + TimeSpan.TicksPerMinute;
                        }
                    }
                    TransactionLog.Wait(1000);
                }

                int count = TransactionLog.Count;
                if (!m_Flushing)
                {    //obey min wait time
                    if (CacheFlushMinWaitTime > 0)
                    {
                        Thread.Sleep(CacheFlushMinWaitTime);
                    }
                    //obey treshold
                    if (CacheFlushTreshold > 0)
                    {
                        if (count < CacheFlushTreshold && DateTime.UtcNow < nextMaxWaitTimeExceeded)
                        {
                            continue;
                        }
                    }
                }

                int flushCount = FlushCount;
                if (flushCount <= 0)
                {
                    flushCount = 1000;
                }

                CatchedFlush();
            }
        }

        /// <summary>
        /// Commits all changes to the database.
        /// </summary>
        public void Flush()
        {
            CheckError();
            lock (this)
            {
                m_Flushing = true;
                if (m_Exit && !m_Task.IsCompleted)
                {
                    TransactionLog.Pulse();
                    m_Task.Wait();
                }
                while (!m_Exit && TransactionLog.Count > 0)
                {
                    TransactionLog.Pulse();
                    Thread.Sleep(10);
                }
                m_Flushing = false;
            }
            while (TransactionLog.Count > 0)
            {
                UncatchedFlush();
            }
        }

        /// <summary>Closes this instance after flushing all data.</summary>
        /// <exception cref="ObjectDisposedException">TableWriter.</exception>
        public void Close()
        {
            lock (this)
            {
                if (m_Exit)
                {
                    return;
                }

                m_Exit = true;
                Flush();
                Dispose();
            }
        }

        /// <summary>
        /// Creates a writer for the specified table.
        /// </summary>
        /// <param name="table">The underlying table.</param>
        public TableWriter(ITable table)
            : this(table, new TransactionLog(table.Layout))
        {
        }

        /// <summary>
        /// Creates a writer for the specified table.
        /// </summary>
        /// <param name="table">The underlying table.</param>
        /// <param name="log">The TransactionLog to watch for updates.</param>
        public TableWriter(ITable table, TransactionLog log)
        {
            LogVerboseMessages = Debugger.IsAttached;
            Table = table;
            TransactionLog = log;
            TransactionFlags = TransactionFlags.AllowRequeue;
            m_LastFlush = DateTime.Now.Ticks;
            m_Task = Task.Factory.StartNew(() =>
            {
                try { Worker(); }
                catch (Exception ex)
                {
                    Debug.WriteLine($"Fatal error in table writer. This is unrecoverable!\n{ex}");
                    Close();
                }
            }, TaskCreationOptions.LongRunning);
        }

        /// <summary>
        /// Obtains the number of items queued for writing.
        /// </summary>
        public int QueueCount => TransactionLog.Count;

        /// <summary>
        /// Obtains the number of items written.
        /// </summary>
        public long WrittenCount => Interlocked.Read(ref m_WrittenCount);

        /// <summary>Gets the error.</summary>
        /// <value>The error.</value>
        public Exception Error { get { Task task = m_Task; return task == null ? null : task.Exception; } }

        /// <summary>
        /// Writes all transaction of the given TransactionLog to the table.
        /// </summary>
        /// <param name="log"></param>
        public void Commit(TransactionLog log)
        {
            if (m_Disposed)
            {
                throw new ObjectDisposedException(LogSourceName);
            }

            if (log == null)
            {
                throw new ArgumentNullException(nameof(log));
            }

            CheckError();
            TransactionLog.AddRange(log.Dequeue(-1));
        }

        /// <summary>
        /// Writes a transaction.
        /// </summary>
        /// <param name="transaction">Transaction to write.</param>
        public void Write(Transaction transaction)
        {
            if (m_Disposed)
            {
                throw new ObjectDisposedException(LogSourceName);
            }

            CheckError();
            TransactionLog.Add(transaction);
        }

        /// <summary>
        /// Writes a transaction.
        /// </summary>
        /// <param name="transactionType">Type of transaction.</param>
        /// <param name="row">Row data.</param>
        public void Write(TransactionType transactionType, Row row)
        {
            if (m_Disposed)
            {
                throw new ObjectDisposedException(LogSourceName);
            }

            CheckError();
            long id = Layout.GetID(row);
            switch (transactionType)
            {
                case TransactionType.Inserted: Write(Transaction.Inserted(id, row)); break;
                case TransactionType.Deleted: Write(Transaction.Deleted(id)); break;
                case TransactionType.Updated: Write(Transaction.Updated(id, row)); break;
                case TransactionType.Replaced: Write(Transaction.Replaced(id, row)); break;
                default: throw new NotImplementedException(string.Format("TransactionType {0} unknown!", transactionType));
            }
        }

        /// <summary>
        /// Inserts a new row at the table using a background transaction.
        /// </summary>
        /// <param name="row">Row data.</param>
        public void Insert(Row row)
        {
            long id = Layout.GetID(row);
            Write(Transaction.Inserted(id, row));
        }

        /// <summary>Inserts rows at the table using a background transaction.</summary>
        /// <param name="rows">The rows.</param>
        public void Insert(IEnumerable<Row> rows)
        {
            foreach (Row row in rows)
            {
                Insert(row);
            }
        }

        /// <summary>
        /// Replaces a row at the table using a background transaction.
        /// </summary>
        /// <param name="row">Row data.</param>
        public void Replace(Row row)
        {
            long id = Layout.GetID(row);
            Write(Transaction.Replaced(id, row));
        }

        /// <summary>Replaces rows at the table using a background transaction.</summary>
        /// <param name="rows">The rows.</param>
        public void Replace(IEnumerable<Row> rows)
        {
            foreach (Row row in rows)
            {
                Replace(row);
            }
        }

        /// <summary>
        /// Updates a row at the table using a background transaction.
        /// </summary>
        /// <param name="row">Row data.</param>
        public void Update(Row row)
        {
            long id = Layout.GetID(row);
            Write(Transaction.Updated(id, row));
        }

        /// <summary>Updates rows at the table using a background transaction.</summary>
        /// <param name="rows">The rows.</param>
        public void Update(IEnumerable<Row> rows)
        {
            foreach (Row row in rows)
            {
                Update(row);
            }
        }

        /// <summary>
        /// Deletes a row at the table using a background transaction.
        /// </summary>
        /// <param name="id">ID to delete.</param>
        public void Delete(long id)
        {
            Write(Transaction.Deleted(id));
        }

        /// <summary>Deletes the specified ids.</summary>
        /// <param name="ids">The ids.</param>
        public void Delete(params long[] ids)
        {
            foreach (long id in ids)
            {
                Delete(id);
            }
        }

        /// <summary>Deletes the specified ids.</summary>
        /// <param name="ids">The ids.</param>
        public void Delete(IEnumerable<long> ids)
        {
            foreach (long id in ids)
            {
                Delete(id);
            }
        }

        /// <summary>
        /// Name Queue:0 Written:0.
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return string.Format("TableWriter {0} Queue:{1} Written:{2}", Table, QueueCount, WrittenCount);
        }

        #region IDisposable Support        
        /// <summary>Releases unmanaged and - optionally - managed resources.</summary>
        /// <param name="disposing"><c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (!m_Disposed)
            {
                m_Exit = true;
                m_Disposed = true;
                if (disposing)
                {
                    if (m_Task != null)
                    {
                        m_Exit = true;
                        TransactionLog.Pulse();
                        Task t = m_Task;
                        m_Task = null;
                        t.Wait();
                        t.Dispose();
                    }
                }
            }
        }

        /// <summary>Finalizes an instance of the <see cref="TableWriter"/> class.</summary>
        ~TableWriter()
        {
            Dispose(false);
        }

        /// <summary>
        /// Führt anwendungsspezifische Aufgaben durch, die mit der Freigabe, der Zurückgabe oder dem Zurücksetzen von nicht verwalteten Ressourcen zusammenhängen.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        #endregion
    }

    /// <summary>
    /// Provides an asynchronous table writer.
    /// </summary>
    public class TableWriter<T> : TableWriter
        where T : struct
    {
        /// <summary>
        /// Creates a writer for the specified table.
        /// </summary>
        /// <param name="table">The underlying table.</param>
        public TableWriter(ITable<T> table) : this(table, new TransactionLog<T>()) { }

        /// <summary>
        /// Creates a writer for the specified table.
        /// </summary>
        /// <param name="table">The underlying table.</param>
        /// <param name="log">The TransactionLog to watch for updates.</param>
        public TableWriter(ITable<T> table, TransactionLog<T> log)
            : base(table, log)
        {
            Table = table;
            TransactionLog = log;
        }

        /// <summary>
        /// Inserts a new row at the table using a background transaction.
        /// </summary>
        /// <param name="item">Row data.</param>
        public void Insert(T item)
        {
            Row row = Row.Create(Layout, item);
            long id = Layout.GetID(row);
            Write(Transaction.Inserted(id, row));
        }

        /// <summary>Inserts rows at the table using a background transaction.</summary>
        /// <param name="items">The items.</param>
        public void Insert(IEnumerable<T> items)
        {
            foreach (T item in items)
            {
                Insert(item);
            }
        }

        /// <summary>
        /// Replaces a row at the table using a background transaction.
        /// </summary>
        /// <param name="item">Row data.</param>
        public void Replace(T item)
        {
            Row row = Row.Create(Layout, item);
            long id = Layout.GetID(row);
            Write(Transaction.Replaced(id, row));
        }

        /// <summary>Replaces rows at the table using a background transaction.</summary>
        /// <param name="items">The items.</param>
        public void Replace(IEnumerable<T> items)
        {
            foreach (T item in items)
            {
                Replace(item);
            }
        }

        /// <summary>
        /// Updates a row at the table using a background transaction.
        /// </summary>
        /// <param name="item">Row data.</param>
        public void Update(T item)
        {
            Row row = Row.Create(Layout, item);
            long id = Layout.GetID(row);
            Write(Transaction.Updated(id, row));
        }

        /// <summary>Updates rows at the table using a background transaction.</summary>
        /// <param name="items">The items.</param>
        public void Update(IEnumerable<T> items)
        {
            foreach (T item in items)
            {
                Update(item);
            }
        }

        /// <summary>The table this instance writes to.</summary>
        public new ITable<T> Table { get; private set; }

        /// <summary>The transaction log.</summary>
        public new TransactionLog<T> TransactionLog { get; private set; }
    }
}
