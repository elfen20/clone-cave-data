using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

namespace Cave.Data
{
    /// <summary>
    /// Provides a thread safe table stored completely in memory.
    /// </summary>
    [DebuggerDisplay("{Name}")]
    public class ConcurrentTable : ITable
    {
        readonly object writeLock = new object();
        int readLock;

        #region constructors

        /// <summary>
        /// Initializes a new instance of the <see cref="ConcurrentTable"/> class.
        /// </summary>
        /// <param name="table">The table to synchronize.</param>
        public ConcurrentTable(ITable table)
        {
            BaseTable = !(table is ConcurrentTable) ? table : throw new ArgumentException("Table is already synchronized!");
        }

        #endregion

        #region properties

        /// <summary>Gets or sets the maximum wait time (milliseconds) while waiting for a write lock.</summary>
        /// <value>The maximum wait time (milliseconds) while waiting for a write lock.</value>
        /// <remarks>
        /// By default the maximum wait time is set to 100 milliseconds in release builds.
        /// Disabling the maximum wait time results in writing operations blocked forever if there are no gaps between reading operations.
        /// </remarks>
        public int MaxWaitTime { get; set; } = 100;

        /// <summary>Gets the sequence number.</summary>
        /// <value>The sequence number.</value>
        public int SequenceNumber => BaseTable.SequenceNumber;

        #endregion

        #region ITable properties

        /// <inheritdoc/>
        public TableFlags Flags => BaseTable.Flags;

        /// <summary>
        /// Gets the base table used.
        /// </summary>
        public ITable BaseTable { get; }

        /// <inheritdoc/>
        public IStorage Storage => BaseTable.Storage;

        /// <inheritdoc/>
        public IDatabase Database => BaseTable.Database;

        /// <inheritdoc/>
        public string Name => BaseTable.Name;

        /// <inheritdoc/>
        public RowLayout Layout => BaseTable.Layout;

        /// <inheritdoc/>
        public long RowCount => ReadLocked(() => BaseTable.RowCount);

        #endregion

        #region ITable functions

        /// <inheritdoc/>
        public virtual void Connect(IDatabase database, TableFlags flags, RowLayout layout) => BaseTable.Connect(database, flags, layout);

        /// <inheritdoc/>
        public virtual void UseLayout(RowLayout layout) => BaseTable.UseLayout(layout);

        /// <inheritdoc/>
        public virtual long Count(Search search = default, ResultOption resultOption = default) => ReadLocked(() => BaseTable.Count(search, resultOption));

        /// <inheritdoc/>
        public double Sum(string fieldName, Search search = null) => ReadLocked(() => BaseTable.Sum(fieldName, search));

        /// <inheritdoc/>
        public virtual void Clear() => WriteLocked(BaseTable.Clear);

        /// <inheritdoc/>
        public Row GetRowAt(int index) => ReadLocked(() => BaseTable.GetRowAt(index));

        /// <inheritdoc/>
        public void SetValue(string field, object value) => WriteLocked(() => BaseTable.SetValue(field, value));

        /// <inheritdoc/>
        public bool Exist(Search search) => ReadLocked(() => BaseTable.Exist(search));

        /// <inheritdoc/>
        public bool Exist(Row row) => ReadLocked(() => BaseTable.Exist(row));

        /// <inheritdoc/>
        public Row Insert(Row row) => WriteLocked(() => BaseTable.Insert(row));

        /// <inheritdoc/>
        public void Insert(IEnumerable<Row> rows) => WriteLocked(() => BaseTable.Insert(rows));

        /// <inheritdoc/>
        public void Update(Row row) => WriteLocked(() => BaseTable.Update(row));

        /// <inheritdoc/>
        public void Update(IEnumerable<Row> rows) => WriteLocked(() => BaseTable.Update(rows));

        /// <inheritdoc/>
        public void Delete(Row row) => WriteLocked(() => BaseTable.Delete(row));

        /// <inheritdoc/>
        public void Delete(IEnumerable<Row> rows) => WriteLocked(() => BaseTable.Delete(rows));

        /// <inheritdoc/>
        public int TryDelete(Search search) => WriteLocked(() => BaseTable.TryDelete(search));

        /// <inheritdoc/>
        public void Replace(Row row) => WriteLocked(() => BaseTable.Replace(row));

        /// <inheritdoc/>
        public void Replace(IEnumerable<Row> rows) => WriteLocked(() => BaseTable.Replace(rows));

        /// <inheritdoc/>
        public Row GetRow(Search search = default, ResultOption resultOption = default) => ReadLocked(() => BaseTable.GetRow(search, resultOption));

        /// <inheritdoc/>
        public IList<Row> GetRows(Search search = default, ResultOption resultOption = default) => ReadLocked(() => BaseTable.GetRows(search, resultOption));

        /// <inheritdoc/>
        public IList<Row> GetRows() => ReadLocked(() => BaseTable.GetRows());

        /// <inheritdoc/>
        public IList<TValue> GetValues<TValue>(string field, Search search = null)
            where TValue : struct, IComparable
            => ReadLocked(() => BaseTable.GetValues<TValue>(field, search));

        /// <inheritdoc/>
        public IList<TValue> Distinct<TValue>(string field, Search search = null)
            where TValue : struct, IComparable
            => ReadLocked(() => BaseTable.Distinct<TValue>(field, search));

        /// <inheritdoc/>
        public TValue? Maximum<TValue>(string field, Search search = null)
            where TValue : struct, IComparable
            => ReadLocked(() => BaseTable.Maximum<TValue>(field, search));

        /// <inheritdoc/>
        public TValue? Minimum<TValue>(string field, Search search = null)
            where TValue : struct, IComparable
            => ReadLocked(() => BaseTable.Minimum<TValue>(field, search));

        /// <inheritdoc/>
        public int Commit(IEnumerable<Transaction> transactions, TransactionFlags flags = TransactionFlags.Default) => WriteLocked(() => BaseTable.Commit(transactions, flags));

        #endregion

        #region ToString and eXtended Text

        /// <summary>Returns a <see cref="string" /> that represents this instance.</summary>
        /// <returns>A <see cref="string" /> that represents this instance.</returns>
        public override string ToString()
        {
            return $"Table {Database.Name}.{Name} [{RowCount} Rows]";
        }
        #endregion

        #region ReadLocked
        void ReadLocked(Action action)
        {
            int v;
            lock (this)
            {
                v = Interlocked.Increment(ref readLock);
            }

            if (Storage.LogVerboseMessages)
            {
                Trace.TraceInformation("ReadLock <green>enter <magenta>{0}", v);
            }

            try
            {
                action();
            }
            finally
            {
                v = Interlocked.Decrement(ref readLock);
                if (Storage.LogVerboseMessages)
                {
                    Trace.TraceInformation("ReadLock <red>exit <magenta>{0}", v);
                }
            }
        }

        TResult ReadLocked<TResult>(Func<TResult> func)
        {
            TResult result = default;
            ReadLocked(() => { result = func(); });
            return result;
        }
        #endregion

        #region WriteLocked

        void WriteLocked(Action action)
        {
            var wait = MaxWaitTime;
            if (Storage.LogVerboseMessages)
            {
                Trace.TraceInformation("WriteLock <cyan>wait (read lock <magenta>{0}<default>)", readLock);
            }

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
                if (Storage.LogVerboseMessages)
                {
                    Trace.TraceInformation("WriteLock <green>acquired (read lock <magenta>{0}<default>)", readLock);
                }

                // write
                try
                {
                    action.Invoke();
                }
                finally
                {
                    Monitor.Exit(this);
                    if (Storage.LogVerboseMessages)
                    {
                        Trace.TraceInformation("WriteLock <red>exit (read lock <magenta>{0}<default>)", readLock);
                    }
                }
            }
        }

        TResult WriteLocked<TResult>(Func<TResult> func)
        {
            TResult result = default;
            WriteLocked(() => { result = func(); });
            return result;
        }

        #endregion
    }
}
