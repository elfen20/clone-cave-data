using System;
using System.Collections.Generic;

namespace Cave.Data
{
    /// <summary>Provides a synchronized wrapper for table instances.</summary>
    public class SynchronizedTable : ITable
    {
        /// <summary>Initializes a new instance of the <see cref="SynchronizedTable" /> class.</summary>
        /// <param name="table">The table to synchronize.</param>
        public SynchronizedTable(ITable table) =>
            BaseTable = !(table is SynchronizedTable) ? table : throw new ArgumentException("Table is already synchronized!");

        /// <summary>Gets the base table used.</summary>
        public ITable BaseTable { get; }

        /// <inheritdoc />
        public TableFlags Flags => BaseTable.Flags;

        /// <inheritdoc />
        public int SequenceNumber => BaseTable.SequenceNumber;

        /// <inheritdoc />
        public IStorage Storage => BaseTable.Storage;

        /// <inheritdoc />
        public IDatabase Database => BaseTable.Database;

        /// <inheritdoc />
        public string Name => BaseTable.Name;

        /// <inheritdoc />
        public RowLayout Layout => BaseTable.Layout;

        /// <inheritdoc />
        public long RowCount
        {
            get
            {
                lock (BaseTable)
                {
                    return BaseTable.RowCount;
                }
            }
        }

        /// <inheritdoc />
        public void Connect(IDatabase database, TableFlags flags, RowLayout layout)
        {
            lock (BaseTable)
            {
                BaseTable.Connect(database, flags, layout);
            }
        }

        /// <inheritdoc />
        public virtual void UseLayout(RowLayout layout)
        {
            lock (BaseTable)
            {
                BaseTable.UseLayout(layout);
            }
        }

        /// <inheritdoc />
        public long Count(Search search = null, ResultOption resultOption = null)
        {
            lock (BaseTable)
            {
                return BaseTable.Count(search, resultOption);
            }
        }

        /// <inheritdoc />
        public void Clear()
        {
            lock (BaseTable)
            {
                BaseTable.Clear();
            }
        }

        /// <inheritdoc />
        public Row GetRowAt(int index)
        {
            lock (BaseTable)
            {
                return BaseTable.GetRowAt(index);
            }
        }

        /// <inheritdoc />
        public void SetValue(string field, object value)
        {
            lock (BaseTable)
            {
                BaseTable.SetValue(field, value);
            }
        }

        /// <inheritdoc />
        public bool Exist(Search search)
        {
            lock (BaseTable)
            {
                return BaseTable.Exist(search);
            }
        }

        /// <inheritdoc />
        public bool Exist(Row row)
        {
            lock (BaseTable)
            {
                return BaseTable.Exist(row);
            }
        }

        /// <inheritdoc />
        public Row Insert(Row row)
        {
            lock (BaseTable)
            {
                return BaseTable.Insert(row);
            }
        }

        /// <inheritdoc />
        public void Insert(IEnumerable<Row> rows)
        {
            lock (BaseTable)
            {
                BaseTable.Insert(rows);
            }
        }

        /// <inheritdoc />
        public void Update(Row row)
        {
            lock (BaseTable)
            {
                BaseTable.Update(row);
            }
        }

        /// <inheritdoc />
        public void Update(IEnumerable<Row> rows)
        {
            lock (BaseTable)
            {
                BaseTable.Update(rows);
            }
        }

        /// <inheritdoc />
        public void Delete(Row row)
        {
            lock (BaseTable)
            {
                BaseTable.Delete(row);
            }
        }

        /// <inheritdoc />
        public void Delete(IEnumerable<Row> rows)
        {
            lock (BaseTable)
            {
                BaseTable.Delete(rows);
            }
        }

        /// <inheritdoc />
        public int TryDelete(Search search)
        {
            lock (BaseTable)
            {
                return BaseTable.TryDelete(search);
            }
        }

        /// <inheritdoc />
        public void Replace(Row row)
        {
            lock (BaseTable)
            {
                BaseTable.Replace(row);
            }
        }

        /// <inheritdoc />
        public void Replace(IEnumerable<Row> rows)
        {
            lock (BaseTable)
            {
                BaseTable.Replace(rows);
            }
        }

        /// <inheritdoc />
        public Row GetRow(Search search = null, ResultOption resultOption = null)
        {
            lock (BaseTable)
            {
                return BaseTable.GetRow(search, resultOption);
            }
        }

        /// <inheritdoc />
        public IList<Row> GetRows()
        {
            lock (BaseTable)
            {
                return BaseTable.GetRows();
            }
        }

        /// <inheritdoc />
        public IList<Row> GetRows(Search search = null, ResultOption resultOption = null)
        {
            lock (BaseTable)
            {
                return BaseTable.GetRows(search, resultOption);
            }
        }

        /// <inheritdoc />
        public double Sum(string fieldName, Search search = null)
        {
            lock (BaseTable)
            {
                return BaseTable.Sum(fieldName, search);
            }
        }

        /// <inheritdoc />
        public TValue? Maximum<TValue>(string fieldName, Search search = null)
            where TValue : struct, IComparable
        {
            lock (BaseTable)
            {
                return BaseTable.Maximum<TValue>(fieldName, search);
            }
        }

        /// <inheritdoc />
        public TValue? Minimum<TValue>(string fieldName, Search search = null)
            where TValue : struct, IComparable
        {
            lock (BaseTable)
            {
                return BaseTable.Minimum<TValue>(fieldName, search);
            }
        }

        /// <inheritdoc />
        public IList<TValue> Distinct<TValue>(string field, Search search = null)
            where TValue : struct, IComparable
        {
            lock (BaseTable)
            {
                return BaseTable.Distinct<TValue>(field, search);
            }
        }

        /// <inheritdoc />
        public IList<TValue> GetValues<TValue>(string field, Search search = null)
            where TValue : struct, IComparable
        {
            lock (BaseTable)
            {
                return BaseTable.GetValues<TValue>(field, search);
            }
        }

        /// <inheritdoc />
        public int Commit(IEnumerable<Transaction> transactions, TransactionFlags flags = TransactionFlags.Default)
        {
            lock (BaseTable)
            {
                return BaseTable.Commit(transactions, flags);
            }
        }
    }
}
