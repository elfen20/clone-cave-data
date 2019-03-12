using System.Collections.Generic;

namespace Cave.Data
{
    /// <summary>
    /// Provides an asynchronous table writer.
    /// </summary>
    public class TableWriter<T> : TableWriter
        where T : struct
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="TableWriter{T}"/> class.
        /// </summary>
        /// <param name="table">The underlying table.</param>
        public TableWriter(ITable<T> table)
            : this(table, new TransactionLog<T>())
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TableWriter{T}"/> class.
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
            var row = Row.Create(Layout, item);
            var id = Layout.GetID(row);
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
            var row = Row.Create(Layout, item);
            var id = Layout.GetID(row);
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
            var row = Row.Create(Layout, item);
            var id = Layout.GetID(row);
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
