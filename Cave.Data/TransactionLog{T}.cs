namespace Cave.Data
{
    /// <summary>
    /// Provides a transaction log for database rows changes / deletions.
    /// </summary>
    public sealed class TransactionLog<T> : TransactionLog
        where T : struct
    {
        /// <summary>Initializes a new instance of the <see cref="TransactionLog{T}"/> class.</summary>
        public TransactionLog()
            : base(RowLayout.CreateTyped(typeof(T)))
        {
        }

        /// <summary>
        /// Adds a row deletion to the log.
        /// </summary>
        /// <param name="row">The deleted dataset.</param>
        public void AddDeleted(T row)
        {
            AddDeleted(Layout.GetID(row));
        }

        /// <summary>
        /// Adds a row update to the log.
        /// </summary>
        /// <param name="item">The updated dataset.</param>
        public void AddUpdated(T item)
        {
            var row = Row.Create(Layout, item);
            var id = Layout.GetID(row);
            AddUpdated(id, row);
        }

        /// <summary>
        /// Adds a row insertion to the log.
        /// </summary>
        /// <param name="item">The inserted dataset.</param>
        public void AddInserted(T item)
        {
            var row = Row.Create(Layout, item);
            var id = Layout.GetID(row);
            AddInserted(id, row);
        }

        /// <summary>
        /// Adds a row replacement to the log.
        /// </summary>
        /// <param name="item">The replacement dataset.</param>
        public void AddReplaced(T item)
        {
            var row = Row.Create(Layout, item);
            var id = Layout.GetID(row);
            AddReplaced(id, row);
        }
    }
}
