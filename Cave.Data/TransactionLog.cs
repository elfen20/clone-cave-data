using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Cave.Collections.Generic;

namespace Cave.Data
{
    /// <summary>
    /// Provides a transaction log for database rows changes / deletions.
    /// </summary>
    [DebuggerDisplay("{Count} Transactions")]
    public class TransactionLog
    {
        /// <summary>
        /// Provides the syncronization root.
        /// </summary>
        readonly Dictionary<long, LinkedListNode<Transaction>> m_Updated = new Dictionary<long, LinkedListNode<Transaction>>();
        readonly Dictionary<long, LinkedListNode<Transaction>> m_Deleted = new Dictionary<long, LinkedListNode<Transaction>>();
        readonly Set<LinkedListNode<Transaction>> m_Inserted = new Set<LinkedListNode<Transaction>>();
        readonly Dictionary<long, LinkedListNode<Transaction>> m_Replaced = new Dictionary<long, LinkedListNode<Transaction>>();
        readonly LinkedList<Transaction> m_List = new LinkedList<Transaction>();

        /// <summary>
        /// Clears the whole log.
        /// </summary>
        public void Clear()
        {
            lock (this)
            {
                m_Deleted.Clear();
                m_Inserted.Clear();
                m_Replaced.Clear();
                m_Updated.Clear();
                m_List.Clear();
                Monitor.PulseAll(this);
            }
        }

        /// <summary>
        /// Obtains the next used ID at the table (positive values are valid, negative ones are invalid, 0 is not defined!).
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public long GetNextUsedID(long id)
        {
            lock (this)
            {
                if (m_List.Count == 0)
                {
                    return -1;
                }

                long l_First = Math.Max(1, id + 1);
                long best = long.MaxValue;
                foreach (Transaction l_Transaction in m_List)
                {
                    if (l_Transaction.ID > id)
                    {
                        if (l_Transaction.ID == l_First)
                        {
                            return l_First;
                        }

                        best = Math.Min(l_Transaction.ID, best);
                    }
                }
                return best;
            }
        }

        void m_Add(Transaction transaction)
        {
            if (transaction.Type == TransactionType.Inserted)
            {
                m_Inserted.Add(m_List.AddLast(transaction));
                return;
            }

            if (transaction.ID <= 0)
            {
                throw new ArgumentException("Invalid ID!");
            }

            switch (transaction.Type)
            {
                case TransactionType.Updated:
                {
                    // delete older update
                    if (m_Updated.ContainsKey(transaction.ID))
                    {
                        m_List.Remove(m_Updated[transaction.ID]);
                    }
                    m_Updated[transaction.ID] = m_List.AddLast(transaction);
                    return;
                }
                case TransactionType.Deleted:
                    if (m_Deleted.ContainsKey(transaction.ID))
                    {
                        throw new InvalidOperationException("Cannot delete row twice!");
                    }

                    // delete previous update
                    if (m_Updated.ContainsKey(transaction.ID))
                    {
                        m_List.Remove(m_Updated[transaction.ID]);
                        if (!m_Updated.Remove(transaction.ID))
                        {
                            throw new KeyNotFoundException();
                        }
                    }
                    m_Deleted[transaction.ID] = m_List.AddLast(transaction);
                    return;

                case TransactionType.Replaced:
                    // delete previous update
                    if (m_Updated.ContainsKey(transaction.ID))
                    {
                        m_List.Remove(m_Updated[transaction.ID]);
                        if (!m_Updated.Remove(transaction.ID))
                        {
                            throw new KeyNotFoundException();
                        }
                    }

                    // delete previous deletion
                    if (m_Deleted.ContainsKey(transaction.ID))
                    {
                        m_List.Remove(m_Deleted[transaction.ID]);
                        if (!m_Deleted.Remove(transaction.ID))
                        {
                            throw new KeyNotFoundException();
                        }
                    }

                    // delete previous replace
                    if (m_Replaced.ContainsKey(transaction.ID))
                    {
                        m_List.Remove(m_Replaced[transaction.ID]);
                        if (!m_Replaced.Remove(transaction.ID))
                        {
                            throw new KeyNotFoundException();
                        }
                    }
                    m_Replaced[transaction.ID] = m_List.AddLast(transaction);
                    return;

                default: throw new NotImplementedException(string.Format("TransactionType {0} unknown!", transaction.Type));
            }
        }


        /// <summary>
        /// Adds a Transaction to the log.
        /// </summary>
        /// <param name="transaction">The Transaction to add.</param>
        public void Add(Transaction transaction)
        {
            lock (this)
            {
                m_Add(transaction);
                Monitor.PulseAll(this);
            }
        }

        /// <summary>
        /// Adds a Transaction to the log.
        /// </summary>
        /// <param name="transactions">The Transactions to add.</param>
        public void AddRange(Transaction[] transactions)
        {
            if (transactions == null)
            {
                throw new ArgumentNullException("Transactions");
            }

            lock (this)
            {
                foreach (Transaction transaction in transactions)
                {
                    m_Add(transaction);
                }
                Monitor.PulseAll(this);
            }
        }

        /// <summary>
        /// Peeks at the oldest entry.
        /// </summary>
        /// <returns></returns>
        public Transaction Peek()
        {
            lock (this)
            {
                return m_List.Count == 0 ? null : m_List.First.Value;
            }
        }

        /// <summary>Tries to dequeue one transaction.</summary>
        /// <param name="transaction">The transaction.</param>
        /// <returns></returns>
        public bool TryDequeue(out Transaction transaction)
        {
            lock (this)
            {
                if (m_List.Count == 0)
                {
                    transaction = default(Transaction);
                    return false;
                }
                transaction = Dequeue();
                return true;
            }
        }

        /// <summary>
        /// Dequeues the oldest entry.
        /// </summary>
        /// <returns></returns>
        public Transaction Dequeue()
        {
            lock (this)
            {
                LinkedListNode<Transaction> node = m_List.First;
                Transaction result = m_List.First.Value;
                m_List.RemoveFirst();
                switch (result.Type)
                {
                    case TransactionType.Inserted: m_Inserted.Remove(node); break;
                    case TransactionType.Replaced: if (!m_Replaced.Remove(result.ID)) { throw new KeyNotFoundException(); } break;
                    case TransactionType.Updated: if (!m_Updated.Remove(result.ID)) { throw new KeyNotFoundException(); } break;
                    case TransactionType.Deleted: if (!m_Deleted.Remove(result.ID)) { throw new KeyNotFoundException(); } break;
                    default: throw new NotImplementedException(string.Format("TransactionType {0} unknown!", result.Type));
                }
                return result;
            }
        }

        /// <summary>Re enqueues transactions at the beginning of the queue.</summary>
        /// <param name="replaceInserts">if set to <c>true</c> [replace inserts with replace statements].</param>
        /// <param name="transactions">Transactions to reenqueue.</param>
        /// <exception cref="ArgumentNullException">Transactions.</exception>
        public void Requeue(bool replaceInserts, params Transaction[] transactions)
        {
            if (transactions == null)
            {
                throw new ArgumentNullException("Transactions");
            }

            if (transactions.Length == 0)
            {
                return;
            }

            bool warningSent = false;
            lock (this)
            {
                LinkedListNode<Transaction> start = null;
                for (int i = 0; i < transactions.Length; i++)
                {
                    Transaction t = transactions[i];
                    if (replaceInserts && t.Type == TransactionType.Inserted)
                    {
                        if (t.ID > 0)
                        {
                            t = Transaction.Replaced(t.ID, t.Row);
                        }
                        else if (!warningSent)
                        {
                            warningSent = true;
                            Trace.TraceWarning("Requeueing insert transaction at table <red>{0}<default> may result in duplicate datasets with different IDs! This may be caused by a previous commit error!", Layout.Name);
                        }
                    }
                    if (start == null)
                    {
                        start = m_List.AddFirst(t);
                    }
                    else
                    {
                        start = m_List.AddAfter(start, t);
                    }
                }
            }
        }

        /// <summary>
        /// Dequeues the oldest entries (use Count &lt;= 0 to do a fast dequeue of all items).
        /// </summary>
        /// <returns></returns>
        public Transaction[] Dequeue(int count)
        {
            lock (this)
            {
                Transaction[] result;
                if ((count <= 0) || (count > m_List.Count))
                {
                    result = new Transaction[m_List.Count];
                    m_List.CopyTo(result, 0);
                    Clear();
                }
                else
                {
                    result = new Transaction[count];
                    for (int i = 0; i < count; i++)
                    {
                        result[i] = Dequeue();
                    }
                }
                return result;
            }
        }

        /// <summary>Pulses all waiting threads.</summary>
        public void Pulse()
        {
            lock (this)
            {
                Monitor.PulseAll(this);
            }
        }

        /// <summary>Waits until a write event occurs or a timeof of the specified milli seconds elapses.</summary>
        /// <param name="milliSeconds">The milli seconds.</param>
        /// <returns></returns>
        public bool Wait(int milliSeconds)
        {
            lock (this)
            {
                return Monitor.Wait(this, milliSeconds);
            }
        }

        /// <summary>
        /// Obtains the number of entries.
        /// </summary>
        public int Count
        {
            get
            {
                lock (this)
                {
                    return m_List.Count;
                }
            }
        }

        /// <summary>Gets the layout.</summary>
        /// <value>The layout.</value>
        public RowLayout Layout { get; private set; }

        #region code for all implementations

        /// <summary>Froms the difference.</summary>
        /// <param name="source">The source.</param>
        /// <param name="target">The target.</param>
        /// <returns></returns>
        public static TransactionLog FromDifference(ITable source, ITable target)
        {
            if (target == null)
            {
                throw new ArgumentNullException("Target");
            }

            if (source == null)
            {
                throw new ArgumentNullException("Source");
            }

            source.Database.Storage.CheckLayout(source.Layout, target.Layout);
            TransactionLog l = new TransactionLog(source.Layout);
            Set<long> ids = new Set<long>(target.IDs);
            foreach (Row row in source.GetRows())
            {
                long id = source.Layout.GetID(row);
                if (ids.TryRemove(id))
                {
                    l.AddReplaced(id, row);
                }
                else
                {
                    l.AddInserted(id, row);
                }
            }
            foreach (long id in ids)
            {
                l.AddDeleted(id);
            }
            return l;
        }

        /// <summary>
        /// Creates a TransactionLog without accumulation.
        /// </summary>
        public TransactionLog(RowLayout layout)
        {
            Layout = layout;
        }

        /// <summary>Adds a row update to the log.</summary>
        /// <param name="id">The identifier.</param>
        /// <param name="row">The updated row.</param>
        public void AddUpdated(long id, Row row)
        {
            Add(Transaction.Updated(id, row));
        }

        /// <summary>Adds a row insertion to the log.</summary>
        /// <param name="id">The identifier.</param>
        /// <param name="row">The inserted row.</param>
        public void AddInserted(long id, Row row)
        {
            Add(Transaction.Inserted(id, row));
        }

        /// <summary>Adds a row replacement to the log.</summary>
        /// <param name="id">The identifier.</param>
        /// <param name="row">The replacement row.</param>
        public void AddReplaced(long id, Row row)
        {
            Add(Transaction.Replaced(id, row));
        }

        /// <summary>
        /// Adds a row deletion to the log.
        /// </summary>
        /// <param name="id">The ID of the deleted dataset.</param>
        public void AddDeleted(long id)
        {
            Add(Transaction.Deleted(id));
        }

        /// <summary>Returns all transactions present without removing them from the queue.</summary>
        /// <returns>Returns an array with all transactions present.</returns>
        public Transaction[] ToArray()
        {
            return m_List.ToArray();
        }
        #endregion
    }

    /// <summary>
    /// Provides a transaction log for database rows changes / deletions.
    /// </summary>
    public sealed class TransactionLog<T> : TransactionLog
        where T : struct
    {
        /// <summary>Initializes a new instance of the <see cref="TransactionLog{T}"/> class.</summary>
        public TransactionLog() : base(RowLayout.CreateTyped(typeof(T))) { }

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
            Row row = Row.Create(Layout, item);
            long id = Layout.GetID(row);
            AddUpdated(id, row);
        }

        /// <summary>
        /// Adds a row insertion to the log.
        /// </summary>
        /// <param name="item">The inserted dataset.</param>
        public void AddInserted(T item)
        {
            Row row = Row.Create(Layout, item);
            long id = Layout.GetID(row);
            AddInserted(id, row);
        }

        /// <summary>
        /// Adds a row replacement to the log.
        /// </summary>
        /// <param name="item">The replacement dataset.</param>
        public void AddReplaced(T item)
        {
            Row row = Row.Create(Layout, item);
            long id = Layout.GetID(row);
            AddReplaced(id, row);
        }
    }
}
