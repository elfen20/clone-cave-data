using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading;
using Cave.Collections.Generic;

namespace Cave.Data
{
    /// <summary>Provides a base class implementing the <see cref="ITable" /> interface.</summary>
    [DebuggerDisplay("{Name}[{RowCount} Rows]")]
    public abstract class Table : ITable
    {
        int sequenceNumber;

        #region SequenceNumber

        /// <summary>Increases the sequence number.</summary>
        /// <returns>The increased sequence number.</returns>
        public int IncreaseSequenceNumber() => Interlocked.Increment(ref sequenceNumber);

        #endregion

        #region ToString and eXtended Text

        /// <summary>Returns a <see cref="string" /> that represents this instance.</summary>
        /// <returns>A <see cref="string" /> that represents this instance.</returns>
        public override string ToString() => $"Table {Database.Name}.{Name}";

        #endregion

        #region constructor

        #endregion

        #region ITable properties

        /// <inheritdoc />
        public TableFlags Flags { get; private set; }

        /// <inheritdoc />
        public int SequenceNumber => sequenceNumber;

        /// <inheritdoc />
        public IStorage Storage => Database.Storage;

        /// <inheritdoc />
        public IDatabase Database { get; private set; }

        /// <inheritdoc />
        public string Name => Layout.Name;

        /// <inheritdoc />
        public RowLayout Layout { get; private set; }

        /// <inheritdoc />
        public abstract long RowCount { get; }

        #endregion

        #region ITable functions

        /// <inheritdoc />
        public virtual void Connect(IDatabase database, TableFlags flags, RowLayout layout)
        {
            if (Database != null)
            {
                throw new InvalidOperationException("Already initialized!");
            }

            if (IncreaseSequenceNumber() != 1)
            {
                throw new InvalidOperationException("Initialization has to take place directly after creating the class!");
            }

            Flags = flags;
            Database = database ?? throw new ArgumentNullException(nameof(database));
            Layout = layout ?? throw new ArgumentNullException(nameof(layout));
        }

        /// <inheritdoc />
        public virtual void UseLayout(RowLayout layout)
        {
            Storage.CheckLayout(Layout, layout);
            Layout = layout;
        }

        /// <inheritdoc />
        public abstract long Count(Search search = default, ResultOption resultOption = default);

        /// <inheritdoc />
        public abstract void Clear();

        /// <inheritdoc />
        public abstract Row GetRowAt(int index);

        /// <inheritdoc />
        public virtual void SetValue(string fieldName, object value)
        {
            var field = Layout[fieldName];
            if ((field.Flags & FieldFlags.ID) != 0)
            {
                throw new ArgumentException("Identifier fields may can not be updated. Use Delete and Insert!");
            }

            foreach (var row in GetRows())
            {
                row.Values[field.Index] = value;
                Update(row);
            }
        }

        /// <inheritdoc />
        public abstract bool Exist(Search search);

        /// <inheritdoc />
        public abstract bool Exist(Row row);

        /// <inheritdoc />
        public abstract Row Insert(Row row);

        /// <inheritdoc />
        public virtual void Insert(IEnumerable<Row> rows) { Commit(rows.Select(r => Transaction.Insert(r))); }

        /// <inheritdoc />
        public abstract void Update(Row row);

        /// <inheritdoc />
        public virtual void Update(IEnumerable<Row> rows) { Commit(rows.Select(r => Transaction.Updated(r))); }

        /// <inheritdoc />
        public abstract void Delete(Row row);

        /// <inheritdoc />
        public virtual void Delete(IEnumerable<Row> rows) { Commit(rows.Select(r => Transaction.Delete(r))); }

        /// <inheritdoc />
        public abstract int TryDelete(Search search);

        /// <inheritdoc />
        public abstract void Replace(Row row);

        /// <inheritdoc />
        public virtual void Replace(IEnumerable<Row> rows) { Commit(rows.Select(r => Transaction.Replace(r))); }

        /// <inheritdoc />
        public abstract Row GetRow(Search search = default, ResultOption resultOption = default);

        /// <inheritdoc />
        public abstract IList<Row> GetRows();

        /// <inheritdoc />
        public abstract IList<Row> GetRows(Search search = default, ResultOption resultOption = default);

        /// <inheritdoc />
        public virtual double Sum(string fieldName, Search search = null)
        {
            double sum = 0;
            search = search ?? Search.None;
            var fieldNumber = Layout.GetFieldIndex(fieldName, true);
            var field = Layout[fieldNumber];
            switch (field.DataType)
            {
                case DataType.TimeSpan:
                    foreach (var row in GetRows(search))
                    {
                        sum += ((TimeSpan) row[fieldNumber]).TotalSeconds;
                    }

                    break;
                case DataType.Binary:
                case DataType.DateTime:
                case DataType.String:
                case DataType.User:
                case DataType.Unknown:
                    throw new NotSupportedException($"Sum() is not supported for field {field}!");
                default:
                    foreach (var row in GetRows(search))
                    {
                        sum += Convert.ToDouble(row[fieldNumber]);
                    }

                    break;
            }

            return sum;
        }

        /// <inheritdoc />
        public virtual TValue? Maximum<TValue>(string fieldName, Search search = null)
            where TValue : struct, IComparable
        {
            search = search ?? Search.None;
            var fieldNumber = Layout.GetFieldIndex(fieldName, true);
            var field = Layout[fieldNumber];
            var rows = GetRows(search);
            if (!rows.Any())
            {
                return null;
            }

            var current = (TValue) rows.First()[fieldNumber];
            foreach (var row in rows)
            {
                var value = (TValue) row[fieldNumber];
                if (value.CompareTo(current) > 0)
                {
                    current = value;
                }
            }

            return current;
        }

        /// <inheritdoc />
        public virtual TValue? Minimum<TValue>(string fieldName, Search search = null)
            where TValue : struct, IComparable
        {
            search = search ?? Search.None;
            var fieldNumber = Layout.GetFieldIndex(fieldName, true);
            var field = Layout[fieldNumber];
            var rows = GetRows(search);
            if (!rows.Any())
            {
                return null;
            }

            var current = (TValue) rows.First()[fieldNumber];
            foreach (var row in rows)
            {
                var value = (TValue) row[fieldNumber];
                if (value.CompareTo(current) > 0)
                {
                    current = value;
                }
            }

            return current;
        }

        /// <inheritdoc />
        public virtual IList<TValue> Distinct<TValue>(string field, Search search = null)
            where TValue : struct, IComparable
        {
            var index = Layout.GetFieldIndex(field, true);
            var rows = GetRows(search);
            var result = new Set<TValue>();
            foreach (var row in rows)
            {
                var value = (TValue) Fields.ConvertValue(typeof(TValue), row[index], CultureInfo.InvariantCulture);
                result.Include(value);
            }

            return result.AsList();
        }

        /// <inheritdoc />
        public virtual IList<TValue> GetValues<TValue>(string field, Search search = null)
            where TValue : struct, IComparable
        {
            var index = Layout.GetFieldIndex(field, true);
            var rows = GetRows(search);
            var result = new List<TValue>();
            foreach (var row in rows)
            {
                var value = (TValue) Fields.ConvertValue(typeof(TValue), row[index], CultureInfo.InvariantCulture);
                result.Add(value);
            }

            return result.AsList();
        }

        /// <inheritdoc />
        public virtual int Commit(IEnumerable<Transaction> transactions, TransactionFlags flags = TransactionFlags.Default)
        {
            if (transactions == null)
            {
                throw new ArgumentNullException(nameof(transactions));
            }

            var i = 0;
            foreach (var transaction in transactions)
            {
                try
                {
                    switch (transaction.Type)
                    {
                        case TransactionType.Inserted:
                            Insert(transaction.Row);
                            break;
                        case TransactionType.Replaced:
                            Replace(transaction.Row);
                            break;
                        case TransactionType.Updated:
                            Update(transaction.Row);
                            break;
                        case TransactionType.Deleted:
                            Delete(transaction.Row);
                            break;
                        default: throw new NotImplementedException();
                    }

                    i++;
                }
                catch (Exception ex)
                {
                    Trace.TraceWarning("Error committing transaction to table <red>{0}.{1}\n{2}", Database.Name, Name, ex);
                    if ((flags & TransactionFlags.ThrowExceptions) != 0)
                    {
                        throw;
                    }

                    break;
                }
            }

            return i;
        }

        #endregion
    }
}
