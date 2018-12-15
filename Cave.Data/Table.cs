using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Threading;
using Cave.Collections.Generic;

namespace Cave.Data
{
    /// <summary>
    /// Provides a base class implementing the <see cref="ITable"/> interface.
    /// </summary>
    [DebuggerDisplay("{Name}[{RowCount} Rows]")]
    public abstract class Table : ITable
    {
        /// <summary>Creates a memory index for the specified layout.</summary>
        /// <param name="layout">The layout.</param>
        /// <param name="options">The options.</param>
        /// <returns>Returns a field index array or null</returns>
        public static FieldIndex[] CreateIndex(RowLayout layout, MemoryTableOptions options = 0)
        {
            if (0 == (options & MemoryTableOptions.DisableIndex))
            {
                int indexCount = 0;
                FieldIndex[] indices = new FieldIndex[layout.FieldCount];
                for (int i = 0; i < indices.Length; i++)
                {
                    if (i == layout.IDFieldIndex)
                    {
                        continue;
                    }

                    FieldProperties field = layout.GetProperties(i);
                    if ((field.Flags & FieldFlags.Index) != 0)
                    {
                        indices[i] = new FieldIndex();
                        indexCount++;
                    }
                }
                if (indexCount > 0)
                {
                    return indices;
                }
            }
            return null;
        }

        /// <summary>Converts rows to structures</summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="layout">The layout.</param>
        /// <param name="rows">The rows.</param>
        /// <returns>Returns a new <see cref="List{T}"/></returns>
        public static List<T> ToStructs<T>(RowLayout layout, IList<Row> rows) where T : struct
        {
            return ToStructs<T>(layout, rows.Count, rows);
        }

        /// <summary>Converts rows to structures</summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="layout">The layout.</param>
        /// <param name="count">The count.</param>
        /// <param name="rows">The rows.</param>
        /// <returns>Returns a new <see cref="List{T}"/></returns>
        public static List<T> ToStructs<T>(RowLayout layout, int count, IEnumerable<Row> rows) where T : struct
        {
            List<T> result = new List<T>(count);
            foreach (Row row in rows)
            {
                result.Add(row.GetStruct<T>(layout));
            }
            return result;
        }

        /// <summary>Retrieves ids from the specified rows</summary>
        /// <param name="layout">The layout.</param>
        /// <param name="rows">The rows.</param>
        /// <returns>Returns a new <see cref="List{ID}"/></returns>
        public static List<long> ToIDs(RowLayout layout, List<Row> rows)
        {
            List<long> ids = new List<long>(rows.Count);
            foreach (Row row in rows)
            {
                ids.Add(layout.GetID(row));
            }

            return ids;
        }

        /// <summary>
        /// Checks a layout if it is correctly typed.
        /// If it is null the layout will be created. If it is typed it will be returned. Otherwise an <see cref="ArgumentException" /> is thrown.
        /// </summary>
        /// <param name="type">Type of the row struct.</param>
        /// <param name="database">Database to use</param>
        /// <param name="layout">Table layout to use</param>
        /// <returns></returns>
        /// <exception cref="ArgumentException">Layout is not typed!;Layout</exception>
        protected static RowLayout CheckTypedLayout(Type type, IDatabase database, RowLayout layout)
        {
            if (database == null)
            {
                throw new ArgumentNullException("Database");
            }

            if (layout == null)
            {
                return RowLayout.CreateTyped(type, null, database.Storage);
            }
            if (layout.IsTyped)
            {
                return layout;
            }

            throw new ArgumentException("Layout is not typed!", "Layout");
        }

        internal sealed class Sorter : IComparer<long>
        {
            readonly bool m_Descending;
            readonly int m_FieldNumber;
            readonly IDictionary<long, Row> m_Table;

            public Sorter(IDictionary<long, Row> table, int fieldNumber, ResultOptionMode mode)
            {
                if (fieldNumber < 0)
                {
                    throw new ArgumentOutOfRangeException(nameof(fieldNumber));
                }

                m_Table = table ?? throw new ArgumentNullException("Table");
                switch (mode)
                {
                    case ResultOptionMode.SortAsc: m_Descending = false; break;
                    case ResultOptionMode.SortDesc: m_Descending = true; break;
                    default: throw new ArgumentOutOfRangeException(nameof(mode));
                }
                m_FieldNumber = fieldNumber;
            }

            public int Compare(long x, long y)
            {
                object val1 = m_Table[x].GetValue(m_FieldNumber);
                object val2 = m_Table[y].GetValue(m_FieldNumber);
                if (m_Descending)
                {
                    return Comparer.Default.Compare(val2, val1);
                }

                return Comparer.Default.Compare(val1, val2);
            }
        }

        #region constructor
        /// <summary>
        /// Creates a new Table instance
        /// </summary>
        /// <param name="database">The database the </param>
        /// <param name="layout">The layout of the table</param>
        protected Table(IDatabase database, RowLayout layout)
        {
            Database = database ?? throw new ArgumentNullException("Database");
            Layout = layout ?? throw new ArgumentNullException("Layout");
        }
        #endregion

        #region SequenceNumber
        int sequenceNumber;

        /// <summary>Increases the sequence number.</summary>
        public void IncreaseSequenceNumber() { Interlocked.Increment(ref sequenceNumber); }

        /// <summary>Gets the sequence number (counting write commands on this table).</summary>
        /// <value>The sequence number.</value>
        public int SequenceNumber => sequenceNumber;
        #endregion

        #region ITable Member

        #region abstract storage functionality

        /// <summary>
        /// Obtains the RowCount
        /// </summary>
        public abstract long RowCount { get; }

        /// <summary>
        /// Clears all rows of the table.
        /// </summary>
        public abstract void Clear();

        /// <summary>
        /// This function does a lookup on the ids of the table and returns the row with the n-th ID where n is the given index.
        /// Note that indices may change on each update, insert, delete and sorting is not garanteed!
        /// <param name="index">The index of the row to be fetched</param>
        /// </summary>
        /// <returns>Returns the row</returns>
        public abstract Row GetRowAt(int index);

        /// <summary>
        /// Obtains a row from the table
        /// </summary>
        /// <param name="id">The ID of the row to be fetched</param>
        /// <returns>Returns the row</returns>
        public abstract Row GetRow(long id);

        /// <summary>
        /// Checks a given ID for existance
        /// </summary>
        /// <param name="id">The dataset ID to look for</param>
        /// <returns>Returns whether the dataset exists or not</returns>
        public abstract bool Exist(long id);

        /// <summary>
        /// Inserts a row to the table. If an ID <![CDATA[<=]]> 0 is given an automatically generated ID will be used to add the dataset.
        /// </summary>
        /// <param name="row">The row to insert</param>
        /// <returns>Returns the ID of the inserted dataset</returns>
        public abstract long Insert(Row row);

        /// <summary>
        /// Updates a row to the table. The row must exist already!
        /// </summary>
        /// <param name="row">The row to update</param>
        public abstract void Update(Row row);

        /// <summary>
        /// Removes a row from the table.
        /// </summary>
        /// <param name="id">The dataset ID to remove</param>
        public abstract void Delete(long id);

        /// <summary>Removes all rows from the table matching the specified search.</summary>
        /// <param name="search">The Search used to identify rows for removal</param>
        /// <returns>Returns the number of dataset deleted.</returns>
        public abstract int TryDelete(Search search);

        /// <summary>
        /// Obtains the next used ID at the table
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public abstract long GetNextUsedID(long id);

        /// <summary>
        /// Obtains the next free ID at the table
        /// </summary>
        /// <returns></returns>
        public abstract long GetNextFreeID();

        /// <summary>
        /// Searches the table for rows with given field value combinations.
        /// </summary>
        /// <param name="search">The search to run</param>
        /// <param name="resultOption">Options for the search and the result set</param>
        /// <returns>Returns the ID of the row found or -1</returns>
        public abstract List<long> FindRows(Search search = default(Search), ResultOption resultOption = default(ResultOption));

        /// <summary>
        /// Obtains the rows with the given ids
        /// </summary>
        /// <param name="ids">IDs of the rows to fetch from the table</param>
        /// <returns>Returns the rows</returns>
        public abstract List<Row> GetRows(IEnumerable<long> ids);

        /// <summary>
        /// Replaces a row at the table. The ID has to be given. This inserts (if the row does not exist) or updates (if it exists) the row.
        /// </summary>
        /// <param name="row">The row to replace (valid ID needed)</param>
        public abstract void Replace(Row row);

        /// <summary>
        /// Obtains an array with all rows
        /// </summary>
        /// <returns></returns>
        public abstract List<Row> GetRows();
        #endregion

        #region implemented functionality

        /// <summary>Gets all currently used IDs.</summary>
        /// <value>The IDs.</value>
        public virtual List<long> IDs => FindRows(Search.None);

        /// <summary>
        /// Counts the results of a given search
        /// </summary>
        /// <param name="search">The search to run</param>
        /// <param name="resultOption">Options for the search and the result set</param>
        /// <returns>Returns the number of rows found matching the criteria given</returns>
        public virtual long Count(Search search = default(Search), ResultOption resultOption = default(ResultOption))
        {
            return FindRows(search, resultOption).Count;
        }

        /// <summary>Checks a given search for any datasets matching</summary>
        /// <param name="search"></param>
        /// <returns></returns>
        public virtual bool Exist(Search search)
        {
            return Count(search) > 0;
        }

        #region Sum
        /// <summary>Calculates the sum of the specified field name for all matching rows.</summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="search">The search.</param>
        /// <returns></returns>
        public virtual double Sum(string fieldName, Search search = null)
        {
            double sum = 0;
            search = search ?? Search.None;
            int fieldNumber = Layout.GetFieldIndex(fieldName);
            if (fieldNumber < 0)
            {
                throw new ArgumentException("Could not find specified field!");
            }

            FieldProperties field = Layout.GetProperties(fieldNumber);
            switch (field.DataType)
            {
                case DataType.TimeSpan:
                    foreach (Row row in GetRows(search))
                    {
                        sum += Convert.ToDouble(((TimeSpan)row.GetValue(fieldNumber)).Ticks);
                    }
                    break;
                case DataType.Binary:
                case DataType.DateTime:
                case DataType.String:
                case DataType.User:
                case DataType.Unknown:
                    throw new NotSupportedException($"Sum() is not supported for field {field}!");
                default:
                    foreach (Row row in GetRows(search))
                    {
                        sum += Convert.ToDouble(row.GetValue(fieldNumber));
                    }
                    break;
            }
            return sum;
        }
        #endregion

        /// <summary>
        /// Obtains the underlying storage engine
        /// </summary>
        public IStorage Storage => Database.Storage;

        /// <summary>
        /// Obtains the database instance with table belongs to
        /// </summary>
        public IDatabase Database { get; }

        /// <summary>
        /// Obtains the name of the table
        /// </summary>
        public string Name => Layout.Name;

        /// <summary>
        /// Obtains the RowLayout of the table
        /// </summary>
        public RowLayout Layout { get; }

        /// <summary>
        /// Searches the table for a row with given field value combinations.
        /// </summary>
        /// <param name="search">The search to run</param>
        /// <param name="resultOption">Options for the search and the result set</param>
        /// <returns>Returns the ID of the row found or -1</returns>
        public long FindRow(Search search = default(Search), ResultOption resultOption = default(ResultOption))
        {
            IList<long> result = FindRows(search, resultOption);
            if (result.Count > 1)
            {
                throw new InvalidDataException(string.Format("Search {0} returned multiple results!", search));
            }

            if (result.Count == 0)
            {
                return -1;
            }

            return result[0];
        }

        /// <summary>
        /// Searches the table for a single row with given search.
        /// </summary>
        /// <param name="search">The search to run</param>
        /// <param name="resultOption">Options for the search and the result set</param>
        /// <returns>Returns the row found</returns>
        public virtual Row GetRow(Search search = default(Search), ResultOption resultOption = default(ResultOption))
        {
            long id = FindRow(search, resultOption);
            if (id <= 0)
            {
                throw new DataException(string.Format("Dataset could not be found!"));
            }

            return GetRow(id);
        }

        /// <summary>
        /// Searches the table for rows with given field value combinations.
        /// </summary>
        /// <param name="search">The search to run</param>
        /// <param name="resultOption">Options for the search and the result set</param>
        /// <returns>Returns the rows found</returns>
        public virtual List<Row> GetRows(Search search = default(Search), ResultOption resultOption = default(ResultOption))
        {
            List<long> ids = FindRows(search, resultOption);
            return GetRows(ids);
        }

        /// <summary>Obtains all different field values of a given field</summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="field">The field.</param>
        /// <param name="includeNull">allow null value to be added to the results</param>
        /// <param name="ids">The ids to check or null for any.</param>
        /// <returns></returns>
        public virtual IItemSet<T> GetValues<T>(string field, bool includeNull = false, IEnumerable<long> ids = null)
        {
            int i = Layout.GetFieldIndex(field);
            if (ids == null)
            {
                ids = FindRows(Search.None, ResultOption.Group(field));
            }
            Set<T> result = new Set<T>();
            foreach (long id in ids)
            {
                T value = (T)Fields.ConvertValue(typeof(T), GetRow(id).GetValue(i), CultureInfo.InvariantCulture);
                if (includeNull || (value != null))
                {
                    result.Add(value);
                }
            }
            return result;
        }

        /// <summary>
        /// Obtains the field count
        /// </summary>
        public int FieldCount => Layout.FieldCount;

        /// <summary>
        /// Sets the specified value to the specified fieldname on all rows
        /// </summary>
        /// <param name="field">The fields name</param>
        /// <param name="value">The value to set</param>
        public virtual void SetValue(string field, object value)
        {
            int index = Layout.GetFieldIndex(field);
            if (index == Layout.IDFieldIndex)
            {
                throw new ArgumentException(string.Format("FieldName may not be the ID fieldname!"));
            }

            foreach (Row row in GetRows())
            {
                object[] r = row.GetValues();
                r[index] = value;
                Update(new Row(r));
            }
        }

        /// <summary>
        /// Copies all rows to a given array
        /// </summary>
        /// <param name="rowArray"></param>
        /// <param name="startIndex"></param>
        public void CopyTo(Row[] rowArray, int startIndex)
        {
            if (rowArray == null)
            {
                throw new ArgumentNullException("RowArray");
            }

            ICollection<Row> items = GetRows();
            items.CopyTo(rowArray, startIndex);
        }

        /// <summary>Commits a whole TransactionLog to the table</summary>
        /// <param name="transactions">The transaction log to read</param>
        /// <param name="flags">The flags to use.</param>
        /// <param name="count">Number of transactions to combine at one write</param>
        /// <returns>Returns the number of transactions done or -1 if unknown</returns>
        public virtual int Commit(TransactionLog transactions, TransactionFlags flags = TransactionFlags.Default, int count = -1)
        {
            if (transactions == null)
            {
                throw new ArgumentNullException("TransactionLog");
            }

            int i = 0;
            count = (count > 0) ? Math.Min(transactions.Count, count) : 1000;
            Trace.TraceInformation("Commiting transaction with {0} rows", count);
            while (true)
            {
                if (!transactions.TryDequeue(out Transaction transaction))
                {
                    break;
                }

                try
                {
                    switch (transaction.Type)
                    {
                        case TransactionType.Inserted: Insert(transaction.Row); break;
                        case TransactionType.Replaced: Replace(transaction.Row); break;
                        case TransactionType.Updated: Update(transaction.Row); break;
                        case TransactionType.Deleted: Delete(transaction.ID); break;
                        default: throw new NotImplementedException();
                    }
                    i++;
                }
                catch (Exception ex)
                {
                    Trace.TraceWarning("Error committing transaction to table <red>{0}.{1}\n{2}", Database.Name, Name, ex);
                    if (0 != (flags & TransactionFlags.AllowRequeue))
                    {
                        transactions.Requeue(true, transaction);
                    }

                    if (0 != (flags & TransactionFlags.ThrowExceptions))
                    {
                        throw;
                    }

                    break;
                }
            }
            return i;
        }

        /// <summary>
        /// Inserts rows into the table using a transaction. 
        /// </summary>
        /// <param name="rows">The rows to insert.</param>
        public void Insert(IEnumerable<Row> rows)
        {
            if (!Storage.SupportsNativeTransactions)
            {
                foreach (Row r in rows) { Insert(r); }
            }
            else
            {
                if (rows == null)
                {
                    throw new ArgumentNullException("Rows");
                }

                TransactionLog l = new TransactionLog(Layout);
                foreach (Row r in rows)
                {
                    long id = Layout.GetID(r);
                    l.AddInserted(id, r);
                }
                Commit(l);
            }
        }

        /// <summary>
        /// Updates rows at the table. The rows must exist already!
        /// </summary>
        /// <param name="rows">The rows to update</param>
        public void Update(IEnumerable<Row> rows)
        {
            if (!Storage.SupportsNativeTransactions)
            {
                foreach (Row r in rows) { Update(r); }
            }
            else
            {
                if (rows == null)
                {
                    throw new ArgumentNullException("Rows");
                }

                TransactionLog log = new TransactionLog(Layout);
                foreach (Row row in rows)
                {
                    long id = Layout.GetID(row);
                    log.AddUpdated(id, row);
                }
                Commit(log);
            }
        }

        /// <summary>
        /// Replaces rows at the table using a transaction. 
        /// </summary>
        /// <param name="rows">The replacement rows.</param>
        public void Replace(IEnumerable<Row> rows)
        {
            if (!Storage.SupportsNativeTransactions)
            {
                foreach (Row r in rows) { Replace(r); }
            }
            else
            {
                if (rows == null)
                {
                    throw new ArgumentNullException("Rows");
                }

                TransactionLog log = new TransactionLog(Layout);
                foreach (Row row in rows)
                {
                    long id = Layout.GetID(row);
                    log.AddReplaced(id, row);
                }
                Commit(log);
            }
        }

        /// <summary>Removes rows from the table using a transaction.</summary>
        /// <param name="ids">The dataset IDs to remove</param>
        /// <exception cref="ArgumentNullException">ids</exception>
        public void Delete(IEnumerable<long> ids)
        {
            if (!Storage.SupportsNativeTransactions)
            {
                foreach (long id in ids) { Delete(id); }
            }
            else
            {
                if (ids == null)
                {
                    throw new ArgumentNullException("ids");
                }

                TransactionLog l = new TransactionLog(Layout);
                foreach (long id in ids) { l.AddDeleted(id); }
                Commit(l);
            }
        }

        #endregion

        #region ToString and eXtended Text        
        /// <summary>Returns a <see cref="string" /> that represents this instance.</summary>
        /// <returns>A <see cref="string" /> that represents this instance.</returns>
        public override string ToString()
        {
            return $"Table {Database.Name}.{Name}";
        }
        #endregion

        #endregion
    }

    /// <summary>
    /// Provides a base class implementing the <see cref="ITable{T}"/> interface.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public abstract class Table<T> : Table, ITable<T> where T : struct
    {
        #region constructor
        /// <summary>
        /// Creates a new Table instance
        /// </summary>
        /// <param name="database">The database the </param>
        /// <param name="layout">Layout and name of the table (optional, will use the typed layout and name if not set)</param>
        protected Table(IDatabase database, RowLayout layout)
            : base(database, CheckTypedLayout(typeof(T), database, layout))
        {
        }
        #endregion

        #region ITable<T> Member

        #region implemented non virtual

        /// <summary>Inserts rows into the table using a transaction.</summary>
        /// <param name="rows">The rows to insert.</param>
        /// <exception cref="ArgumentNullException">Rows</exception>
        public void Insert(IEnumerable<T> rows)
        {
            if (!Storage.SupportsNativeTransactions)
            {
                foreach (T r in rows) { Insert(r); }
            }
            else
            {
                if (rows == null)
                {
                    throw new ArgumentNullException("Rows");
                }

                TransactionLog<T> l = new TransactionLog<T>();
                foreach (T r in rows) { l.AddInserted(r); }
                Commit(l);
            }
        }

        /// <summary>Updates rows at the table. The rows must exist already!</summary>
        /// <param name="rows">The rows to update</param>
        /// <exception cref="ArgumentNullException">Rows</exception>
        public void Update(IEnumerable<T> rows)
        {
            if (!Storage.SupportsNativeTransactions)
            {
                foreach (T r in rows) { Update(r); }
            }
            else
            {
                if (rows == null)
                {
                    throw new ArgumentNullException("Rows");
                }

                TransactionLog<T> l = new TransactionLog<T>();
                foreach (T r in rows) { l.AddUpdated(r); }
                Commit(l);
            }
        }

        /// <summary>
        /// Replaces rows at the table. This inserts (if the row does not exist) or updates (if it exists) each row.
        /// </summary>
        /// <param name="rows">The rows to replace (valid ID needed)</param>
        /// <exception cref="ArgumentNullException">Rows</exception>
        public void Replace(IEnumerable<T> rows)
        {
            if (!Storage.SupportsNativeTransactions)
            {
                foreach (T r in rows) { Replace(r); }
            }
            else
            {
                if (rows == null)
                {
                    throw new ArgumentNullException("Rows");
                }

                TransactionLog<T> l = new TransactionLog<T>();
                foreach (T i in rows) { l.AddReplaced(i); }
                Commit(l);
            }
        }
        #endregion

        #region implemented virtual
        /// <summary>
        /// Obtains a row from the table
        /// </summary>
        /// <param name="id">The ID of the row to be fetched</param>
        /// <returns>Returns the row</returns>
        public abstract T GetStruct(long id);

        /// <summary>
        /// Searches the table for a single row with given search.
        /// </summary>
        /// <param name="search">The search to run</param>
        /// <param name="resultOption">Options for the search and the result set</param>
        /// <returns>Returns the row found</returns>
        public virtual T GetStruct(Search search = default(Search), ResultOption resultOption = default(ResultOption))
        {
            long id = FindRow(search, resultOption);
            if (id <= 0)
            {
                throw new DataException(string.Format("Dataset could not be found!"));
            }

            return GetStruct(id);
        }

        /// <summary>
        /// Obtains the row struct with the given index.
        /// This allows a memorytable to be used as virtual list for listviews, ...
        /// Note that indices may change on each update, insert, delete and sorting is not garanteed!
        /// </summary>
        /// <param name="index">The rows index (0..RowCount-1)</param>
        /// <returns></returns>
        /// <exception cref="IndexOutOfRangeException"></exception>
        public abstract T GetStructAt(int index);

        /// <summary>
        /// Searches the table for rows with given field value combinations.
        /// </summary>
        /// <param name="search">The search to run</param>
        /// <param name="resultOption">Options for the search and the result set</param>
        /// <returns>Returns the rows found</returns>
        public virtual List<T> GetStructs(Search search = default(Search), ResultOption resultOption = default(ResultOption))
        {
            List<long> ids = FindRows(search, resultOption);
            return GetStructs(ids);
        }

        /// <summary>
        /// Obtains the rows with the given ids
        /// </summary>
        /// <param name="ids">IDs of the rows to fetch from the table</param>
        /// <returns>Returns the rows</returns>
        public abstract List<T> GetStructs(IEnumerable<long> ids);

        /// <summary>
        /// Inserts a row to the table. If an ID <![CDATA[<=]]> 0 is given an automatically generated ID will be used to add the dataset.
        /// </summary>
        /// <param name="row">The row to insert</param>
        /// <returns>Returns the ID of the inserted dataset</returns>
        public virtual long Insert(T row)
        {
            return Insert(Row.Create(Layout, row));
        }

        /// <summary>
        /// Updates a row to the table. The row must exist already!
        /// </summary>
        /// <param name="row">The row to update</param>
        public virtual void Update(T row)
        {
            Update(Row.Create(Layout, row));
        }

        /// <summary>
        /// Replaces a row at the table. The ID has to be given. This inserts (if the row does not exist) or updates (if it exists) the row.
        /// </summary>
        /// <param name="row">The row to replace (valid ID needed)</param>
        public virtual void Replace(T row)
        {
            Replace(Row.Create(Layout, row));
        }

        /// <summary>
        /// Checks whether a row is present unchanged at the database and removes it.
        /// (Use Delete(ID) to delete a DataSet without any checks)
        /// </summary>
        /// <param name="row"></param>
        public virtual void Delete(T row)
        {
            long id = Layout.GetID(row);
            T data = GetStruct(id);
            if (!data.Equals(row))
            {
                throw new DataException(string.Format("Dataset could not be found!"));
            }

            Delete(id);
        }

        /// <summary>
        /// Provides access to the row with the specified ID
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public virtual T this[long id] => GetStruct(id);

        /// <summary>
        /// Copies all rows to a given array
        /// </summary>
        /// <param name="rowArray"></param>
        /// <param name="startIndex"></param>
        public void CopyTo(T[] rowArray, int startIndex)
        {
            IList<T> items = GetStructs();
            items.CopyTo(rowArray, startIndex);
        }

        #endregion

        #endregion
    }
}
