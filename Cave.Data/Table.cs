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
        /// <returns>Returns a field index array or null.</returns>
        public static FieldIndex[] CreateIndex(RowLayout layout, MemoryTableOptions options = 0)
        {
            if ((options & MemoryTableOptions.DisableIndex) == 0)
            {
                var indexCount = 0;
                var indices = new FieldIndex[layout.FieldCount];
                for (var i = 0; i < indices.Length; i++)
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

        /// <summary>Converts rows to structures.</summary>
        /// <typeparam name="T">Structure type.</typeparam>
        /// <param name="layout">The layout.</param>
        /// <param name="rows">The rows.</param>
        /// <returns>Returns a new <see cref="List{T}"/>.</returns>
        public static List<T> ToStructs<T>(RowLayout layout, IList<Row> rows)
            where T : struct
        {
            return ToStructs<T>(layout, rows.Count, rows);
        }

        /// <summary>Converts rows to structures.</summary>
        /// <typeparam name="T">Structure type.</typeparam>
        /// <param name="layout">The layout.</param>
        /// <param name="count">The count.</param>
        /// <param name="rows">The rows.</param>
        /// <returns>Returns a new <see cref="List{T}"/>.</returns>
        public static List<T> ToStructs<T>(RowLayout layout, int count, IEnumerable<Row> rows)
            where T : struct
        {
            var result = new List<T>(count);
            foreach (Row row in rows)
            {
                result.Add(row.GetStruct<T>(layout));
            }
            return result;
        }

        /// <summary>Retrieves ids from the specified rows.</summary>
        /// <param name="layout">The layout.</param>
        /// <param name="rows">The rows.</param>
        /// <returns>Returns a new <see cref="List{ID}"/>.</returns>
        public static List<long> ToIDs(RowLayout layout, List<Row> rows)
        {
            var ids = new List<long>(rows.Count);
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
        /// <param name="database">Database to use.</param>
        /// <param name="layout">Table layout to use.</param>
        /// <returns></returns>
        /// <exception cref="ArgumentException">Layout is not typed!;Layout.</exception>
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

        #region constructor

        /// <summary>
        /// Initializes a new instance of the <see cref="Table"/> class.
        /// </summary>
        /// <param name="database">The database the. </param>
        /// <param name="layout">The layout of the table.</param>
        protected Table(IDatabase database, RowLayout layout)
        {
            Database = database ?? throw new ArgumentNullException("Database");
            Layout = layout ?? throw new ArgumentNullException("Layout");
        }
        #endregion

        #region SequenceNumber
        int sequenceNumber;

        /// <summary>Increases the sequence number.</summary>
        public void IncreaseSequenceNumber()
        {
            Interlocked.Increment(ref sequenceNumber);
        }

        /// <summary>Gets the sequence number (counting write commands on this table).</summary>
        /// <value>The sequence number.</value>
        public int SequenceNumber => sequenceNumber;
        #endregion

        #region ITable Member

        #region abstract storage functionality

        /// <summary>
        /// Gets the RowCount.
        /// </summary>
        public abstract long RowCount { get; }

        /// <summary>
        /// Clears all rows of the table.
        /// </summary>
        public abstract void Clear();

        /// <summary>
        /// This function does a lookup on the ids of the table and returns the row with the n-th ID where n is the given index.
        /// Note that indices may change on each update, insert, delete and sorting is not garanteed!.
        /// <param name="index">The index of the row to be fetched</param>
        /// </summary>
        /// <returns>Returns the row.</returns>
        public abstract Row GetRowAt(int index);

        /// <summary>
        /// Gets a row from the table.
        /// </summary>
        /// <param name="id">The ID of the row to be fetched.</param>
        /// <returns>Returns the row.</returns>
        public abstract Row GetRow(long id);

        /// <summary>
        /// Checks a given ID for existance.
        /// </summary>
        /// <param name="id">The dataset ID to look for.</param>
        /// <returns>Returns whether the dataset exists or not.</returns>
        public abstract bool Exist(long id);

        /// <summary>
        /// Inserts a row to the table. If an ID <![CDATA[<=]]> 0 is given an automatically generated ID will be used to add the dataset.
        /// </summary>
        /// <param name="row">The row to insert.</param>
        /// <returns>Returns the ID of the inserted dataset.</returns>
        public abstract long Insert(Row row);

        /// <summary>
        /// Updates a row to the table. The row must exist already!.
        /// </summary>
        /// <param name="row">The row to update.</param>
        public abstract void Update(Row row);

        /// <summary>
        /// Removes a row from the table.
        /// </summary>
        /// <param name="id">The dataset ID to remove.</param>
        public abstract void Delete(long id);

        /// <summary>Removes all rows from the table matching the specified search.</summary>
        /// <param name="search">The Search used to identify rows for removal.</param>
        /// <returns>Returns the number of dataset deleted.</returns>
        public abstract int TryDelete(Search search);

        /// <summary>
        /// Gets the next used ID at the table.
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public abstract long GetNextUsedID(long id);

        /// <summary>
        /// Gets the next free ID at the table.
        /// </summary>
        /// <returns></returns>
        public abstract long GetNextFreeID();

        /// <summary>
        /// Searches the table for rows with given field value combinations.
        /// </summary>
        /// <param name="search">The search to run.</param>
        /// <param name="resultOption">Options for the search and the result set.</param>
        /// <returns>Returns the ID of the row found or -1.</returns>
        public abstract IList<long> FindRows(Search search = default(Search), ResultOption resultOption = default(ResultOption));

        /// <summary>
        /// Gets the rows with the given ids.
        /// </summary>
        /// <param name="ids">IDs of the rows to fetch from the table.</param>
        /// <returns>Returns the rows.</returns>
        public abstract IList<Row> GetRows(IEnumerable<long> ids);

        /// <summary>
        /// Replaces a row at the table. The ID has to be given. This inserts (if the row does not exist) or updates (if it exists) the row.
        /// </summary>
        /// <param name="row">The row to replace (valid ID needed).</param>
        public abstract void Replace(Row row);

        /// <summary>
        /// Gets an array with all rows.
        /// </summary>
        /// <returns></returns>
        public abstract IList<Row> GetRows();
        #endregion

        #region implemented functionality

        #region IDs

        /// <inheritdoc/>
        public virtual IList<long> IDs => FindRows(Search.None, ResultOption.SortAscending(Layout.IDField.Name));

        /// <inheritdoc/>
        public virtual IList<long> SortedIDs => FindRows();

        #endregion

        /// <summary>
        /// Counts the results of a given search.
        /// </summary>
        /// <param name="search">The search to run.</param>
        /// <param name="resultOption">Options for the search and the result set.</param>
        /// <returns>Returns the number of rows found matching the criteria given.</returns>
        public virtual long Count(Search search = default(Search), ResultOption resultOption = default(ResultOption))
        {
            return FindRows(search, resultOption).Count;
        }

        /// <summary>Checks a given search for any datasets matching.</summary>
        /// <param name="search"></param>
        /// <returns></returns>
        public virtual bool Exist(Search search)
        {
            return Count(search) > 0;
        }

        #region Sum

        /// <summary>Calculates the sum of the specified field name for all matching rows.</summary>
        /// <remarks>For TimeSpan fields, the result is the number of seconds.</remarks>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="search">The search.</param>
        /// <returns></returns>
        public virtual double Sum(string fieldName, Search search = null)
        {
            double sum = 0;
            search = search ?? Search.None;
            var fieldNumber = Layout.GetFieldIndex(fieldName);
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
                        sum += ((TimeSpan)row.GetValue(fieldNumber)).TotalSeconds;
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
        /// Gets the underlying storage engine.
        /// </summary>
        public IStorage Storage => Database.Storage;

        /// <summary>
        /// Gets the database instance with table belongs to.
        /// </summary>
        public IDatabase Database { get; }

        /// <summary>
        /// Gets the name of the table.
        /// </summary>
        public string Name => Layout.Name;

        /// <summary>
        /// Gets the RowLayout of the table.
        /// </summary>
        public RowLayout Layout { get; }

        /// <summary>
        /// Searches the table for a row with given field value combinations.
        /// </summary>
        /// <param name="search">The search to run.</param>
        /// <param name="resultOption">Options for the search and the result set.</param>
        /// <returns>Returns the ID of the row found or -1.</returns>
        public long FindRow(Search search = default(Search), ResultOption resultOption = default(ResultOption))
        {
            IList<long> result = FindRows(search, resultOption);
            if (result.Count > 1)
            {
                throw new InvalidDataException(string.Format("Search {0} returned multiple results!", search));
            }

            return result.Count == 0 ? -1 : result[0];
        }

        /// <summary>
        /// Searches the table for a single row with given search.
        /// </summary>
        /// <param name="search">The search to run.</param>
        /// <param name="resultOption">Options for the search and the result set.</param>
        /// <returns>Returns the row found.</returns>
        public virtual Row GetRow(Search search = default(Search), ResultOption resultOption = default(ResultOption))
        {
            var id = FindRow(search, resultOption);
            if (id <= 0)
            {
                throw new DataException(string.Format("Dataset could not be found!"));
            }

            return GetRow(id);
        }

        /// <summary>
        /// Searches the table for rows with given field value combinations.
        /// </summary>
        /// <param name="search">The search to run.</param>
        /// <param name="resultOption">Options for the search and the result set.</param>
        /// <returns>Returns the rows found.</returns>
        public virtual IList<Row> GetRows(Search search = default(Search), ResultOption resultOption = default(ResultOption))
        {
            var ids = FindRows(search, resultOption);
            return GetRows(ids);
        }

        /// <summary>Obtains all different field values of a given field.</summary>
        /// <typeparam name="T">Structure type.</typeparam>
        /// <param name="field">The field.</param>
        /// <param name="includeNull">allow null value to be added to the results.</param>
        /// <param name="ids">The ids to check or null for any.</param>
        /// <returns></returns>
        public virtual IItemSet<T> GetValues<T>(string field, bool includeNull = false, IEnumerable<long> ids = null)
        {
            var i = Layout.GetFieldIndex(field);
            if (ids == null)
            {
                ids = FindRows(Search.None, ResultOption.Group(field));
            }
            var result = new Set<T>();
            foreach (var id in ids)
            {
                var value = (T)Fields.ConvertValue(typeof(T), GetRow(id).GetValue(i), CultureInfo.InvariantCulture);
                if (includeNull || (value != null))
                {
                    result.Add(value);
                }
            }
            return result;
        }

        /// <summary>
        /// Gets the field count.
        /// </summary>
        public int FieldCount => Layout.FieldCount;

        /// <summary>
        /// Sets the specified value to the specified fieldname on all rows.
        /// </summary>
        /// <param name="field">The fields name.</param>
        /// <param name="value">The value to set.</param>
        public virtual void SetValue(string field, object value)
        {
            var index = Layout.GetFieldIndex(field);
            if (index == Layout.IDFieldIndex)
            {
                throw new ArgumentException(string.Format("FieldName may not be the ID fieldname!"));
            }

            foreach (Row row in GetRows())
            {
                var r = row.GetValues();
                r[index] = value;
                Update(new Row(r));
            }
        }

        /// <summary>
        /// Copies all rows to a given array.
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

        /// <summary>Commits a whole TransactionLog to the table.</summary>
        /// <param name="transactions">The transaction log to read.</param>
        /// <param name="flags">The flags to use.</param>
        /// <param name="count">Number of transactions to combine at one write.</param>
        /// <returns>Returns the number of transactions done or -1 if unknown.</returns>
        public virtual int Commit(TransactionLog transactions, TransactionFlags flags = TransactionFlags.Default, int count = -1)
        {
            if (transactions == null)
            {
                throw new ArgumentNullException("TransactionLog");
            }

            var i = 0;
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
                    if ((flags & TransactionFlags.AllowRequeue) != 0)
                    {
                        transactions.Requeue(true, transaction);
                    }

                    if ((flags & TransactionFlags.ThrowExceptions) != 0)
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
                foreach (Row r in rows)
                {
                    Insert(r);
                }
            }
            else
            {
                if (rows == null)
                {
                    throw new ArgumentNullException("Rows");
                }

                var l = new TransactionLog(Layout);
                foreach (Row r in rows)
                {
                    var id = Layout.GetID(r);
                    l.AddInserted(id, r);
                }
                Commit(l);
            }
        }

        /// <summary>
        /// Updates rows at the table. The rows must exist already!.
        /// </summary>
        /// <param name="rows">The rows to update.</param>
        public void Update(IEnumerable<Row> rows)
        {
            if (!Storage.SupportsNativeTransactions)
            {
                foreach (Row r in rows)
                {
                    Update(r);
                }
            }
            else
            {
                if (rows == null)
                {
                    throw new ArgumentNullException("Rows");
                }

                var log = new TransactionLog(Layout);
                foreach (Row row in rows)
                {
                    var id = Layout.GetID(row);
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
                foreach (Row r in rows)
                {
                    Replace(r);
                }
            }
            else
            {
                if (rows == null)
                {
                    throw new ArgumentNullException("Rows");
                }

                var log = new TransactionLog(Layout);
                foreach (Row row in rows)
                {
                    var id = Layout.GetID(row);
                    log.AddReplaced(id, row);
                }
                Commit(log);
            }
        }

        /// <summary>Removes rows from the table using a transaction.</summary>
        /// <param name="ids">The dataset IDs to remove.</param>
        /// <exception cref="ArgumentNullException">ids.</exception>
        public void Delete(IEnumerable<long> ids)
        {
            if (!Storage.SupportsNativeTransactions)
            {
                foreach (var id in ids)
                {
                    Delete(id);
                }
            }
            else
            {
                if (ids == null)
                {
                    throw new ArgumentNullException("ids");
                }

                var l = new TransactionLog(Layout);
                foreach (var id in ids)
                {
                    l.AddDeleted(id);
                }
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
}
