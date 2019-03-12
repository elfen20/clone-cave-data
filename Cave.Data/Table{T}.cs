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
    /// Provides a base class implementing the <see cref="ITable{T}"/> interface.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public abstract class Table<T> : Table, ITable<T>
        where T : struct
    {
        #region constructor

        /// <summary>
        /// Creates a new Table instance.
        /// </summary>
        /// <param name="database">The database the. </param>
        /// <param name="layout">Layout and name of the table (optional, will use the typed layout and name if not set).</param>
        protected Table(IDatabase database, RowLayout layout)
            : base(database, CheckTypedLayout(typeof(T), database, layout))
        {
        }
        #endregion

        #region ITable<T> Member

        #region implemented non virtual

        /// <summary>Inserts rows into the table using a transaction.</summary>
        /// <param name="rows">The rows to insert.</param>
        /// <exception cref="ArgumentNullException">Rows.</exception>
        public void Insert(IEnumerable<T> rows)
        {
            if (!Storage.SupportsNativeTransactions)
            {
                foreach (T r in rows)
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

                var l = new TransactionLog<T>();
                foreach (T r in rows)
                {
                    l.AddInserted(r);
                }
                Commit(l);
            }
        }

        /// <summary>Updates rows at the table. The rows must exist already!.</summary>
        /// <param name="rows">The rows to update.</param>
        /// <exception cref="ArgumentNullException">Rows.</exception>
        public void Update(IEnumerable<T> rows)
        {
            if (!Storage.SupportsNativeTransactions)
            {
                foreach (T r in rows)
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

                var l = new TransactionLog<T>();
                foreach (T r in rows)
                {
                    l.AddUpdated(r);
                }
                Commit(l);
            }
        }

        /// <summary>
        /// Replaces rows at the table. This inserts (if the row does not exist) or updates (if it exists) each row.
        /// </summary>
        /// <param name="rows">The rows to replace (valid ID needed).</param>
        /// <exception cref="ArgumentNullException">Rows.</exception>
        public void Replace(IEnumerable<T> rows)
        {
            if (!Storage.SupportsNativeTransactions)
            {
                foreach (T r in rows)
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

                var l = new TransactionLog<T>();
                foreach (T i in rows)
                {
                    l.AddReplaced(i);
                }
                Commit(l);
            }
        }
        #endregion

        #region implemented virtual

        /// <summary>
        /// Gets a row from the table.
        /// </summary>
        /// <param name="id">The ID of the row to be fetched.</param>
        /// <returns>Returns the row.</returns>
        public abstract T GetStruct(long id);

        /// <summary>
        /// Searches the table for a single row with given search.
        /// </summary>
        /// <param name="search">The search to run.</param>
        /// <param name="resultOption">Options for the search and the result set.</param>
        /// <returns>Returns the row found.</returns>
        public virtual T GetStruct(Search search = default(Search), ResultOption resultOption = default(ResultOption))
        {
            var id = FindRow(search, resultOption);
            if (id <= 0)
            {
                throw new DataException(string.Format("Dataset could not be found!"));
            }

            return GetStruct(id);
        }

        /// <summary>
        /// Gets the row struct with the given index.
        /// This allows a memorytable to be used as virtual list for listviews, ...
        /// Note that indices may change on each update, insert, delete and sorting is not garanteed!.
        /// </summary>
        /// <param name="index">The rows index (0..RowCount-1).</param>
        /// <returns></returns>
        /// <exception cref="IndexOutOfRangeException"></exception>
        public abstract T GetStructAt(int index);

        /// <summary>
        /// Searches the table for rows with given field value combinations.
        /// </summary>
        /// <param name="search">The search to run.</param>
        /// <param name="resultOption">Options for the search and the result set.</param>
        /// <returns>Returns the rows found.</returns>
        public virtual IList<T> GetStructs(Search search = default(Search), ResultOption resultOption = default(ResultOption))
        {
            var ids = FindRows(search, resultOption);
            return GetStructs(ids);
        }

        /// <summary>
        /// Gets the rows with the given ids.
        /// </summary>
        /// <param name="ids">IDs of the rows to fetch from the table.</param>
        /// <returns>Returns the rows.</returns>
        public abstract IList<T> GetStructs(IEnumerable<long> ids);

        /// <summary>
        /// Inserts a row to the table. If an ID <![CDATA[<=]]> 0 is given an automatically generated ID will be used to add the dataset.
        /// </summary>
        /// <param name="row">The row to insert.</param>
        /// <returns>Returns the ID of the inserted dataset.</returns>
        public virtual long Insert(T row)
        {
            return Insert(Row.Create(Layout, row));
        }

        /// <summary>
        /// Updates a row to the table. The row must exist already!.
        /// </summary>
        /// <param name="row">The row to update.</param>
        public virtual void Update(T row)
        {
            Update(Row.Create(Layout, row));
        }

        /// <summary>
        /// Replaces a row at the table. The ID has to be given. This inserts (if the row does not exist) or updates (if it exists) the row.
        /// </summary>
        /// <param name="row">The row to replace (valid ID needed).</param>
        public virtual void Replace(T row)
        {
            Replace(Row.Create(Layout, row));
        }

        /// <summary>
        /// Checks whether a row is present unchanged at the database and removes it.
        /// (Use Delete(ID) to delete a DataSet without any checks).
        /// </summary>
        /// <param name="row"></param>
        public virtual void Delete(T row)
        {
            var id = Layout.GetID(row);
            T data = GetStruct(id);
            if (!data.Equals(row))
            {
                throw new DataException(string.Format("Dataset could not be found!"));
            }

            Delete(id);
        }

        /// <summary>
        /// Provides access to the row with the specified ID.
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public virtual T this[long id] => GetStruct(id);

        /// <summary>
        /// Copies all rows to a given array.
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
