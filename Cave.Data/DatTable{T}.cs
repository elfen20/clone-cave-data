using System;
using System.Collections.Generic;
using System.Data;
using Cave.IO;

namespace Cave.Data
{
    /// <summary>
    /// Provides a binary dat file database table.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class DatTable<T> : DatTable, ITable<T>
        where T : struct
    {
        #region constructor

        /// <summary>
        /// Creates a new <see cref="DatTable{T}"/> instance.
        /// </summary>
        /// <param name="database">Database this table belongs to.</param>
        /// <param name="file">Filename of the table file.</param>
        /// <param name="layout">Layout and name of the table.</param>
        public DatTable(DatDatabase database, RowLayout layout, string file)
            : base(database, file, layout)
        {
        }
        #endregion

        #region additional functionality

        /// <summary>
        /// searches for a specified databaset (first occurence).
        /// </summary>
        /// <param name="match"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        public bool FindFirst(Predicate<T> match, out T result)
        {
            FileStream.Position = 0;
            return FindNext(match, out result);
        }

        /// <summary>
        /// searches for a specified databaset (next occurence after <see cref="FindFirst"/>).
        /// </summary>
        /// <param name="match"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        public bool FindNext(Predicate<T> match, out T result)
        {
            if (match == null)
            {
                throw new ArgumentNullException("match");
            }

            while (FileStream.Position < FileStream.Length)
            {
                Row row = ReadCurrentRow(new DataReader(FileStream), Version, Layout);
                if (row != null)
                {
                    T value = row.GetStruct<T>(Layout);
                    if (match(value))
                    {
                        result = value;
                        return true;
                    }
                }
            }
            result = default(T);
            return false;
        }

        #endregion

        #region ITable<T> Member

        #region implemented non virtual

        /// <summary>
        /// Gets the row struct with the specified index.
        /// This allows a memorytable to be used as virtual list for listviews, ...
        /// Note that indices may change on each update, insert, delete and sorting is not garanteed!.
        /// </summary>
        /// <param name="index">The rows index (0..RowCount-1).</param>
        /// <returns></returns>
        /// <exception cref="IndexOutOfRangeException"></exception>
        public T GetStructAt(int index)
        {
            return GetRowAt(index).GetStruct<T>(Layout);
        }

        /// <summary>
        /// Inserts rows into the table using a transaction.
        /// </summary>
        /// <param name="rows">The rows to insert.</param>
        public void Insert(IEnumerable<T> rows)
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

        /// <summary>
        /// Updates rows at the table. The rows must exist already!.
        /// </summary>
        /// <param name="rows">The rows to update.</param>
        public void Update(IEnumerable<T> rows)
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

        /// <summary>
        /// Replaces rows at the table. This inserts (if the row does not exist) or updates (if it exists) each row.
        /// </summary>
        /// <param name="rows">The rows to replace (valid ID needed).</param>
        public void Replace(IEnumerable<T> rows)
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

        /// <summary>Tries to get the (unique) row with the given fieldvalue.</summary>
        /// <param name="search">The search.</param>
        /// <param name="row">The row.</param>
        /// <returns>Returns true on success, false otherwise.</returns>
        public bool TryGetStruct(Search search, out T row)
        {
            var id = FindRow(search);
            if (id > -1)
            {
                row = GetStruct(id);
                return true;
            }
            row = default(T);
            return false;
        }

        #endregion

        #region implemented virtual

        /// <summary>
        /// Gets a row from the table.
        /// </summary>
        /// <param name="id">The ID of the row to be fetched.</param>
        /// <returns>Returns the row.</returns>
        public virtual T GetStruct(long id)
        {
            return GetRow(id).GetStruct<T>(Layout);
        }

        /// <summary>
        /// Searches the table for a single row with specified search.
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
        /// Searches the table for rows with specified field value combinations.
        /// </summary>
        /// <param name="search">The search to run.</param>
        /// <param name="resultOption">Options for the search and the result set.</param>
        /// <returns>Returns the rows found.</returns>
        public virtual IList<T> GetStructs(Search search = default(Search), ResultOption resultOption = default(ResultOption))
        {
            return ToStructs<T>(Layout, GetRows(search, resultOption));
        }

        /// <summary>
        /// Gets the rows with the specified ids.
        /// </summary>
        /// <param name="ids">IDs of the rows to fetch from the table.</param>
        /// <returns>Returns the rows.</returns>
        public virtual IList<T> GetStructs(IEnumerable<long> ids)
        {
            return ToStructs<T>(Layout, GetRows(ids));
        }

        /// <summary>
        /// Inserts a row to the table. If an ID <![CDATA[<=]]> 0 is specified an automatically generated ID will be used to add the dataset.
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
                throw new DataException(string.Format("Row does not match row at database!"));
            }

            Delete(id);
        }

        /// <summary>
        /// Provides access to the row with the specified ID.
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public virtual T this[long id]
        {
            get => GetStruct(id);
            set
            {
                var i = Layout.GetID(value);
                if (i != id)
                {
                    throw new ArgumentException(string.Format("ID mismatch!"));
                }

                Replace(value);
            }
        }

        /// <summary>
        /// Copies all rows to a specified array.
        /// </summary>
        /// <param name="rowArray"></param>
        /// <param name="startIndex"></param>
        public void CopyTo(T[] rowArray, int startIndex)
        {
            GetStructs().CopyTo(rowArray, startIndex);
        }

        #endregion

        #endregion
    }
}
