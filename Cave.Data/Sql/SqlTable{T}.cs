using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Text;
using Cave.Collections.Generic;

namespace Cave.Data.Sql
{
    /// <summary>
    /// Provides a table implementation for generic sql92 databases.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public abstract class SqlTable<T> : SqlTable, ITable<T>
        where T : struct
    {
        /// <summary>Creates a new SqlTable instance.</summary>
        /// <param name="database">The database this table belongs to.</param>
        /// <param name="layout">The layout of the table.</param>
        protected SqlTable(IDatabase database, RowLayout layout)
            : base(database, CheckTypedLayout(typeof(T), database, layout))
        {
        }

        #region sql92 protected find functions

        /// <summary>Runs a sql group function and returns the result as structures.</summary>
        /// <param name="search">The search.</param>
        /// <param name="options">The options.</param>
        /// <returns>Returns a list of structures.</returns>
        protected internal virtual IList<T> SqlGetGroupStructs(SqlSearch search, ResultOption options)
        {
            List<Row> rows = SqlGetGroupRows(search, options);
            var result = new List<T>(rows.Count);
            if (SqlStorage.SupportsAllFieldsGroupBy)
            {
                foreach (Row row in rows)
                {
                    result.Add(row.GetStruct<T>(Layout));
                }
            }
            else
            {
                foreach (Row row in rows)
                {
                    Search singleSearch = Search.None;
                    for (var i = 0; i < Layout.FieldCount; i++)
                    {
                        if (i == Layout.IDFieldIndex)
                        {
                            continue;
                        }

                        singleSearch &= Search.FieldEquals(Layout.GetProperties(i).Name, row.GetValue(i));
                    }
                    result.Add(GetStruct(singleSearch, ResultOption.Limit(1) + ResultOption.SortDescending(SqlStorage.EscapeFieldName(Layout.IDField))));
                }
            }
            return result;
        }

        /// <summary>
        /// Searches the table for rows with given field value combinations.
        /// </summary>
        /// <param name="search">The search to run.</param>
        /// <param name="options">Options for the search and the result set.</param>
        /// <returns>Returns the ID of the row found or -1.</returns>
        protected internal virtual IList<T> SqlGetStructs(SqlSearch search, ResultOption options)
        {
            if (options.Contains(ResultOptionMode.Group))
            {
                return SqlGetGroupStructs(search, options);
            }
            var command = new StringBuilder();
            command.Append("SELECT * FROM ");
            command.Append(FQTN);
            command.Append(" WHERE ");

            command.Append(search.ToString());

            var orderCount = 0;
            foreach (ResultOption o in options.ToArray(ResultOptionMode.SortAsc, ResultOptionMode.SortDesc))
            {
                if (orderCount++ == 0)
                {
                    command.Append(" ORDER BY ");
                }
                else
                {
                    command.Append(",");
                }
                command.Append(SqlStorage.EscapeFieldName(Layout.GetProperties(o.Parameter)));
                if (o.Mode == ResultOptionMode.SortAsc)
                {
                    command.Append(" ASC");
                }
                else
                {
                    command.Append(" DESC");
                }
            }

            var limit = 0;
            foreach (ResultOption o in options.ToArray(ResultOptionMode.Limit))
            {
                if (limit++ > 0)
                {
                    throw new InvalidOperationException(string.Format("Cannot set two different limits!"));
                }

                command.Append(" LIMIT " + o.Parameter);
            }
            var offset = 0;
            foreach (ResultOption o in options.ToArray(ResultOptionMode.Offset))
            {
                if (offset++ > 0)
                {
                    throw new InvalidOperationException(string.Format("Cannot set two different offsets!"));
                }

                command.Append(" OFFSET " + o.Parameter);
            }

            return SqlStorage.Query<T>(Database.Name, Name, command.ToString(), search.Parameters.ToArray());
        }
        #endregion

        #region implemented functionality

        #region non virtual

        /// <summary>
        /// Gets the row struct with the given index.
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
        /// Gets the rows with the given ids.
        /// </summary>
        /// <param name="ids">IDs of the rows to fetch from the table.</param>
        /// <returns>Returns the rows.</returns>
        public IList<T> GetStructs(IEnumerable<long> ids)
        {
            if (ids == null)
            {
                throw new ArgumentNullException("IDs");
            }

            var command = new StringBuilder();
            command.AppendLine($"SELECT * FROM {FQTN} WHERE");
            var first = true;
            var idFieldName = Layout.IDField.NameAtDatabase;
            foreach (var id in ids.AsSet())
            {
                if (first)
                {
                    first = false;
                    command.AppendLine($"({idFieldName}={id})");
                }
                else
                {
                    command.AppendLine($"OR({idFieldName}={id})");
                }
            }
            return first ? new List<T>() : SqlStorage.Query<T>(Database.Name, Name, command.ToString());
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

        #region virtual

        /// <summary>
        /// Gets a row from the table.
        /// </summary>
        /// <param name="id">The ID of the row to be fetched.</param>
        /// <returns>Returns the row.</returns>
        public virtual T GetStruct(long id)
        {
            return SqlStorage.QueryRow<T>(Database.Name, Name, "SELECT * FROM " + FQTN + " WHERE " + SqlStorage.EscapeFieldName(Layout.IDField) + "=" + id.ToString());
        }

        /// <summary>
        /// Searches the table for a single row with given search.
        /// </summary>
        /// <param name="search">The search to run.</param>
        /// <param name="resultOption">Options for the search and the result set.</param>
        /// <returns>Returns the row found.</returns>
        public virtual T GetStruct(Search search = default(Search), ResultOption resultOption = default(ResultOption))
        {
            if (search == null)
            {
                search = Search.None;
            }

            if (resultOption == null)
            {
                resultOption = ResultOption.None;
            }

            SqlSearch s = search.ToSql(Layout, SqlStorage);
            s.CheckFieldsPresent(resultOption);
            var rows = SqlGetStructs(s, resultOption);
            if (rows.Count == 0)
            {
                throw new ArgumentException(string.Format("Dataset could not be found!"));
            }

            if (rows.Count > 1)
            {
                throw new InvalidDataException("Dataset not unique!");
            }

            return rows[0];
        }

        /// <summary>
        /// Searches the table for rows with given field value combinations.
        /// </summary>
        /// <param name="search">The search to run.</param>
        /// <param name="resultOption">Options for the search and the result set.</param>
        /// <returns>Returns the rows found.</returns>
        public virtual IList<T> GetStructs(Search search = default(Search), ResultOption resultOption = default(ResultOption))
        {
            if (search == null)
            {
                search = Search.None;
            }

            if (resultOption == null)
            {
                resultOption = ResultOption.None;
            }

            SqlSearch s = search.ToSql(Layout, SqlStorage);
            s.CheckFieldsPresent(resultOption);
            return SqlGetStructs(s, resultOption);
        }

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
                    throw new InvalidDataException(string.Format("ID mismatch!"));
                }

                Replace(value);
            }
        }

        /// <summary>
        /// Copies all rows to a given array.
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
