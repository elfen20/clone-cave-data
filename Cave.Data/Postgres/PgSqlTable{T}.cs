using System;
using Cave.Data.Sql;

namespace Cave.Data.Postgres
{
    /// <summary>
    /// Provides a postgre sql table implementation.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class PgSqlTable<T> : SqlTable<T>
        where T : struct
    {
        /// <summary>This is always enabled at postgresql.</summary>
        public override bool TransactionsUseParameters { get; } = false;

        #region PgSql specific overrides

        /// <summary>Creates the replace.</summary>
        /// <param name="cb">The cb.</param>
        /// <param name="row">The row.</param>
        protected override void CreateReplace(SqlCommandBuilder cb, Row row)
        {
            cb.Append("INSERT INTO ");
            cb.Append(FQTN);
            cb.Append(" VALUES (");
            for (var i = 0; i < Layout.FieldCount; i++)
            {
                if (i > 0)
                {
                    cb.Append(",");
                }

                var value = row.GetValue(i);
                if (value == null)
                {
                    cb.Append("NULL");
                }
                else
                {
                    value = SqlStorage.GetDatabaseValue(Layout.GetProperties(i), value);
                    if (TransactionsUseParameters)
                    {
                        cb.CreateAndAddParameter(value);
                    }
                    else
                    {
                        cb.Append(SqlStorage.EscapeFieldValue(Layout.GetProperties(i), value));
                    }
                }
            }
            cb.Append(") ON CONFLICT (");
            cb.Append(SqlStorage.EscapeFieldName(Layout.IDField));
            cb.AppendLine(") DO");
            cb.Append("UPDATE SET ");
            var count = 0;
            for (var i = 0; i < Layout.FieldCount; i++)
            {
                if (i == Layout.IDFieldIndex)
                {
                    continue;
                }

                if (count++ > 0)
                {
                    cb.Append(",");
                }

                cb.Append(SqlStorage.EscapeFieldName(Layout.GetProperties(i)));
                cb.Append("=");
                var value = row.GetValue(i);
                if (value == null)
                {
                    cb.Append("NULL");
                }
                else
                {
                    value = SqlStorage.GetDatabaseValue(Layout.GetProperties(i), value);
                    if (TransactionsUseParameters)
                    {
                        cb.CreateAndAddParameter(value);
                    }
                    else
                    {
                        cb.Append(SqlStorage.EscapeFieldValue(Layout.GetProperties(i), value));
                    }
                }
            }
            cb.AppendLine(";");
        }

        /// <summary>
        /// This function does a lookup on the ids of the table and returns the row with the n-th ID where n is the specified index.
        /// Note that indices may change on each update, insert, delete and sorting is not garanteed!.
        /// <param name="index">The index of the row to be fetched</param>
        /// </summary>
        /// <returns>Returns the row.</returns>
        public override Row GetRowAt(int index)
        {
            var id = (long)SqlStorage.QueryValue(Database.Name, Name, "SELECT ID FROM " + FQTN + " ORDER BY ID LIMIT " + index + ",1");
            return GetRow(id);
        }

        /// <summary>
        /// Gets the command to retrieve the last inserted row.
        /// </summary>
        /// <param name="row">The row to be inserted.</param>
        /// <returns></returns>
        protected override string GetLastInsertedIDCommand(Row row)
        {
            return "SELECT LASTVAL();";
        }

        #endregion

        /// <summary>
        /// Creates a new postgre sql table instance.
        /// </summary>
        /// <param name="database">The database the table belongs to.</param>
        /// <param name="layout">Layout and name of the table.</param>
        public PgSqlTable(PgSqlDatabase database, RowLayout layout)
            : base(database, layout)
        {
            AutoIncrementValue = "DEFAULT";
        }
    }
}
