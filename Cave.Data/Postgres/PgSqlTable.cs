using System;
using Cave.Data.Sql;

namespace Cave.Data.Postgres
{
    /// <summary>
    /// Provides a postgre sql table implementation.
    /// </summary>
    public class PgSqlTable : SqlTable
    {
        #region PgSql specific overrides

        /// <summary>Creates the replace.</summary>
        /// <param name="cb">The cb.</param>
        /// <param name="row">The row.</param>
        protected override void CreateReplace(SqlCommandBuilder cb, Row row)
        {
            throw new NotImplementedException();
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
        /// Creates a new postgre sql table instance (checks layout against database).
        /// </summary>
        /// <param name="database">The database the table belongs to.</param>
        /// <param name="layout">Layout of the table.</param>
        public PgSqlTable(PgSqlDatabase database, RowLayout layout)
            : base(database, layout)
        {
            AutoIncrementValue = "DEFAULT";
        }

        /// <summary>
        /// Creates a new postgre sql table instance (retrieves layout from database).
        /// </summary>
        /// <param name="database">The database the table belongs to.</param>
        /// <param name="table">Name of the table.</param>
        public PgSqlTable(PgSqlDatabase database, string table)
            : base(database, table)
        {
            AutoIncrementValue = "DEFAULT";
        }
    }
}
