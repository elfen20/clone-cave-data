using Cave.Data.Sql;

namespace Cave.Data.SQLite
{
    /// <summary>
    /// Provides a sqlite table implementation
    /// </summary>
    public class SQLiteTable : SqlTable
    {
        #region SQLite specific overrides
        /// <summary>
        /// This function does a lookup on the ids of the table and returns the row with the n-th ID where n is the specified index.
        /// Note that indices may change on each update, insert, delete and sorting is not garanteed!
        /// <param name="index">The index of the row to be fetched</param>
        /// </summary>
        /// <returns>Returns the row</returns>
        public override Row GetRowAt(int index)
        {
            long id = (long)SqlStorage.QueryValue(Database.Name, Name, "SELECT ID FROM " + FQTN + " ORDER BY ID LIMIT " + index + ",1");
            return GetRow(id);
        }

        /// <summary>
        /// Obtains the command to retrieve the last inserted row
        /// </summary>
        /// <param name="row">The row to be inserted</param>
        /// <returns></returns>
        protected override string GetLastInsertedIDCommand(Row row)
        {
            return "SELECT last_insert_rowid();";
        }

        #endregion

        /// <summary>
        /// Creates a new sqlite table instance (checks layout against database)
        /// </summary>
        /// <param name="database">The database this table belongs to</param>
        /// <param name="layout">The layout of the table</param>
        public SQLiteTable(SQLiteDatabase database, RowLayout layout)
            : base(database, layout)
        {
        }

        /// <summary>
        /// Creates a new sqlite table instance (retrieves layout from database)
        /// </summary>
        /// <param name="database">The database this table belongs to</param>
        /// <param name="name">The name of the table</param>
        public SQLiteTable(SQLiteDatabase database, string name)
            : base(database, name)
        {
        }
    }

    /// <summary>
    /// Provides a sqlite table implementation
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class SQLiteTable<T> : SqlTable<T> where T : struct
    {
        #region SQLite specific overrides
        /// <summary>
        /// This function does a lookup on the ids of the table and returns the row with the n-th ID where n is the specified index.
        /// Note that indices may change on each update, insert, delete and sorting is not garanteed!
        /// <param name="index">The index of the row to be fetched</param>
        /// </summary>
        /// <returns>Returns the row</returns>
        public override Row GetRowAt(int index)
        {
            long id = (long)SqlStorage.QueryValue(Database.Name, Name, "SELECT ID FROM " + FQTN + " ORDER BY ID LIMIT " + index + ",1");
            return GetRow(id);
        }

        /// <summary>
        /// Obtains the command to retrieve the last inserted row
        /// </summary>
        /// <param name="row">The row to be inserted</param>
        /// <returns></returns>
        protected override string GetLastInsertedIDCommand(Row row)
        {
            return "SELECT last_insert_rowid();";
        }

        #endregion

        /// <summary>
        /// Creates a new sqlite table instance
        /// </summary>
        /// <param name="database">The database this table belongs to</param>
        /// <param name="layout">Layout and name of the table</param>
        public SQLiteTable(SQLiteDatabase database, RowLayout layout)
                 : base(database, layout)
        {
        }
    }
}
