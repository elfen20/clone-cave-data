using System;
using System.Collections.Generic;
using System.Text;
using Cave.Data.Sql;

namespace Cave.Data.Microsoft
{
    /// <summary>
    /// Provides a MsSql table implementation.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class MsSqlTable<T> : SqlTable<T>, ITable<T>
        where T : struct
    {
        #region MsSql specific overrides

        /// <summary>
        /// This function does a lookup on the ids of the table and returns the row with the n-th ID where n is the specified index.
        /// Note that indices may change on each update, insert, delete and sorting is not garanteed!.
        /// <param name="index">The index of the row to be fetched</param>
        /// </summary>
        /// <returns>Returns the row.</returns>
        public override Row GetRowAt(int index)
        {
            var cmd = $"WITH TempOrderedData AS (SELECT ID, ROW_NUMBER() OVER (ORDER BY ID) AS 'RowNumber' FROM {FQTN}) SELECT * FROM TempOrderedData WHERE RowNumber={index}";
            var id = (long)SqlStorage.QueryValue(Database.Name, Name, cmd);
            return GetRow(id);
        }

        /// <summary>
        /// Gets the command to retrieve the last inserted row.
        /// </summary>
        /// <param name="row">The row to be inserted.</param>
        /// <returns></returns>
        protected override string GetLastInsertedIDCommand(Row row)
        {
            return "SELECT SCOPE_IDENTITY() AS [SCOPE_IDENTITY];";
        }

        #endregion

        #region constructor

        /// <summary>
        /// Creates a new mssql table instance.
        /// </summary>
        /// <param name="database">The database the table belongs to.</param>
        /// <param name="layout">Layout and name of the table.</param>
        public MsSqlTable(MsSqlDatabase database, RowLayout layout)
            : base(database, layout)
        {
        }
        #endregion
    }
}
