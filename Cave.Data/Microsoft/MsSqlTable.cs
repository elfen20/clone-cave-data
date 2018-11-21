#region CopyRight 2018
/*
    Copyright (c) 2005-2018 Andreas Rohleder (andreas@rohleder.cc)
    All rights reserved
*/
#endregion
#region License LGPL-3
/*
    This program/library/sourcecode is free software; you can redistribute it
    and/or modify it under the terms of the GNU Lesser General Public License
    version 3 as published by the Free Software Foundation subsequent called
    the License.

    You may not use this program/library/sourcecode except in compliance
    with the License. The License is included in the LICENSE file
    found at the installation directory or the distribution package.

    Permission is hereby granted, free of charge, to any person obtaining
    a copy of this software and associated documentation files (the
    "Software"), to deal in the Software without restriction, including
    without limitation the rights to use, copy, modify, merge, publish,
    distribute, sublicense, and/or sell copies of the Software, and to
    permit persons to whom the Software is furnished to do so, subject to
    the following conditions:

    The above copyright notice and this permission notice shall be included
    in all copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
    EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
    MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
    NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
    LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
    OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
    WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/
#endregion License
#region Authors & Contributors
/*
   Author:
     Andreas Rohleder <andreas@rohleder.cc>

   Contributors:
 */
#endregion Authors & Contributors

using Cave.Data.Sql;
using System;
using System.Collections.Generic;
using System.Text;

namespace Cave.Data.Microsoft
{
    /// <summary>
    /// Provides a MsSql table implementation
    /// </summary>
    public class MsSqlTable : SqlTable
    {
        #region MsSql specific overrides
        /// <summary>
        /// This function does a lookup on the ids of the table and returns the row with the n-th ID where n is the specified index.
        /// Note that indices may change on each update, insert, delete and sorting is not garanteed!
        /// <param name="index">The index of the row to be fetched</param>
        /// </summary>
        /// <returns>Returns the row</returns>
        public override Row GetRowAt(int index)
        {
            long id = (long)SqlStorage.QueryValue(Database.Name, Name,
                "WITH TempOrderedData AS (SELECT ID, ROW_NUMBER() OVER (ORDER BY ID) AS 'RowNumber' FROM " + FQTN + ")" +
                "SELECT * FROM TempOrderedData WHERE RowNumber=" + index);
            return GetRow(id);
        }

        /// <summary>
        /// Obtains the command to retrieve the last inserted row
        /// </summary>
        /// <param name="row">The row to be inserted</param>
        /// <returns></returns>
        protected override string GetLastInsertedIDCommand(Row row)
        {
            return "SELECT SCOPE_IDENTITY() AS [SCOPE_IDENTITY];";
        }
        /// <summary>
        /// Searches the table for rows with specified field value combinations.
        /// </summary>
        /// <param name="search">The search to run</param>
        /// <param name="option">Options for the search and the result set</param>
        /// <returns>Returns the ID of the row found or -1</returns>
        protected internal override List<long> SqlFindIDs(SqlSearch search, ResultOption option)
        {
            if (Layout.IDFieldIndex < 0)
            {
                throw new ArgumentNullException("IDField");
            }

            StringBuilder command = new StringBuilder();
            command.Append("SELECT ");

            int offset = 0;
            foreach (ResultOption o in option.ToArray(ResultOptionMode.Offset))
            {
                if (offset++ > 0)
                {
                    throw new InvalidOperationException(string.Format("Cannot set two different offsets!"));
                }

                command.Append("OFFSET ");
                command.Append(o.Parameter);
                command.Append(" ROWS ");
            }
            int limit = 0;
            foreach (ResultOption o in option.ToArray(ResultOptionMode.Limit))
            {
                if (limit++ > 0)
                {
                    throw new InvalidOperationException(string.Format("Cannot set two different limits!"));
                }

                command.Append("FETCH NEXT ");
                command.Append(o.Parameter);
                command.Append(" ROWS ONLY");
            }

            command.Append(SqlStorage.EscapeFieldName(Layout.IDField));
            foreach (string fieldName in search.FieldNames)
            {
                if (fieldName == Layout.IDField.Name)
                {
                    continue;
                }

                command.Append(", ");
                command.Append(SqlStorage.EscapeFieldName(Layout.GetProperties(fieldName)));
            }

            command.Append(" FROM ");
            command.Append(FQTN);
            command.Append(" WHERE ");

            command.Append(search.ToString());

            int orderCount = 0;
            foreach (ResultOption o in option.ToArray(ResultOptionMode.SortAsc, ResultOptionMode.SortDesc))
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

            if (option.Contains(ResultOptionMode.Group))
            {
                throw new InvalidOperationException(string.Format("Cannot use Option.Group and Option.Sort at once!"));
            }

            var rows = SqlStorage.Query(null, Database.Name, Name, command.ToString(), search.Parameters.ToArray());
            return ToIDs(Layout, rows);
        }
        #endregion

        /// <summary>
        /// Creates a new mysql table instance (checks layout against database)
        /// </summary>
        /// <param name="database">The database the table belongs to</param>
        /// <param name="layout">Layout and name of the table</param>
        public MsSqlTable(MsSqlDatabase database, RowLayout layout)
            : base(database, layout)
        {
        }

        /// <summary>
        /// Creates a new mysql table instance (retrieves layout from database)
        /// </summary>
        /// <param name="database">The database the table belongs to</param>
        /// <param name="table">The Name of the table</param>
        public MsSqlTable(MsSqlDatabase database, string table)
            : base(database, table)
        {
        }
    }

    /// <summary>
    /// Provides a MsSql table implementation
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class MsSqlTable<T> : SqlTable<T>, ITable<T> where T : struct
    {
        #region MsSql specific overrides
        /// <summary>
        /// This function does a lookup on the ids of the table and returns the row with the n-th ID where n is the specified index.
        /// Note that indices may change on each update, insert, delete and sorting is not garanteed!
        /// <param name="index">The index of the row to be fetched</param>
        /// </summary>
        /// <returns>Returns the row</returns>
        public override Row GetRowAt(int index)
        {
            long id = (long)SqlStorage.QueryValue(Database.Name, Name,
                "WITH TempOrderedData AS (SELECT ID, ROW_NUMBER() OVER (ORDER BY ID) AS 'RowNumber' FROM " + FQTN + ")" +
                "SELECT * FROM TempOrderedData WHERE RowNumber=" + index);
            return GetRow(id);
        }

        /// <summary>
        /// Obtains the command to retrieve the last inserted row
        /// </summary>
        /// <param name="row">The row to be inserted</param>
        /// <returns></returns>
        protected override string GetLastInsertedIDCommand(Row row)
        {
            return "SELECT SCOPE_IDENTITY() AS [SCOPE_IDENTITY];";
        }

        #endregion

        #region constructor
        /// <summary>
        /// Creates a new mssql table instance
        /// </summary>
        /// <param name="database">The database the table belongs to</param>
        /// <param name="layout">Layout and name of the table</param>
        public MsSqlTable(MsSqlDatabase database, RowLayout layout)
            : base(database, layout)
        {
        }
        #endregion
    }
}
