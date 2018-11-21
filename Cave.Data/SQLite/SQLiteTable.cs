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
