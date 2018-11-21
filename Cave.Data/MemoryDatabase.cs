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

using Cave.Collections.Generic;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Cave.Data
{
    /// <summary>
    /// Provides a memory based database
    /// </summary>
    public sealed class MemoryDatabase : Database
    {
        /// <summary>Gets the default memory database.</summary>
        /// <value>The default memory database.</value>
        public static MemoryDatabase Default { get; } = new MemoryDatabase();

        /// <summary>Returns true assuming that no one else accesses the system memory</summary>
        /// <value><c>true</c></value>
        public override bool IsSecure { get { return true; } }

        Dictionary<string, ITable> m_Tables = new Dictionary<string, ITable>();

        /// <summary>
        /// Creates an empty unbound memory database (within a new memory storage)
        /// This is used by temporary tables and query results
        /// </summary>
        private MemoryDatabase()
            : base(MemoryStorage.Default, "Default")
        {
        }

        /// <summary>
        /// Creates a new memory based database
        /// </summary>
        /// <param name="storage">The storage engine</param>
        /// <param name="name">The name of the database</param>
        public MemoryDatabase(MemoryStorage storage, string name)
            : base(storage, name)
        {
        }

        #region IDatabase Member

        /// <summary>
        /// Obtains all available table names
        /// </summary>
        public override string[] TableNames
        {
            get
            {
                if (Closed)
                {
                    throw new ObjectDisposedException(Name);
                }

                return m_Tables.Keys.ToArray();
            }
        }

        /// <summary>
        /// Obtains whether the specified table exists or not
        /// </summary>
        /// <param name="table">The name of the table</param>
        /// <returns></returns>
        public override bool HasTable(string table)
        {
            if (Closed)
            {
                throw new ObjectDisposedException(Name);
            }

            return m_Tables.ContainsKey(table);
        }

        /// <summary>
        /// Opens the table with the specified name
        /// </summary>
        /// <param name="table">The name of the table</param>
        /// <returns>Returns an <see cref="ITable"/> instance for the specified table</returns>
        public override ITable GetTable(string table)
        {
            if (!HasTable(table))
            {
                throw new InvalidOperationException(string.Format("Table '{0}' does not exist!", table));
            }

            return m_Tables[table];
        }

        /// <summary>
        /// Opens and retrieves the existing table with the specified layout
        /// </summary>
        /// <typeparam name="T">Row structure type</typeparam>
        /// <param name="layout">Layout and name of the table</param>
        /// <returns>Returns a table instance</returns>
        protected override ITable<T> OpenTable<T>(RowLayout layout)
        {
            if (layout == null)
            {
                throw new ArgumentNullException("Layout");
            }

            ITable table = GetTable(layout.Name);
            Storage.CheckLayout(layout, table.Layout);

            MemoryTable<T> t = table as MemoryTable<T>;
            if (t != null)
            {
                return t;
            }

            return Load<T>();
        }

        /// <summary>Adds a new table with the specified name</summary>
        /// <param name="layout">Layout of the table</param>
        /// <param name="flags">The table creation flags</param>
        /// <returns>Returns an <see cref="ITable" /> instance for the specified table</returns>
        /// <exception cref="Exception"></exception>
        public override ITable CreateTable(RowLayout layout, TableFlags flags)
        {
            if (layout == null)
            {
                throw new ArgumentNullException("Layout");
            }

            if (m_Tables.ContainsKey(layout.Name))
            {
                throw new InvalidOperationException(string.Format("Table '{0}' already exists!", layout.Name));
            }

            MemoryTable  table = new MemoryTable(layout);
            m_Tables[layout.Name] = table;
            return table;
        }

        /// <summary>Adds a new table with the specified type</summary>
        /// <typeparam name="T">The row struct to use for the table</typeparam>
        /// <param name="flags">The table creation flags</param>
        /// <returns>Returns an <see cref="ITable{T}" /> instance for the specified table</returns>
        /// <exception cref="Exception"></exception>
        /// <param name="tableName">Name of the table to create (optional, use this to overwrite the default table name)</param>
        public override ITable<T> CreateTable<T>(TableFlags flags, string tableName)
        {
            RowLayout layout = RowLayout.CreateTyped(typeof(T), tableName, Storage);
            LogCreateTable(layout);
            if (m_Tables.ContainsKey(layout.Name))
            {
                throw new InvalidOperationException(string.Format("Table '{0}' already exists!", layout.Name));
            }

            MemoryTable<T> table = new MemoryTable<T>();
            m_Tables[layout.Name] = table;
            return table;
        }

        /// <summary>Removes a table from the database</summary>
        /// <param name="table">The name of the table</param>
        /// <exception cref="ArgumentException"></exception>
        public override void DeleteTable(string table)
        {
            if (!m_Tables.Remove(table))
            {
                throw new ArgumentException(string.Format("Table '{0}' does not exist!", table));
            }
        }

        /// <summary>
        /// Closes the instance and flushes all cached data
        /// </summary>
        public override void Close()
        {
            if (Closed)
            {
                throw new ObjectDisposedException(Name);
            }

            m_Tables.Clear();
            m_Tables = null;
        }

        /// <summary>
        /// Obtains whether the database was already closed or not
        /// </summary>
        public override bool Closed { get { return m_Tables == null; } }
        #endregion
    }
}
