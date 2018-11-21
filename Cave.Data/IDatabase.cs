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

namespace Cave.Data
{
    /// <summary>
    /// Provides an interface for system independent database connections
    /// </summary>
    public interface IDatabase
    {
        /// <summary>Gets a value indicating whether this instance is using a secure connection to the storage.</summary>
        /// <value><c>true</c> if this instance is using a secure connection; otherwise, <c>false</c>.</value>
        bool IsSecure { get; }

        /// <summary>
        /// The storage engine the database belongs to
        /// </summary>
        IStorage Storage { get; }

        /// <summary>
        /// Obtains the name of the database
        /// </summary>
        string Name { get; }

        /// <summary>
        /// Obtains the available table names
        /// </summary>
        string[] TableNames { get; }

        /// <summary>
        /// Obtains whether the specified table exists or not
        /// </summary>
        /// <param name="table">The name of the table</param>
        /// <returns></returns>
        bool HasTable(string table);

        #region GetTable functions

        /// <summary>
        /// Opens the table with the specified name
        /// </summary>
        /// <param name="table">The name of the table</param>
        /// <returns>Returns an <see cref="ITable"/> instance for the specified table</returns>
        ITable GetTable(string table);

        /// <summary>
        /// Opens or creates the table with the specified layout
        /// </summary>
        /// <param name="layout">Layout of the table</param>
        /// <returns>Returns an <see cref="ITable"/> instance for the specified table</returns>
        ITable GetTable(RowLayout layout);

        /// <summary>
        /// Opens or creates the table with the specified layout
        /// </summary>
        /// <param name="layout">Layout of the table</param>
        /// <param name="flags">Flags for table loading</param>
        /// <returns>Returns an <see cref="ITable"/> instance for the specified table</returns>
        ITable GetTable(RowLayout layout, TableFlags flags);

        /// <summary>
        /// Opens or creates the table with the specified type
        /// </summary>
        /// <typeparam name="T">The row struct to use for the table</typeparam>
        /// <returns>Returns an <see cref="ITable{T}"/> instance for the specified table</returns>
        ITable<T> GetTable<T>() where T : struct;

        /// <summary>
        /// Opens or creates the table with the specified type
        /// </summary>
        /// <typeparam name="T">The row struct to use for the table</typeparam>
        /// <param name="flags">Flags for table loading</param>
        /// <returns>Returns an <see cref="ITable{T}"/> instance for the specified table</returns>
        ITable<T> GetTable<T>(TableFlags flags) where T : struct;

        /// <summary>
        /// Opens or creates the table with the specified type
        /// </summary>
        /// <typeparam name="T">The row struct to use for the table</typeparam>
        /// <param name="table">The name of the table, default value is read from Table attribute</param>
        /// <returns>Returns an <see cref="ITable{T}"/> instance for the specified table</returns>
        ITable<T> GetTable<T>(string table) where T : struct;

        /// <summary>
        /// Opens or creates the table with the specified type
        /// </summary>
        /// <typeparam name="T">The row struct to use for the table</typeparam>
        /// <param name="flags">Flags for table loading</param>
        /// <param name="table">The name of the table, default value is read from Table attribute</param>
        /// <returns>Returns an <see cref="ITable{T}"/> instance for the specified table</returns>
        ITable<T> GetTable<T>(TableFlags flags, string table) where T : struct;

        #endregion

        #region CreateTable functions
        /// <summary>
        /// Adds a new table with the specified layout
        /// </summary>
        /// <param name="layout">Layout of the table</param>
        /// <returns>Returns an <see cref="ITable"/> instance for the specified table</returns>
        ITable CreateTable(RowLayout layout);

        /// <summary>
        /// Adds a new table with the specified layout
        /// </summary>
        /// <param name="layout">Layout of the table</param>
        /// <param name="flags">The table creation flags</param>
        /// <returns>Returns an <see cref="ITable"/> instance for the specified table</returns>
        ITable CreateTable(RowLayout layout, TableFlags flags);

        /// <summary>
        /// Adds a new table with the specified type
        /// </summary>
        /// <typeparam name="T">The row struct to use for the table</typeparam>
        /// <returns>Returns an <see cref="ITable{T}"/> instance for the specified table</returns>
        ITable<T> CreateTable<T>() where T : struct;

        /// <summary>
        /// Adds a new table with the specified type
        /// </summary>
        /// <typeparam name="T">The row struct to use for the table</typeparam>
        /// <param name="flags">The table creation flags</param>
        /// <returns>Returns an <see cref="ITable{T}"/> instance for the specified table</returns>
        ITable<T> CreateTable<T>(TableFlags flags) where T : struct;

        /// <summary>
        /// Adds a new table with the specified type
        /// </summary>
        /// <typeparam name="T">The row struct to use for the table</typeparam>
        /// <param name="table">The name of the table, default value is read from Table attribute</param>
        /// <returns>Returns an <see cref="ITable{T}"/> instance for the specified table</returns>
        ITable<T> CreateTable<T>(string table) where T : struct;

        /// <summary>
        /// Adds a new table with the specified type
        /// </summary>
        /// <typeparam name="T">The row struct to use for the table</typeparam>
        /// <param name="flags">The table creation flags</param>
        /// <param name="table">The name of the table, default value is read from Table attribute</param>
        /// <returns>Returns an <see cref="ITable{T}"/> instance for the specified table</returns>
        ITable<T> CreateTable<T>(TableFlags flags, string table) where T : struct;
        #endregion

        /// <summary>
        /// Loads a whole table into memory
        /// </summary>
        /// <param name="table">The name of the table</param>
        /// <returns>Returns a new <see cref="MemoryTable"/> instance containing all row of the table</returns>
        MemoryTable Load(string table);

        /// <summary>
        /// Loads a whole table into memory
        /// </summary>
        /// <typeparam name="T">The row struct to use for the table</typeparam>
        /// <returns>Returns a new <see cref="MemoryTable{T}"/> instance containing all row of the table</returns>
        MemoryTable<T> Load<T>() where T : struct;

        /// <summary>
        /// Loads a whole table into memory
        /// </summary>
        /// <typeparam name="T">The row struct to use for the table</typeparam>
        /// <param name="table">The name of the table, default value is read from Table attribute</param>
        /// <returns>Returns a new <see cref="MemoryTable{T}"/> instance containing all row of the table</returns>
        MemoryTable<T> Load<T>(string table) where T : struct;

        /// <summary>
        /// Removes a table from the database
        /// </summary>
        /// <param name="table">The name of the table</param>
        void DeleteTable(string table);

        /// <summary>
        /// Closes the database
        /// </summary>
        void Close();

        /// <summary>
        /// Obtains whether the database was already closed or not
        /// </summary>
        bool Closed { get; }
    }
}
