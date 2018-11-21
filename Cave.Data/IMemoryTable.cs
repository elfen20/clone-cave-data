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

using Cave.IO;
using System.Collections.Generic;

namespace Cave.Data
{
    /// <summary>
    /// Provides an interface for tables stored at the memory
    /// </summary>
    public interface IMemoryTable : ITable
    {
        /// <summary>Gets a value indicating whether this instance is readonly.</summary>
        /// <value><c>true</c> if this instance is readonly; otherwise, <c>false</c>.</value>
        bool IsReadonly { get; }

        /// <summary>Replaces all data present with the data at the specified table</summary>
        /// <param name="table">The table to load</param>
        /// <param name="search">The search.</param>
        /// <param name="callback">The callback.</param>
        /// <param name="userItem">The user item.</param>
        void LoadTable(ITable table, Search search = null, ProgressCallback callback = null, object userItem = null);

        /// <summary>
        /// Gets/sets the transaction log used to store all changes. The user has to create it, dequeue the items and
        /// dispose it after usage!
        /// </summary>
        TransactionLog TransactionLog { get; set; }

        /// <summary>
        /// Replaces the whole data at the table with the specified one without writing transactions
        /// </summary>
        /// <param name="rows"></param>
        void SetRows(IEnumerable<Row> rows);

        /// <summary>
        /// Replaces a row at the table. The ID has to be given. This inserts (if the row does not exist) or updates (if it exists).
        /// </summary>
        /// <param name="row">The row to replace (valid ID needed)</param>
        /// <param name="writeTransaction">If true a transaction is generated at the <see cref="TransactionLog"/></param>
        void Replace(Row row, bool writeTransaction);

        /// <summary>
        /// Replaces rows at the table. The ID has to be given. This inserts (if the row does not exist) or updates (if it exists).
        /// </summary>
        /// <param name="rows">The rows to replace (valid ID needed)</param>
        /// <param name="writeTransaction">If true a transaction is generated at the <see cref="TransactionLog"/></param>
        void Replace(IEnumerable<Row> rows, bool writeTransaction);

        /// <summary>
        /// Inserts a row to the table. If an ID &lt; 0 is specified an automatically generated ID will be used to add the dataset.
        /// </summary>
        /// <param name="row">The row to insert. If an ID &lt;= 0 is specified an automatically generated ID will be used to add the dataset.</param>
        /// <param name="writeTransaction">If true a transaction is generated at the <see cref="TransactionLog"/></param>
        /// <returns>Returns the ID of the inserted dataset</returns>
        long Insert(Row row, bool writeTransaction);

        /// <summary>
        /// Inserts rows into the table using a transaction. 
        /// </summary>
        /// <param name="rows">The rows to insert.</param>
        /// <param name="writeTransaction">If true a transaction is generated at the <see cref="TransactionLog"/></param>
        void Insert(IEnumerable<Row> rows, bool writeTransaction);

        /// <summary>
        /// Updates a row at the table. The row must exist already!
        /// </summary>
        /// <param name="row">The row to update</param>
        /// <param name="writeTransaction">If true a transaction is generated at the <see cref="TransactionLog"/></param>
        void Update(Row row, bool writeTransaction);

        /// <summary>
        /// Updates rows at the table using a transaction. 
        /// </summary>
        /// <param name="rows">The rows to insert.</param>
        /// <param name="writeTransaction">If true a transaction is generated at the <see cref="TransactionLog"/></param>
        void Update(IEnumerable<Row> rows, bool writeTransaction);

        /// <summary>
        /// Removes a row from the table.
        /// </summary>
        /// <param name="id">The dataset ID to remove</param>
        /// <param name="writeTransaction">If true a transaction is generated at the <see cref="TransactionLog"/></param>
        void Delete(long id, bool writeTransaction);

        /// <summary>Removes all rows with the specified ids from the table</summary>
        /// <param name="ids">The ids.</param>
        /// <param name="writeTransaction">If true a transaction is generated at the <see cref="TransactionLog" /></param>
        void Delete(IEnumerable<long> ids, bool writeTransaction);

		/// <summary>Removes all rows from the table matching the specified search.</summary>
		/// <param name="search">The Search used to identify rows for removal</param>
		/// <param name="writeTransaction">If true a transaction is generated at the <see cref="TransactionLog" /></param>
		/// <returns>Returns the number of dataset deleted.</returns>
		int TryDelete(Search search, bool writeTransaction);

        /// <summary>
        /// Clears all rows of the table (this operation will not write anything to the transaction log).
        /// </summary>
        /// <param name="resetIDs">if set to <c>true</c> [the next insert will get id 1].</param>
        void Clear(bool resetIDs);
    }

    /// <summary>
    /// Provides an interface for tables stored at the memory
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface IMemoryTable<T> : ITable<T>, IMemoryTable where T: struct
    {
        /// <summary>
        /// Replaces the whole data at the table with the specified one without writing transactions
        /// </summary>
        /// <param name="items"></param>
        void SetStructs(IEnumerable<T> items);

        /// <summary>
        /// Inserts a row into the table. If an ID &lt; 0 is specified an automatically generated ID will be used to add the dataset.
        /// </summary>
        /// <param name="row">The row to insert. If an ID &lt;= 0 is specified an automatically generated ID will be used to add the dataset.</param>
        /// <param name="writeTransaction">If true a transaction is generated at the <see cref="TransactionLog"/></param>
        /// <returns>Returns the ID of the inserted dataset</returns>
        long Insert(T row, bool writeTransaction);

        /// <summary>
        /// Replaces a row at the table. The ID has to be given. This inserts (if the row does not exist) or updates (if it exists) the row.
        /// </summary>
        /// <param name="row">The row to replace (valid ID needed)</param>
        /// <param name="writeTransaction"></param>
        void Replace(T row, bool writeTransaction);

        /// <summary>
        /// Updates a row at the table. The row must exist already!
        /// </summary>
        /// <param name="row">The row to update</param>
        /// <param name="writeTransaction">If true a transaction is generated at the <see cref="TransactionLog"/></param>
        void Update(T row, bool writeTransaction);

        /// <summary>
        /// Inserts rows into the table. If an ID &lt; 0 is specified an automatically generated ID will be used to add the dataset.
        /// </summary>
        /// <param name="rows">The rows.</param>
        /// <param name="writeTransaction">If true a transaction is generated at the <see cref="TransactionLog" /></param>
        void Insert(IEnumerable<T> rows, bool writeTransaction);

        /// <summary>
        /// Replaces rows at the table. The ID has to be given. This inserts (if the row does not exist) or updates (if it exists) the row.
        /// </summary>
        /// <param name="rows">The rows.</param>
        /// <param name="writeTransaction">if set to <c>true</c> [write transaction].</param>
        void Replace(IEnumerable<T> rows, bool writeTransaction);

        /// <summary>Updates rows at the table. The row must exist already!</summary>
        /// <param name="rows">The rows.</param>
        /// <param name="writeTransaction">If true a transaction is generated at the <see cref="TransactionLog" /></param>
        void Update(IEnumerable<T> rows, bool writeTransaction);
    }
}
