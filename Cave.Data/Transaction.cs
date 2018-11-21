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

using System;
namespace Cave.Data
{
    /// <summary>
    /// Proivides transactions for database rows
    /// </summary>
    public sealed class Transaction
    {
        /// <summary>Creates a new transaction</summary>
        /// <param name="type">Type</param>
        /// <param name="id">The identifier.</param>
        /// <param name="row">Data</param>
        /// <returns></returns>
        /// <exception cref="System.ArgumentOutOfRangeException">Row ID is invalid!</exception>
        public static Transaction Create(TransactionType type, long id, Row row)
        {
            if (id <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(row), "Row ID is invalid!");
            }

            return new Transaction(type, id, row);
        }

        /// <summary>
        /// Creates a new "deleted row" transaction
        /// </summary>
        /// <param name="id">id of the row</param>
        /// <returns></returns>
        public static Transaction Deleted(long id)
        {
            return new Transaction(TransactionType.Deleted, id);
        }

        /// <summary>Creates a new "inserted row" transaction using the specified ID of the inserted row</summary>
        /// <param name="id">The identifier.</param>
        /// <param name="row">row data of the inserted row</param>
        /// <returns></returns>
        public static Transaction Inserted(long id, Row row)
        {
            return new Transaction(TransactionType.Inserted, id, row);
        }

        /// <summary>
        /// Creates a new "insert row" transaction using a new ID at the target table
        /// </summary>
        /// <param name="row">row data of the row to be inserted</param>
        /// <returns></returns>
        public static Transaction InsertNew(Row row)
        {
            return new Transaction(TransactionType.Inserted, -1, row);
        }

        /// <summary>Creates a new "inserted row" transaction</summary>
        /// <param name="id">The identifier.</param>
        /// <param name="row">row data of the inserted row</param>
        /// <returns></returns>
        public static Transaction Replaced(long id, Row row)
        {
            return new Transaction(TransactionType.Replaced, id, row);
        }

        /// <summary>Creates a new "updated row" transaction</summary>
        /// <param name="id">The identifier.</param>
        /// <param name="row">row data of the updated row</param>
        /// <returns></returns>
        public static Transaction Updated(long id, Row row)
        {
            return new Transaction(TransactionType.Updated, id, row);
        }

        /// <summary>
        /// Obtains the <see cref="TransactionType"/>
        /// </summary>
        public TransactionType Type { get; }

        /// <summary>
        /// Obtains the ID of the entry
        /// </summary>
        public long ID { get; }

        /// <summary>
        /// Obtains the full row data (only set on <see cref="TransactionType.Updated"/> and <see cref="TransactionType.Inserted"/>)
        /// </summary>
        public Row Row { get; }

        /// <summary>Gets the created date time.</summary>
        /// <value>The created date time.</value>
        public DateTime Created { get; } = DateTime.UtcNow;


        Transaction(TransactionType type, long id)
        {
            Type = type;
            ID = id;
        }

        Transaction(TransactionType type, long id, Row row)
        {
            Type = type;
            ID = id;
            Row = row;
        }

        /// <summary>
        /// Provides a string "ID &lt;ID&gt; &lt;Type&gt;"
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            if (Type == TransactionType.Deleted)
            {
                return "ID <" + ID + "> <" + Type + ">";
            }

            return "ID <" + ID + "> <" + Type + "> " + Row.ToString();
        }

        /// <summary>
        /// Obtains the HashCode based on the ID of the dataset
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            return unchecked((int)((ID >> 32) ^ (ID & 0xFFFFFFFF)));
        }
    }
}
