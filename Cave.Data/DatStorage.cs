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
using System.IO;
using Cave.IO;
using Cave.Text;

namespace Cave.Data
{
	/// <summary>
	/// Provides a simple directory based storage engine
	/// </summary>
	public sealed class DatStorage : FileStorage
    {
		/// <summary>
		/// Opens a file storage.
		/// <para>
		/// Following formats are supported:<br />
		/// file://server/relativepath<br />
		/// file:absolutepath<br /></para>
		/// </summary>
		/// <param name="connectionString">The connection string.</param>
		/// <param name="options">The options.</param>
		public DatStorage(string connectionString, DbConnectionOptions options)
            : this(ConnectionString.Parse(connectionString), options)
        {
        }

		/// <summary>
		/// Opens a file storage.
		/// <para>
		/// Following formats are supported:<br />
		/// file://server/relativepath<br />
		/// file:absolutepath<br /></para>
		/// </summary>
		/// <param name="connectionString"></param>
		/// <param name="options"></param>
		public DatStorage(ConnectionString connectionString, DbConnectionOptions options)
            : base(connectionString, options)
        {
        }

        /// <summary>
        /// Obtains the database with the specified name
        /// </summary>
        /// <param name="database">Name of the database</param>
        /// <returns></returns>
        public override IDatabase GetDatabase(string database)
        {
            if (Closed)
            {
                throw new ObjectDisposedException(ToString());
            }

            return new DatDatabase(this, Path.Combine(Folder, database));
        }

        /// <summary>
        /// Gets a value indicating whether the storage engine supports native transactions with faster execution than single commands.
        /// </summary>
        /// <value>
        /// <c>true</c> if supports native transactions; otherwise, <c>false</c>.
        /// </value>
        public override bool SupportsNativeTransactions { get; } = false;
    }
}
