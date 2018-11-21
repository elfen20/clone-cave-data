#region CopyRight 2018
/*
    Copyright (c) 2003-2018 Andreas Rohleder (andreas@rohleder.cc)
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
#endregion
#region Authors & Contributors
/*
   Author:
     Andreas Rohleder <andreas@rohleder.cc>

   Contributors:
 */
#endregion

using Cave.Text;

namespace Cave.Data
{
    /// <summary>
    /// Provides a generic table connector interface
    /// </summary>
    public interface ITableConnector
    {
        /// <summary>Gets the database connection used.</summary>
        /// <value>The database.</value>
        IDatabase Database { get; }

        /// <summary>Gets all connected tables of this instance.</summary>
        /// <value>The tables.</value>
        ITable[] Tables { get; }

        /// <summary>Gets the current mode of the connector.</summary>
        /// <value>The <see cref="TableConnectorMode"/>.</value>
        TableConnectorMode Mode { get; }

        /// <summary>Connects to the specified database using the specified <see cref="TableConnectorMode"/>.</summary>
        /// <param name="mode">The <see cref="TableConnectorMode"/></param>
        /// <param name="database">The database.</param>
        void Connect(TableConnectorMode mode, IDatabase database);

        /// <summary>Closes all tables, the database and the storage engine.</summary>
        void Close(bool closeStorage);
    }
}
