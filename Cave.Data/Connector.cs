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
using Cave.Data.Microsoft;
using Cave.Data.Mysql;
using Cave.Data.Postgres;
using Cave.Data.SQLite;
using Cave.IO;

namespace Cave.Data
{
    /// <summary>
    /// Connects to different database types
    /// </summary>
    public static class Connector
    {
		/// <summary>Connects to a database storage</summary>
		/// <param name="connectionString">The connection string.</param>
		/// <param name="options">The options.</param>
		/// <returns>Returns a new storage connection.</returns>
		/// <exception cref="NotSupportedException"></exception>
		public static IStorage ConnectStorage(ConnectionString connectionString, DbConnectionOptions options)
        {
            switch (connectionString.ConnectionType)
            {
                case ConnectionType.MEMORY: return new MemoryStorage();
                case ConnectionType.FILE: return new DatStorage(connectionString, options);
                case ConnectionType.MYSQL: return new MySqlStorage(connectionString, options);
                case ConnectionType.MSSQL: return new MsSqlStorage(connectionString, options);
                case ConnectionType.SQLITE: return new SQLiteStorage(connectionString, options);
                case ConnectionType.PGSQL: return new PgSqlStorage(connectionString, options);
                default: throw new NotSupportedException(string.Format("Unknown database provider '{0}'!", connectionString.Protocol));
            }
        }

		/// <summary>Connects to a database using the specified <see cref="ConnectionString" />.</summary>
		/// <param name="connection">The ConnectionString.</param>
		/// <param name="options">The database connection options.</param>
		/// <returns>Returns a new database connection.</returns>
		/// <exception cref="ArgumentException">Missing database name at connection string!</exception>
		/// <exception cref="Exception">Missing database name at connection string!</exception>
		public static IDatabase ConnectDatabase(ConnectionString connection, DbConnectionOptions options = DbConnectionOptions.None)
        {
            IStorage storage = ConnectStorage(connection, options);
            if (connection.Location == null)
            {
                throw new ArgumentOutOfRangeException("connection", "Database name not specified!");
            }

            string[] parts = connection.Location.Split(new char[] { '/' }, StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length < 1)
            {
                throw new ArgumentException("Missing database name at connection string!");
            }

            return storage.GetDatabase(parts[0], 0 != (options & DbConnectionOptions.AllowCreate));
        }

		/// <summary>Connects to a database using the specified <see cref="ISettings" />.</summary>
		/// <param name="settings">The settings.</param>
		/// <param name="databaseName">Name of the database.</param>
		/// <param name="options">The options.</param>
		/// <returns>Returns a new database connection.</returns>
		public static IDatabase ConnectDatabase(ISettings settings, string databaseName = null, DbConnectionOptions options = DbConnectionOptions.None)
        {
            //use [Database]Connection if present
            var connectionString = settings.ReadSetting("Database", "Connection");
            if (connectionString != null)
            {
                return ConnectDatabase(connectionString);
            }

            //not present, read database name
            if (databaseName == null)
            {
                databaseName = settings.ReadString("Database", "Database");
            }

            //prepare database name if none specified            
            if (databaseName == null)
            {
                string serviceName = AssemblyVersionInfo.Program.Product;
                string service = serviceName.GetValidChars(ASCII.Strings.SafeName).ToLower();
                string programID = Base32.Safe.Encode(AppDom.ProgramID);
                string machine = Environment.MachineName.Split('.')[0].GetValidChars(ASCII.Strings.SafeName).ToLower();
                databaseName = $"{service}_{machine}_{programID}";
            }

            //read the [Database] section
            string type = settings.ReadString("Database", "Type");
            string user = settings.ReadString("Database", "Username");
            string pass = settings.ReadString("Database", "Password");
            string server = settings.ReadString("Database", "Server");
            return ConnectDatabase($"{type}://{user}:{pass}@{server}/{databaseName}", options);
        }
    }
}
