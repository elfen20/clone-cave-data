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
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Reflection;
using Cave.Data.Sql;
using Cave.Text;

namespace Cave.Data.Postgres
{
	/// <summary>
	/// Provides a postgre sql storage implementation.
	/// Attention: <see cref="float"/> variables stored at the mysqldatabase loose their last precision digit (a value of 1 may differ by &lt;= 0.000001f)
	/// </summary>
	public sealed class PgSqlStorage : SqlStorage
    {
        #region protected overrides

        /// <summary>
        /// Obtains FieldProperties for the Database based on requested FieldProperties
        /// </summary>
        /// <param name="field"></param>
        /// <returns></returns>
        public override FieldProperties GetDatabaseFieldProperties(FieldProperties field)
        {
            if (field == null)
			{
				throw new ArgumentNullException("LocalField");
			}

			switch (field.DataType)
            {
                case DataType.UInt8: return new FieldProperties(field, DataType.Int16, GetObjectName(field.Name));
                case DataType.UInt16: return new FieldProperties(field, DataType.Int32, GetObjectName(field.Name));
                case DataType.UInt32: return new FieldProperties(field, DataType.Int64, GetObjectName(field.Name));
                case DataType.UInt64: return new FieldProperties(field, DataType.Decimal, GetObjectName(field.Name));
            }
            return new FieldProperties(field, field.TypeAtDatabase, GetObjectName(field.Name));
        }

        /// <summary>Obtains the local <see cref="DataType" /> for the specified database fieldtype</summary>
        /// <param name="fieldType">The field type at the database</param>
        /// <param name="fieldSize">The field size at the database</param>
        /// <returns></returns>
        protected override DataType GetLocalDataType(Type fieldType, uint fieldSize)
        {
            var dataType = RowLayout.DataTypeFromType(fieldType);
            switch (dataType)
            {
                case DataType.UInt8: return DataType.Int16;
                case DataType.UInt16: return DataType.Int32;
                case DataType.UInt32: return DataType.Int64;
                case DataType.UInt64: return DataType.Decimal;
            }
            return dataType;
        }

        /// <summary>
        /// Obtains a reusable connection or creates a new one
        /// </summary>
        /// <param name="database">The database to connect to</param>
        /// <returns></returns>
        protected override string GetConnectionString(string database)
        {
            bool requireSSL = !AllowUnsafeConnections;
            if (requireSSL)
            {
                if (ConnectionString.Server == "127.0.0.1" || ConnectionString.Server == "::1" || ConnectionString.Server == "localhost")
                {
                    requireSSL = false;
                }
            }
            return
                "Host=" + ConnectionString.Server + ";" +
                "Username=" + ConnectionString.UserName + ";" +
                "Password=" + ConnectionString.Password + ";" +
                "Port=" + ConnectionString.GetPort(5432) + ";" +
                "Database=" + (database ?? "postgres") + ";" +
                "SSL Mode=" + (requireSSL ? "Require;" : "Prefer;");
        }

        /// <summary>Creates a new database connection</summary>
        /// <param name="databaseName">The name of the database</param>
        /// <returns></returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2100:SQL-Abfragen auf Sicherheitsrisiken überprüfen")]
        public override IDbConnection CreateNewConnection(string databaseName)
        {
            IDbConnection connection = base.CreateNewConnection(databaseName);
            using (IDbCommand command = connection.CreateCommand())
            {
                command.CommandText = "SET CLIENT_ENCODING TO 'UTF8'; SET NAMES 'UTF8';";
                if (LogVerboseMessages)
				{
					LogQuery(command);
				}
				//return value is not globally defined. some implementations use it correctly, some dont use it, some return positive or negative result enum values
				//so we ignore this: int affectedRows = 
				command.ExecuteNonQuery();
            }
            return connection;
        }

        #endregion

        /// <summary>
        /// Obtains the mysql storage version
        /// </summary>
        public readonly string VersionString;

        /// <summary>
        /// Obtains the mysql storage version
        /// </summary>
        public readonly Version Version;

		/// <summary>Creates a new mysql storage instance</summary>
		/// <param name="connectionString">the connection details</param>
		/// <param name="options">The options.</param>
		public PgSqlStorage(ConnectionString connectionString, DbConnectionOptions options)
            : base(connectionString, options)
        {
            VersionString = (string)QueryValue(null, null, "SELECT VERSION()");
            string[]parts = VersionString.Split(' ');
            Version = new Version(parts[1]);
            Trace.TraceInformation(string.Format("pgsql version {0}", Version));
        }

        /// <summary>Escapes a field name for direct use in a query</summary>
        /// <param name="field">The field.</param>
        /// <returns></returns>
        /// <exception cref="System.ArgumentException">FieldName is invalid!</exception>
        public override string EscapeFieldName(FieldProperties field)
        {
            return "\"" + field.NameAtDatabase + "\"";
        }

        /// <summary>
        /// Obtains a full qualified table name
        /// </summary>
        /// <param name="database"></param>
        /// <param name="table"></param>
        /// <returns></returns>
        public override string FQTN(string database, string table)
        {
            if (table.IndexOf('"') > -1)
			{
				throw new ArgumentException("Tablename is invalid!");
			}
			return "\"" + table + "\"";
        }

        /// <summary>Gets the postgresql name of the database/table/field.</summary>
        /// <param name="name">Name of the object.</param>
        /// <returns></returns>
        public string GetObjectName(string name)
        {
            return name.ReplaceInvalidChars(ASCII.Strings.Letters + ASCII.Strings.Digits, "_");
            //.ToLower().ReplaceInvalidChars(ASCII.Strings.LowercaseLetters + ASCII.Strings.Digits, "_");
        }

        /// <summary>
        /// Obtains all available database names
        /// </summary>
        public override string[] DatabaseNames
        {
            get
            {
                List<string> result = new List<string>();
                var rows = Query(null, null, "SCHEMATA", "SELECT datname FROM pg_database;");
                foreach (Row row in rows)
                {
                    result.Add((string)row.GetValue(0));
                }
                return result.ToArray();
            }
        }

        /// <summary>
        /// Checks whether the database with the specified name exists at the database or not
        /// </summary>
        /// <param name="database">The name of the database</param>
        /// <returns></returns>
        public override bool HasDatabase(string database)
        {
            object value = QueryValue(null, "SCHEMATA", "SELECT COUNT(*) FROM pg_database WHERE datname LIKE " + EscapeString(GetObjectName(database)) + ";");
            return Convert.ToInt32(value) > 0;
        }

        /// <summary>
        /// Obtains the database with the specified name
        /// </summary>
        /// <param name="database">The name of the database</param>
        /// <returns></returns>
        public override IDatabase GetDatabase(string database)
        {
            if (!HasDatabase(database))
			{
				throw new ArgumentException(string.Format("Database does not exist!"));
			}

			return new PgSqlDatabase(this, database);
        }

        /// <summary>
        /// Adds a new database with the specified name
        /// </summary>
        /// <param name="database">The name of the database</param>
        /// <returns></returns>
        public override IDatabase CreateDatabase(string database)
        {
            if (database.HasInvalidChars(ASCII.Strings.SafeName))
            {
                throw new ArgumentException("Database name contains invalid chars!");
            }
            Execute(null, null, "CREATE DATABASE " + GetObjectName(database) + " WITH OWNER = " +
                EscapeString(ConnectionString.UserName) + " ENCODING 'UTF8' CONNECTION LIMIT = -1;");
            return GetDatabase(database);
        }

        /// <summary>
        /// Removes the specified database
        /// </summary>
        /// <param name="database">The name of the database</param>
        public override void DeleteDatabase(string database)
        {
            if (database.HasInvalidChars(ASCII.Strings.SafeName))
            {
                throw new ArgumentException("Database name contains invalid chars!");
            }
            Execute(null, null, "DROP DATABASE " + GetObjectName(database) + ";");
        }

        /// <summary>
        /// Obtains whether the db connections can change the database with the Sql92 "USE Database" command.
        /// </summary>
        protected override bool DBConnectionCanChangeDataBase { get { return true; } }

        /// <summary>
        /// Initializes the needed interop assembly and type
        /// </summary>
        /// <param name="dbAdapterAssembly">Assembly containing all needed types</param>
        /// <param name="dbConnectionType">IDbConnection type used for the database</param>
        protected override void InitializeInterOp(out Assembly dbAdapterAssembly, out Type dbConnectionType)
        {
            dbConnectionType = AppDom.FindType("Npgsql.NpgsqlConnection", AppDom.LoadMode.LoadAssemblies);
            dbAdapterAssembly = dbConnectionType.Assembly;
            IDbConnection connection = (IDbConnection)Activator.CreateInstance(dbConnectionType);
            connection.Dispose();
        }

        /// <summary>
        /// true
        /// </summary>
        public override bool SupportsNamedParameters
        {
            get { return true; }
        }

        /// <summary>
        /// Obtains wether the connection supports select * groupby
        /// </summary>
        public override bool SupportsAllFieldsGroupBy
        {
            get { return true; }
        }

        /// <summary>
        /// Obtains the parameter prefix char (?)
        /// </summary>
        public override string ParameterPrefix
        {
            get { return "@_"; }
        }

        /// <summary>
        /// Gets a value indicating whether the storage engine supports native transactions with faster execution than single commands.
        /// </summary>
        /// <value>
        /// <c>true</c> if supports native transactions; otherwise, <c>false</c>.
        /// </value>
        public override bool SupportsNativeTransactions { get; } = true;

        #region precision members
        /// <summary>
        /// Obtains the maximum <see cref="float"/> precision at the value of 1.0f of this storage engine
        /// </summary>
        public override float FloatPrecision { get { return 0.00001f; } }

        /// <summary>
        /// Obtains the maximum <see cref="DateTime"/> value precision of this storage engine
        /// </summary>
        public override TimeSpan DateTimePrecision { get { return TimeSpan.FromSeconds(1); } }

        /// <summary>
        /// Obtains the maximum <see cref="TimeSpan"/> value precision of this storage engine
        /// </summary>
        public override TimeSpan TimeSpanPrecision { get { return TimeSpan.FromMilliseconds(1); } }

        /// <summary>
        /// Obtains the maximum <see cref="decimal"/> value precision of this storage engine
        /// </summary>
        public override decimal GetDecimalPrecision(float count)
        {
            if (count == 0)
			{
				count = 65.30f;
			}
			return base.GetDecimalPrecision(count);
        }
        #endregion
    }
}
