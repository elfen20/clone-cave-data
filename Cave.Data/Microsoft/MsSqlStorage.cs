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
using System.Text;
using Cave.Data.Sql;
using Cave.Text;

namespace Cave.Data.Microsoft
{
	/// <summary>
	/// Provides a MsSql storage implementation.
	/// </summary>
	public sealed class MsSqlStorage : SqlStorage
    {
#if DEBUG
        static bool s_RequireSSL = true;

        /// <summary>Enforce SSL encryption for database connections</summary>
        public static bool RequireSSL { get { return s_RequireSSL; } }

        /// <summary>Disables the SSL encryption for the database connections.</summary>
        /// <remarks>This cannot be used in release mode!</remarks>
        public static void DisableSSL()
        {
            Trace.TraceError("MsSqlStorage", "SSL deactivated. This will not work in release mode!");
            s_RequireSSL = false;
        }
#else
        /// <summary>Enforce SSL encryption for database connections</summary>
        public const bool RequireSSL = true;
#endif

        /// <summary>
        /// Escapes a field name for direct use in a query
        /// </summary>
        /// <param name="field"></param>
        /// <returns></returns>
        public override string EscapeFieldName(FieldProperties field)
        {
            return "[" + field.NameAtDatabase + "]";
        }

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
                case DataType.Int8: return new FieldProperties(field, DataType.Int16);
            }
            return base.GetDatabaseFieldProperties(field);
        }

        /// <summary>
        /// Obtains the database value for the specified local value.
        /// MsSql does not support Int8 so we patch Int8 to Int16.
        /// </summary>
        /// <param name="field">The <see cref="FieldProperties"/> of the affected field</param>
        /// <param name="localValue">The local value to be encoded for the database</param>
        /// <returns></returns>
        public override object GetDatabaseValue(FieldProperties field, object localValue)
        {
            if (field == null)
            {
                throw new ArgumentNullException("Field");
            }

            if (field.DataType == DataType.Int8)
            {
                return Convert.ToInt16(localValue);
            }
            if (field.DataType == DataType.Decimal)
            {
                double l_PreDecimal = 28;
                double l_Decimal = 8;
                if (field.MaximumLength != 0)
                {
                    l_PreDecimal = Math.Truncate(field.MaximumLength);
                    l_Decimal = field.MaximumLength - l_PreDecimal;
                }
                decimal l_Max = (decimal)Math.Pow(10, l_PreDecimal - l_Decimal);
                decimal l_LocalValue = (decimal)localValue;

                if (l_LocalValue >= l_Max)
                {
                    throw new ArgumentOutOfRangeException(field.Name, string.Format("Field {0} with value {1} is greater than the maximum of {2}!", field.Name, l_LocalValue, l_Max));
                }

                if (l_LocalValue <= -l_Max)
                {
                    throw new ArgumentOutOfRangeException(field.Name, string.Format("Field {0} with value {1} is smaller than the minimum of {2}!", field.Name, l_LocalValue, -l_Max));
                }
            }
            return base.GetDatabaseValue(field, localValue);
        }

        /// <summary>
        /// Obtains a reusable connection or creates a new one
        /// </summary>
        /// <param name="database">The database to connect to</param>
        /// <returns></returns>
        protected override string GetConnectionString(string database)
        {
            bool l_RequireSSL = RequireSSL;
            if (l_RequireSSL)
            {
                if (ConnectionString.Server == "127.0.0.1" || ConnectionString.Server == "::1" || ConnectionString.Server == "localhost")
                {
                    l_RequireSSL = false;
                }
            }
            StringBuilder result = new StringBuilder();
            result.Append("Server=");
            result.Append(ConnectionString.Server);
            if (ConnectionString.Port > 0)
            {
                result.Append(",");
                result.Append(ConnectionString.Port);
            }
            if (!string.IsNullOrEmpty(ConnectionString.Location))
            {
                result.Append("\\");
                result.Append(ConnectionString.Location);
            }
            result.Append(";");

            if (string.IsNullOrEmpty(ConnectionString.UserName))
            {
                result.Append("Trusted_Connection=yes;");
            }
            else
            {
                result.Append("UID=" + ConnectionString.UserName + ";");
                result.Append("PWD=" + ConnectionString.UserName + ";");
            }
            result.Append("Encrypt=" + l_RequireSSL.ToString() + ";");
            return result.ToString();
        }

		/// <summary>Creates a new MsSql storage instance</summary>
		/// <param name="connectionString">the connection details</param>
		/// <param name="options">The options.</param>
		public MsSqlStorage(ConnectionString connectionString, DbConnectionOptions options)
            : base(connectionString, options)
        {
        }

        #region execute function
        /// <summary>
        /// Executes a database dependent sql statement silently
        /// </summary>
        /// <param name="database">The affected database (dependent on the storage engine this may be null)</param>
        /// <param name="table">The affected table (dependent on the storage engine this may be null)</param>
        /// <param name="cmd">the database dependent sql statement</param>
        /// <param name="parameters">the parameters for the sql statement</param>
        public override int Execute(string database, string table, string cmd, params DatabaseParameter[] parameters)
        {
            if (Closed)
            {
                throw new ObjectDisposedException(ToString());
            }

            for (int i = 1; ; i++)
            {
                SqlConnection connection = GetConnection(database);
                bool error = false;
                try
                {
                    using (IDbCommand command = CreateCommand(connection, cmd, parameters))
                    {
                        int result = command.ExecuteNonQuery();
                        if (result == 0)
                        {
                            throw new InvalidOperationException();
                        }

                        return result;
                    }
                }
                catch (Exception ex)
                {
                    error = true;
                    if (i > MaxErrorRetries)
                    {
                        throw;
                    }

                    Trace.TraceInformation("<red>{3}<default> Error during Execute(<cyan>{0}<default>, <cyan>{1}<default>) -> <yellow>retry {2}", database, table, i, ex.Message);
                }
                finally { ReturnConnection(ref connection, error); }
            }
        }

        #endregion

        /// <summary>
        /// Obtains a full qualified table name
        /// </summary>
        /// <param name="database"></param>
        /// <param name="table"></param>
        /// <returns></returns>
        public override string FQTN(string database, string table)
        {
            return "[" + database + "].[dbo].[" + table + "]";
        }

        /// <summary>
        /// Obtains all available database names
        /// </summary>
        public override string[] DatabaseNames
        {
            get
            {
                List<string> result = new List<string>();
                var rows = Query(null, "master", "sdatabases", "EXEC sdatabases;");
                foreach (Row row in rows)
                {
                    string databaseName = (string)row.GetValue(0);
                    switch (databaseName)
                    {
                        case "master":
                        case "model":
                        case "msdb":
                        case "tempdb":
                            continue;

                        default:
                            result.Add(databaseName);
                            continue;
                    }
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
            if (database.HasInvalidChars(ASCII.Strings.SafeName))
            {
                throw new ArgumentException("Database name contains invalid chars!");
            }
            foreach (string name in DatabaseNames)
            {
                if (string.Equals(database, name, StringComparison.CurrentCultureIgnoreCase))
                {
                    return true;
                }
            }
            return false;
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
                throw new DataException(string.Format("Database does not exist!"));
            }

            return new MsSqlDatabase(this, database);
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
            Execute("information_schema", "SCHEMATA", "CREATE DATABASE " + database);
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
            Execute("information_schema", "SCHEMATA", "DROP DATABASE " + database);
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
            Trace.TraceInformation(string.Format("Searching for MS SQL interop libraries..."));
            dbConnectionType = AppDom.FindType("System.Data.SqlClient.SqlConnection", AppDom.LoadMode.LoadAssemblies);
            dbAdapterAssembly = dbConnectionType.Assembly;
            IDisposable connection = (IDisposable)Activator.CreateInstance(dbConnectionType);
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
        /// Obtains the parameter prefix char (@)
        /// </summary>
        public override string ParameterPrefix
        {
            get { return "@"; }
        }

        #region precision members
        /// <summary>
        /// Obtains the maximum <see cref="DateTime"/> value precision of this storage engine
        /// </summary>
        public override TimeSpan DateTimePrecision { get { return TimeSpan.FromMilliseconds(4); } }

        /// <summary>
        /// Obtains the maximum <see cref="TimeSpan"/> value precision of this storage engine
        /// </summary>
        public override TimeSpan TimeSpanPrecision { get { return TimeSpan.FromMilliseconds(1) - new TimeSpan(1); } }

        /// <summary>
        /// Obtains the maximum <see cref="decimal"/> value precision of this storage engine
        /// </summary>
        public override decimal GetDecimalPrecision(float count)
        {
            if (count == 0)
            {
                count = 28.08f;
            }

            return base.GetDecimalPrecision(count);
        }
        #endregion
    }
}
