using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Reflection;
using System.Text;
using Cave.Data.Sql;

namespace Cave.Data.Microsoft
{
    /// <summary>
    /// Provides a MsSql storage implementation.
    /// </summary>
    public sealed class MsSqlStorage : SqlStorage
    {
#if DEBUG
        /// <summary>Enforce SSL encryption for database connections.</summary>
        public static bool RequireSSL { get; private set; } = true;

        /// <summary>Disables the SSL encryption for the database connections.</summary>
        /// <remarks>This cannot be used in release mode!.</remarks>
        public static void DisableSSL()
        {
            Trace.TraceError("MsSqlStorage", "SSL deactivated. This will not work in release mode!");
            RequireSSL = false;
        }
#else
        /// <summary>Enforce SSL encryption for database connections</summary>
        public const bool RequireSSL = true;
#endif

        /// <summary>
        /// Escapes a field name for direct use in a query.
        /// </summary>
        /// <param name="field"></param>
        /// <returns></returns>
        public override string EscapeFieldName(FieldProperties field)
        {
            return "[" + field.NameAtDatabase + "]";
        }

        /// <summary>
        /// Gets FieldProperties for the Database based on requested FieldProperties.
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
        /// Gets the database value for the specified local value.
        /// MsSql does not support Int8 so we patch Int8 to Int16.
        /// </summary>
        /// <param name="field">The <see cref="FieldProperties"/> of the affected field.</param>
        /// <param name="localValue">The local value to be encoded for the database.</param>
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
                var l_Max = (decimal)Math.Pow(10, l_PreDecimal - l_Decimal);
                var l_LocalValue = (decimal)localValue;

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
        /// Gets a reusable connection or creates a new one.
        /// </summary>
        /// <param name="database">The database to connect to.</param>
        /// <returns></returns>
        protected override string GetConnectionString(string database)
        {
            var l_RequireSSL = RequireSSL;
            if (l_RequireSSL)
            {
                if (ConnectionString.Server == "127.0.0.1" || ConnectionString.Server == "::1" || ConnectionString.Server == "localhost")
                {
                    l_RequireSSL = false;
                }
            }
            var result = new StringBuilder();
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

        /// <summary>Creates a new MsSql storage instance.</summary>
        /// <param name="connectionString">the connection details.</param>
        /// <param name="options">The options.</param>
        public MsSqlStorage(ConnectionString connectionString, DbConnectionOptions options)
            : base(connectionString, options)
        {
        }

        #region execute function

        /// <summary>
        /// Executes a database dependent sql statement silently.
        /// </summary>
        /// <param name="database">The affected database (dependent on the storage engine this may be null).</param>
        /// <param name="table">The affected table (dependent on the storage engine this may be null).</param>
        /// <param name="cmd">the database dependent sql statement.</param>
        /// <param name="parameters">the parameters for the sql statement.</param>
        public override int Execute(string database, string table, string cmd, params DatabaseParameter[] parameters)
        {
            if (Closed)
            {
                throw new ObjectDisposedException(ToString());
            }

            for (var i = 1; ; i++)
            {
                SqlConnection connection = GetConnection(database);
                var error = false;
                try
                {
                    using (IDbCommand command = CreateCommand(connection, cmd, parameters))
                    {
                        var result = command.ExecuteNonQuery();
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
                finally
                {
                    ReturnConnection(ref connection, error);
                }
            }
        }

        #endregion

        /// <summary>
        /// Gets a full qualified table name.
        /// </summary>
        /// <param name="database"></param>
        /// <param name="table"></param>
        /// <returns></returns>
        public override string FQTN(string database, string table)
        {
            return "[" + database + "].[dbo].[" + table + "]";
        }

        /// <summary>
        /// Gets all available database names.
        /// </summary>
        public override string[] DatabaseNames
        {
            get
            {
                var result = new List<string>();
                List<Row> rows = Query(null, "master", "sdatabases", "EXEC sdatabases;");
                foreach (Row row in rows)
                {
                    var databaseName = (string)row.GetValue(0);
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
        /// Checks whether the database with the specified name exists at the database or not.
        /// </summary>
        /// <param name="database">The name of the database.</param>
        /// <returns></returns>
        public override bool HasDatabase(string database)
        {
            if (database.HasInvalidChars(ASCII.Strings.SafeName))
            {
                throw new ArgumentException("Database name contains invalid chars!");
            }
            foreach (var name in DatabaseNames)
            {
                if (string.Equals(database, name, StringComparison.CurrentCultureIgnoreCase))
                {
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// Gets the database with the specified name.
        /// </summary>
        /// <param name="database">The name of the database.</param>
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
        /// Adds a new database with the specified name.
        /// </summary>
        /// <param name="database">The name of the database.</param>
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
        /// Removes the specified database.
        /// </summary>
        /// <param name="database">The name of the database.</param>
        public override void DeleteDatabase(string database)
        {
            if (database.HasInvalidChars(ASCII.Strings.SafeName))
            {
                throw new ArgumentException("Database name contains invalid chars!");
            }
            Execute("information_schema", "SCHEMATA", "DROP DATABASE " + database);
        }

        /// <summary>
        /// Gets whether the db connections can change the database with the Sql92 "USE Database" command.
        /// </summary>
        protected override bool DBConnectionCanChangeDataBase => true;

        /// <summary>
        /// Initializes the needed interop assembly and type.
        /// </summary>
        /// <param name="dbAdapterAssembly">Assembly containing all needed types.</param>
        /// <param name="dbConnectionType">IDbConnection type used for the database.</param>
        protected override void InitializeInterOp(out Assembly dbAdapterAssembly, out Type dbConnectionType)
        {
            Trace.TraceInformation(string.Format("Searching for MS SQL interop libraries..."));
            dbConnectionType = AppDom.FindType("System.Data.SqlClient.SqlConnection", AppDom.LoadMode.LoadAssemblies);
            dbAdapterAssembly = dbConnectionType.Assembly;
            var connection = (IDisposable)Activator.CreateInstance(dbConnectionType);
            connection.Dispose();
        }

        /// <summary>
        /// true.
        /// </summary>
        public override bool SupportsNamedParameters => true;

        /// <summary>
        /// Gets wether the connection supports select * groupby.
        /// </summary>
        public override bool SupportsAllFieldsGroupBy => true;

        /// <summary>
        /// Gets the parameter prefix char (@).
        /// </summary>
        public override string ParameterPrefix => "@";

        #region precision members

        /// <summary>
        /// Gets the maximum <see cref="DateTime"/> value precision of this storage engine.
        /// </summary>
        public override TimeSpan DateTimePrecision => TimeSpan.FromMilliseconds(4);

        /// <summary>
        /// Gets the maximum <see cref="TimeSpan"/> value precision of this storage engine.
        /// </summary>
        public override TimeSpan TimeSpanPrecision => TimeSpan.FromMilliseconds(1) - new TimeSpan(1);

        /// <summary>
        /// Gets the maximum <see cref="decimal"/> value precision of this storage engine.
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
