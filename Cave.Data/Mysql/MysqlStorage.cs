using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Reflection;
using System.Text;
using Cave.Data.Sql;

namespace Cave.Data.Mysql
{
    /// <summary>
    /// Provides a mysql storage implementation.
    /// Attention: <see cref="float"/> variables stored at the mysqldatabase loose their last precision digit (a value of 1 may differ by &lt;= 0.000001f).
    /// </summary>
    public sealed class MySqlStorage : SqlStorage
    {
        #region protected overrides

        /// <summary>Converts a database value into a local value.</summary>
        /// <param name="field">The <see cref="FieldProperties" /> of the affected field.</param>
        /// <param name="databaseValue">The value retrieved from the database.</param>
        /// <returns>Returns a value for local use.</returns>
        /// <exception cref="ArgumentNullException">Field.</exception>
        public override object GetLocalValue(FieldProperties field, object databaseValue)
        {
            if (field == null)
            {
                throw new ArgumentNullException("Field");
            }

            if (databaseValue != DBNull.Value)
            {
                switch (field.DataType)
                {
                    case DataType.Double:
                    {
                        var d = Convert.ToDouble(databaseValue);
                        if (d >= double.MaxValue)
                        {
                            return double.PositiveInfinity;
                        }
                        return d <= double.MinValue ? double.NegativeInfinity : (object)d;
                    }
                    case DataType.Single:
                    {
                        var f = Convert.ToSingle(databaseValue);
                        if (f >= float.MaxValue)
                        {
                            return float.PositiveInfinity;
                        }
                        return f <= float.MinValue ? float.NegativeInfinity : (object)f;
                    }
                }
            }

            return base.GetLocalValue(field, databaseValue);
        }

        /// <summary>
        /// Converts a local value into a database value.
        /// </summary>
        /// <param name="field">The <see cref="FieldProperties" /> of the affected field.</param>
        /// <param name="localValue">The local value to be encoded for the database.</param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException">You can not store 4 byte utf-8 characters to mysql prior version 5.5.3!.</exception>
        public override object GetDatabaseValue(FieldProperties field, object localValue)
        {
            if (field == null)
            {
                throw new ArgumentNullException("Field");
            }

            switch (field.DataType)
            {
                case DataType.String:
                {
                    if (localValue == null)
                    {
                        return null;
                    }

                    if (!SupportsFullUTF8)
                    {
                        // dirty hack: check mysql 3 byte utf-8
                        var value = (string)localValue;
                        foreach (var c in value)
                        {
                            if (Encoding.UTF8.GetByteCount(new char[] { c }) > 3)
                            {
                                throw new NotSupportedException("You can not store 4 byte utf-8 characters to mysql prior version 5.5.3!");
                            }
                        }
                        return DataEncoding.GetBytes(value);
                    }
                    break;
                }
                case DataType.Double:
                {
                    var d = Convert.ToDouble(localValue);
                    if (double.IsPositiveInfinity(d))
                    {
                        return double.MaxValue;
                    }

                    return double.IsNegativeInfinity(d) ? double.MinValue : (object)d;
                }
                case DataType.Single:
                {
                    var f = Convert.ToSingle(localValue);
                    if (float.IsPositiveInfinity(f))
                    {
                        return float.MaxValue;
                    }

                    return float.IsNegativeInfinity(f) ? float.MinValue : (object)f;
                }
            }

            return base.GetDatabaseValue(field, localValue);
        }

        /// <summary>
        /// Gets the <see cref="DataType"/> for the specified fieldtype.
        /// </summary>
        /// <param name="fieldType">The field type.</param>
        /// <param name="fieldSize">The size of the field.</param>
        /// <returns></returns>
        protected override DataType GetLocalDataType(Type fieldType, uint fieldSize)
        {
            if (fieldType == null)
            {
                throw new ArgumentNullException("FieldType");
            }

            DataType dataType;
            if (fieldType.Name == "MySqlDateTime")
            {
                // fix mysql date time
                dataType = DataType.DateTime;
            }
            else
            {
                // handle all default types
                dataType = RowLayout.DataTypeFromType(fieldType);

                // handle mysql bool
                if (fieldSize == 1)
                {
                    if ((dataType == DataType.User) || (dataType == DataType.UInt64))
                    {
                        // fix mysql bool data type
                        dataType = DataType.Bool;
                    }
                }
            }
            return dataType;
        }

        /// <summary>
        /// Gets a reusable connection or creates a new one.
        /// </summary>
        /// <param name="database">The database to connect to.</param>
        /// <returns></returns>
        protected override string GetConnectionString(string database)
        {
            var requireSSL = !AllowUnsafeConnections;
            if (requireSSL)
            {
                if (ConnectionString.Server == "127.0.0.1" || ConnectionString.Server == "::1" || ConnectionString.Server == "localhost")
                {
                    requireSSL = false;
                }
            }
            return
                    "Allow User Variables=true;" +
                    "Server=" + ConnectionString.Server + ";" +
                    "Uid=" + ConnectionString.UserName + ";" +
                    "Pwd=" + ConnectionString.Password + ";" +
                    "Port=" + ConnectionString.GetPort(3306) + ";" +
                    "CharSet=" + (SupportsFullUTF8 ? "utf8mb4" : "utf8") + ";" +

                    // "Protocol=socket;" +
                    "Allow Zero Datetime=true;" +
                    (requireSSL ? "SslMode=Required;" : "SslMode=Preferred;");
        }

        /// <summary>Creates a new database connection.</summary>
        /// <param name="databaseName">The name of the database.</param>
        /// <returns></returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2100:SQL-Abfragen auf Sicherheitsrisiken überprüfen")]
        public override IDbConnection CreateNewConnection(string databaseName)
        {
            IDbConnection connection = base.CreateNewConnection(databaseName);
            using (IDbCommand command = connection.CreateCommand())
            {
                command.CommandText = string.Format("SET NAMES `{0}` COLLATE `{0}_unicode_ci`; SET CHARACTER SET `{0}`;", CharacterSet);
                if (LogVerboseMessages)
                {
                    LogQuery(command);
                }

                // return value is not globally defined. some implementations use it correctly, some dont use it, some return positive or negative result enum values
                // so we ignore this: int affectedRows =
                command.ExecuteNonQuery();
            }
            return connection;
        }

        #endregion

        /// <summary>Gets the data encoding.</summary>
        /// <value>The data encoding.</value>
        public Encoding DataEncoding { get; private set; } = Encoding.UTF8;

        /// <summary>Gets a value indicating whether the server instance supports full utf8 or only the 3 byte (BMP characters only) character set.</summary>
        /// <value><c>true</c> if [supports full utf8]; otherwise, <c>false</c>.</value>
        public bool SupportsFullUTF8 { get; private set; }

        /// <summary>Gets the character set.</summary>
        /// <value>The character set.</value>
        public string CharacterSet { get; } = "utf8";

        /// <summary>
        /// Gets the mysql storage version.
        /// </summary>
        public string VersionString { get; }

        /// <summary>
        /// Gets the mysql storage version.
        /// </summary>
        public Version Version { get; }

        /// <summary>Creates a new mysql storage instance.</summary>
        /// <param name="connectionString">the connection details.</param>
        /// <param name="options">The options.</param>
        public MySqlStorage(ConnectionString connectionString, DbConnectionOptions options)
            : base(connectionString, options)
        {
            VersionString = (string)QueryValue(null, null, "SELECT VERSION()");
            if (VersionString.IndexOf('-') > -1)
            {
                Version = new Version(VersionString.Substring(0, VersionString.IndexOf('-')));
            }
            else
            {
                Version = new Version(VersionString);
            }
            if (Version < new Version(5, 5, 3))
            {
                SupportsFullUTF8 = false;
                CharacterSet = "utf8";
            }
            else
            {
                SupportsFullUTF8 = true;
                CharacterSet = "utf8mb4";
            }
            Trace.TraceInformation(string.Format("mysql version <cyan>{0}<default> supports full utf-8 {1}", Version, (SupportsFullUTF8 ? "<green>" : "<red>") + SupportsFullUTF8));
            ClearCachedConnections();
        }

        /// <summary>
        /// Escapes a field name for direct use in a query.
        /// </summary>
        /// <param name="field"></param>
        /// <returns></returns>
        public override string EscapeFieldName(FieldProperties field)
        {
            return "`" + field.NameAtDatabase + "`";
        }

        /// <summary>
        /// Gets a full qualified table name.
        /// </summary>
        /// <param name="database"></param>
        /// <param name="table"></param>
        /// <returns></returns>
        public override string FQTN(string database, string table)
        {
            return "`" + database + "`.`" + table + "`";
        }

        /// <summary>
        /// Gets all available database names.
        /// </summary>
        public override string[] DatabaseNames
        {
            get
            {
                var result = new List<string>();
                List<Row> rows = Query(null, "information_schema", "SCHEMATA", "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA;");
                foreach (Row row in rows)
                {
                    result.Add((string)row.GetValue(0));
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
            var value = QueryValue("information_schema", "SCHEMATA", "SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME LIKE " + EscapeString(database) + ";");
            return Convert.ToInt32(value) > 0;
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
                throw new ArgumentException(string.Format("Database does not exist!"));
            }

            return new MySqlDatabase(this, database);
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
            Execute("information_schema", "SCHEMATA", $"CREATE DATABASE `{database}` CHARACTER SET {CharacterSet} COLLATE {CharacterSet}_general_ci;");
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
            Execute("information_schema", "SCHEMATA", $"DROP DATABASE {database};");
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
            Trace.TraceInformation("Searching for mySQL interop libraries...");
            dbConnectionType = AppDom.FindType("MySql.Data.MySqlClient.MySqlConnection", AppDom.LoadMode.LoadAssemblies);
            dbAdapterAssembly = DbConnectionType.Assembly;
            var connection = (IDbConnection)Activator.CreateInstance(dbConnectionType);
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
        /// Gets the parameter prefix char (?).
        /// </summary>
        public override string ParameterPrefix => "?";

        #region precision members

        /// <summary>
        /// Gets the maximum <see cref="float"/> precision at the value of 1.0f of this storage engine.
        /// </summary>
        public override float FloatPrecision => 0.00001f;

        /// <summary>
        /// Gets the maximum <see cref="DateTime"/> value precision of this storage engine.
        /// </summary>
        public override TimeSpan DateTimePrecision => TimeSpan.FromSeconds(1);

        /// <summary>
        /// Gets the maximum <see cref="TimeSpan"/> value precision of this storage engine.
        /// </summary>
        public override TimeSpan TimeSpanPrecision => TimeSpan.FromMilliseconds(1);

        /// <summary>
        /// Gets the maximum <see cref="decimal"/> value precision of this storage engine.
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
