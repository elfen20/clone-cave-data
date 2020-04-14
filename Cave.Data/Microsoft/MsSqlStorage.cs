using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Text;
using Cave.Data.Sql;

namespace Cave.Data.Microsoft
{
    /// <summary>
    /// Provides a MsSql storage implementation.
    /// </summary>
    public sealed class MsSqlStorage : SqlStorage
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="MsSqlStorage"/> class.
        /// </summary>
        /// <param name="connectionString">the connection details.</param>
        /// <param name="flags">The connection flags.</param>
        public MsSqlStorage(ConnectionString connectionString, ConnectionFlags flags = default)
            : base(connectionString, flags)
        {
        }

        #region properties

        /// <inheritdoc/>
        public override TimeSpan DateTimePrecision => TimeSpan.FromMilliseconds(4);

        /// <inheritdoc/>
        public override TimeSpan TimeSpanPrecision => TimeSpan.FromMilliseconds(1) - new TimeSpan(1);

        /// <inheritdoc/>
        public override string[] DatabaseNames
        {
            get
            {
                var result = new List<string>();
                RowLayout layout = null;
                var rows = Query("EXEC sdatabases;", ref layout, "master", "sdatabases");
                foreach (Row row in rows)
                {
                    var databaseName = (string)row[0];
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

        /// <inheritdoc/>
        public override bool SupportsNamedParameters => true;

        /// <inheritdoc/>
        public override bool SupportsAllFieldsGroupBy => true;

        /// <inheritdoc/>
        public override string ParameterPrefix => "@";

        /// <inheritdoc/>
        protected internal override bool DBConnectionCanChangeDataBase => true;

        #endregion

        /// <inheritdoc/>
        public override string EscapeFieldName(IFieldProperties field) => "[" + field.NameAtDatabase + "]";

        /// <inheritdoc/>
        public override IFieldProperties GetDatabaseFieldProperties(IFieldProperties field)
        {
            if (field == null)
            {
                throw new ArgumentNullException("LocalField");
            }

            switch (field.DataType)
            {
                case DataType.Int8:
                    var result = field.Clone();
                    result.TypeAtDatabase = DataType.Int16;
                    return result;
            }
            return base.GetDatabaseFieldProperties(field);
        }

        /// <inheritdoc/>
        public override object GetDatabaseValue(IFieldProperties field, object localValue)
        {
            if (field == null)
            {
                throw new ArgumentNullException(nameof(field));
            }

            if (field.DataType == DataType.Int8)
            {
                return Convert.ToInt16(localValue);
            }
            if (field.DataType == DataType.Decimal)
            {
                double preDecimal = 28;
                double valDecimal = 8;
                if (field.MaximumLength != 0)
                {
                    preDecimal = Math.Truncate(field.MaximumLength);
                    valDecimal = field.MaximumLength - preDecimal;
                }
                var max = (decimal)Math.Pow(10, preDecimal - valDecimal);
                var val = (decimal)localValue;

                if (val >= max)
                {
                    throw new ArgumentOutOfRangeException(field.Name, string.Format("Field {0} with value {1} is greater than the maximum of {2}!", field.Name, localValue, max));
                }

                if (val <= -max)
                {
                    throw new ArgumentOutOfRangeException(field.Name, string.Format("Field {0} with value {1} is smaller than the minimum of {2}!", field.Name, localValue, -max));
                }
            }
            return base.GetDatabaseValue(field, localValue);
        }

        #region execute function

        /// <inheritdoc/>
        public override int Execute(SqlCmd cmd, string database = null, string table = null)
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
                    using (IDbCommand command = CreateCommand(connection, cmd))
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

        /// <inheritdoc/>
        public override string FQTN(string database, string table)
        {
            return "[" + database + "].[dbo].[" + table + "]";
        }

        /// <inheritdoc/>
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

        /// <inheritdoc/>
        public override IDatabase GetDatabase(string database)
        {
            if (!HasDatabase(database))
            {
                throw new DataException(string.Format("Database does not exist!"));
            }

            return new MsSqlDatabase(this, database);
        }

        /// <inheritdoc/>
        public override IDatabase CreateDatabase(string database)
        {
            if (database.HasInvalidChars(ASCII.Strings.SafeName))
            {
                throw new ArgumentException("Database name contains invalid chars!");
            }
            Execute(database: "information_schema", table: "SCHEMATA", cmd: "CREATE DATABASE " + database);
            return GetDatabase(database);
        }

        /// <inheritdoc/>
        public override void DeleteDatabase(string database)
        {
            if (database.HasInvalidChars(ASCII.Strings.SafeName))
            {
                throw new ArgumentException("Database name contains invalid chars!");
            }
            Execute(database: "information_schema", table: "SCHEMATA", cmd: "DROP DATABASE " + database);
        }

        /// <inheritdoc/>
        public override decimal GetDecimalPrecision(float count)
        {
            if (count == 0)
            {
                count = 28.08f;
            }

            return base.GetDecimalPrecision(count);
        }

        /// <inheritdoc/>
        protected override IDbConnection GetDbConnectionType()
        {
            var type = Type.GetType("System.Data.SqlClient.SqlConnection, System.Data.SqlClient", true);
            return (IDbConnection)Activator.CreateInstance(type);
        }

        /// <inheritdoc/>
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
            result.Append("Encrypt=" + requireSSL.ToString() + ";");
            return result.ToString();
        }
    }
}
