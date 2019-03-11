using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using Cave.Data.Sql;

namespace Cave.Data.SQLite
{
    /// <summary>
    /// Provides a sqlite storage implementation.
    /// </summary>
    public sealed class SQLiteStorage : SqlStorage
    {
        const string s_ConnectionString = "Data Source={0}";

        #region static functions
        /// <summary>
        /// Obtains the database type for the specified field type.
        /// Sqlite does not implement all different sql92 types directly instead they are reusing only 4 different types.
        /// So we have to check only the sqlite value types and convert to the dotnet type.
        /// </summary>
        /// <param name="dataType">Local DataType.</param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException">FieldType.</exception>
        public static DataType GetDatabaseDataType(DataType dataType)
        {
            switch (GetValueType(dataType))
            {
                case SQLiteValueType.BLOB: return DataType.Binary;
                case SQLiteValueType.INTEGER: return DataType.Int64;
                case SQLiteValueType.REAL: return DataType.Double;
                case SQLiteValueType.TEXT: return DataType.String;
                default: throw new NotImplementedException(string.Format("FieldType {0} is not implemented!", dataType));
            }
        }

        /// <summary>
        /// Obtains the sqlite value type of the specified datatype.
        /// </summary>
        /// <param name="dataType"></param>
        /// <returns></returns>
        public static SQLiteValueType GetValueType(DataType dataType)
        {
            switch (dataType)
            {
                case DataType.Binary:
                    return SQLiteValueType.BLOB;

                case DataType.Bool:
                case DataType.Enum:
                case DataType.Int8:
                case DataType.Int16:
                case DataType.Int32:
                case DataType.Int64:
                case DataType.UInt8:
                case DataType.UInt16:
                case DataType.UInt32:
                case DataType.UInt64:
                    return SQLiteValueType.INTEGER;

                case DataType.DateTime:
                case DataType.Char:
                case DataType.String:
                case DataType.User:
                    return SQLiteValueType.TEXT;

                case DataType.TimeSpan:
                case DataType.Decimal:
                case DataType.Double:
                case DataType.Single:
                    return SQLiteValueType.REAL;

                default:
                    throw new NotImplementedException(string.Format("DataType {0} is not implemented!", dataType));
            }
        }
        #endregion

        #region protected overrides
        /// <summary>
        /// Obtains whether the db connections can change the database with the Sql92 "USE Database" command.
        /// </summary>
        protected override bool DBConnectionCanChangeDataBase => false;

        /// <summary>
        /// Initializes the needed interop assembly and type.
        /// </summary>
        /// <param name="dbAdapterAssembly">Assembly containing all needed types.</param>
        /// <param name="dbConnectionType">IDbConnection type used for the database.</param>
        protected override void InitializeInterOp(out Assembly dbAdapterAssembly, out Type dbConnectionType)
        {
            Trace.TraceInformation(string.Format("Searching for SQLite interop libraries..."));
            Type[] types = new Type[]
            {
                Type.GetType("System.Data.SQLite.SQLiteConnection, System.Data.SQLite", false),
                Type.GetType("Mono.Data.SQLite.SQLiteConnection, Mono.Data.SQLite", false),
            };
            foreach (Type type in types)
            {
                if (type == null)
                {
                    continue;
                }

                Trace.TraceInformation(string.Format("Trying to use Type {0}", type));
                try
                {
                    dbAdapterAssembly = type.Assembly;
                    dbConnectionType = type;
                    IDbConnection connection = (IDbConnection)Activator.CreateInstance(dbConnectionType);
                    connection.Dispose();
                    Trace.TraceInformation(string.Format("Using {0}", dbAdapterAssembly));
                    return;
                }
                catch (Exception ex)
                {
                    Trace.TraceInformation(string.Format("Cannot use type {0}!", type), ex);
                    continue;
                }
            }
            throw new NotSupportedException(string.Format("Could not find any working *.Data.SQLite.SQLiteConnection!"));
        }

        /// <summary>
        /// Obtains a connection string for the <see cref="SqlStorage.DbConnectionType"/>.
        /// </summary>
        /// <param name="database">The database to connect to.</param>
        /// <returns></returns>
        protected override string GetConnectionString(string database)
        {
            if (string.IsNullOrEmpty(database))
            {
                throw new ArgumentNullException("Database");
            }

            string path = GetFileName(database);
            return string.Format(s_ConnectionString, path);
        }
        #endregion

        /// <summary>
        /// Obtains the fileName for the specified database name.
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        string GetFileName(string database)
        {
            return Path.GetFullPath(Path.Combine(ConnectionString.Location, database + ".db"));
        }

        /// <summary>Creates a new sqlite storage instance.</summary>
        /// <param name="connectionString">the connection details.</param>
        /// <param name="options">The options.</param>
        public SQLiteStorage(ConnectionString connectionString, DbConnectionOptions options)
            : base(connectionString, options)
        {
        }

        #region IStorage implementation

        /// <summary>
        /// Obtains FieldProperties for the Database based on requested FieldProperties.
        /// </summary>
        /// <param name="field"></param>
        /// <returns></returns>
        public override FieldProperties GetDatabaseFieldProperties(FieldProperties field)
        {
            if (field == null)
            {
                throw new ArgumentNullException("LocalField");
            }

            DataType dataType = GetDatabaseDataType(field.DataType);
            return field.DataType == dataType ? field : new FieldProperties(field, dataType);
        }

        /// <summary>
        /// Converts a database value into a local value.
        /// Sqlite does not implement all different sql92 types directly instead they are reusing only 4 different types.
        /// So we have to check only the sqlite value types and convert to the dotnet type.
        /// </summary>
        /// <param name="field">The <see cref="FieldProperties"/> of the affected field.</param>
        /// <param name="databaseValue">The value retrieved from the database.</param>
        /// <returns>Returns a value for local use.</returns>
        public override object GetLocalValue(FieldProperties field, object databaseValue)
        {
            if (field == null)
            {
                throw new ArgumentNullException("Field");
            }

            if (field.DataType == DataType.Decimal)
            {
                //unbox double and convert
                double d = (double)databaseValue;
                if (d >= (double)decimal.MaxValue)
                {
                    return decimal.MaxValue;
                }

                return d <= (double)decimal.MinValue ? decimal.MinValue : (object)(decimal)d;
            }
            return base.GetLocalValue(field, databaseValue);
        }

        /// <summary>
        /// Escapes a field name for direct use in a query.
        /// </summary>
        /// <param name="field"></param>
        /// <returns></returns>
        public override string EscapeFieldName(FieldProperties field)
        {
            if (field == null)
            {
                throw new ArgumentNullException("FieldName");
            }

            return "[" + field.NameAtDatabase + "]";
        }

        /// <summary>
        /// Obtains a full qualified table name.
        /// </summary>
        /// <param name="database"></param>
        /// <param name="table"></param>
        /// <returns></returns>
        public override string FQTN(string database, string table)
        {
            return table;
        }

        /// <summary>
        /// Obtains all available database names.
        /// </summary>
        public override string[] DatabaseNames
        {
            get
            {
                List<string> result = new List<string>();
                foreach (string directory in Directory.GetFiles(ConnectionString.Location, "*.db"))
                {
                    result.Add(Path.GetFileNameWithoutExtension(directory));
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
            return (File.Exists(GetFileName(database)));
        }

        /// <summary>
        /// Obtains the database with the specified name.
        /// </summary>
        /// <param name="database">The name of the database.</param>
        /// <returns></returns>
        public override IDatabase GetDatabase(string database)
        {
            if (!HasDatabase(database))
            {
                throw new InvalidOperationException(string.Format("Database '{0}' does not exist!", database));
            }

            return new SQLiteDatabase(this, database);
        }

        /// <summary>
        /// Adds a new database with the specified name.
        /// </summary>
        /// <param name="database">The name of the database.</param>
        /// <returns></returns>
        public override IDatabase CreateDatabase(string database)
        {
            string file = GetFileName(database);
            if (File.Exists(file))
            {
                throw new InvalidOperationException(string.Format("Database '{0}' already exists!", database));
            }

            Directory.CreateDirectory(Path.GetDirectoryName(file));
            File.WriteAllBytes(file, new byte[0]);
            return new SQLiteDatabase(this, database);
        }

        /// <summary>
        /// Removes the specified database.
        /// </summary>
        /// <param name="database">The name of the database.</param>
        public override void DeleteDatabase(string database)
        {
            if (!HasDatabase(database))
            {
                throw new InvalidOperationException(string.Format("Database '{0}' does not exist!", database));
            }

            File.Delete(GetFileName(database));
        }

        /// <summary>
        /// Obtains whether the connection supports named parameters or not.
        /// </summary>
        public override bool SupportsNamedParameters => true;

        /// <summary>
        /// Obtains wether the connection supports select * groupby.
        /// </summary>
        public override bool SupportsAllFieldsGroupBy => true;

        /// <summary>
        /// Obtains the parameter prefix char (@).
        /// </summary>
        public override string ParameterPrefix => "@";
        #endregion

        #region precision members
        /// <summary>
        /// Obtains the maximum <see cref="TimeSpan"/> value precision (absolute) of this storage engine.
        /// </summary>
        public override TimeSpan TimeSpanPrecision => TimeSpan.FromMilliseconds(1);

        /// <summary>
        /// Obtains the maximum <see cref="decimal"/> value precision of this storage engine.
        /// </summary>
        public override decimal GetDecimalPrecision(float count)
        {
            return 0.000000000000001m;
        }
        #endregion
    }
}
