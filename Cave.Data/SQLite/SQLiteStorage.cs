using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using Cave.Data.Sql;

namespace Cave.Data.SQLite
{
    /// <summary>
    /// Provides a sqlite storage implementation.
    /// </summary>
    public sealed class SQLiteStorage : SqlStorage
    {
        const string StaticConnectionString = "Data Source={0}";

        /// <summary>
        /// Initializes a new instance of the <see cref="SQLiteStorage"/> class.
        /// </summary>
        /// <param name="connectionString">the connection details.</param>
        /// <param name="flags">The connection flags.</param>
        public SQLiteStorage(ConnectionString connectionString, ConnectionFlags flags = default)
            : base(connectionString, flags)
        {
        }

        /// <inheritdoc/>
        public override string[] DatabaseNames
        {
            get
            {
                var result = new List<string>();
                foreach (var directory in Directory.GetFiles(ConnectionString.Location, "*.db"))
                {
                    result.Add(Path.GetFileNameWithoutExtension(directory));
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
        public override TimeSpan TimeSpanPrecision => TimeSpan.FromMilliseconds(1);

        /// <inheritdoc/>
        protected internal override bool DBConnectionCanChangeDataBase => false;

        /// <summary>
        /// Gets the database type for the specified field type.
        /// Sqlite does not implement all different sql92 types directly instead they are reusing only 4 different types.
        /// So we have to check only the sqlite value types and convert to the dotnet type.
        /// </summary>
        /// <param name="dataType">Local DataType.</param>
        /// <returns>The database data type to use.</returns>
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
        /// Gets the sqlite value type of the specified datatype.
        /// </summary>
        /// <param name="dataType">Data type.</param>
        /// <returns>The sqlite value type.</returns>
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

        #region IStorage functions

        /// <inheritdoc/>
        public override IFieldProperties GetDatabaseFieldProperties(IFieldProperties field)
        {
            if (field == null)
            {
                throw new ArgumentNullException("LocalField");
            }

            DataType typeAtDatabase = GetDatabaseDataType(field.DataType);
            if (field.TypeAtDatabase != typeAtDatabase)
            {
                var result = field.Clone();
                result.TypeAtDatabase = typeAtDatabase;
                return result;
            }
            return field;
        }

        /// <inheritdoc/>
        public override string EscapeFieldName(IFieldProperties field)
        {
            if (field == null)
            {
                throw new ArgumentNullException("FieldName");
            }

            return "[" + field.NameAtDatabase + "]";
        }

        /// <inheritdoc/>
        public override string FQTN(string database, string table)
        {
            return table;
        }

        /// <inheritdoc/>
        public override bool HasDatabase(string database) => File.Exists(GetFileName(database));

        /// <inheritdoc/>
        public override IDatabase GetDatabase(string database)
        {
            if (!HasDatabase(database))
            {
                throw new InvalidOperationException(string.Format("Database '{0}' does not exist!", database));
            }

            return new SQLiteDatabase(this, database);
        }

        /// <inheritdoc/>
        public override IDatabase CreateDatabase(string database)
        {
            var file = GetFileName(database);
            if (File.Exists(file))
            {
                throw new InvalidOperationException(string.Format("Database '{0}' already exists!", database));
            }

            Directory.CreateDirectory(Path.GetDirectoryName(file));
            File.WriteAllBytes(file, new byte[0]);
            return new SQLiteDatabase(this, database);
        }

        /// <inheritdoc/>
        public override void DeleteDatabase(string database)
        {
            if (!HasDatabase(database))
            {
                throw new InvalidOperationException(string.Format("Database '{0}' does not exist!", database));
            }

            File.Delete(GetFileName(database));
        }

        #endregion

        /// <inheritdoc/>
        public override decimal GetDecimalPrecision(float count)
        {
            return 0.000000000000001m;
        }

        /// <inheritdoc/>
        protected override IDbConnection GetDbConnectionType()
        {
            var type =
                Type.GetType("System.Data.SQLite.SQLiteConnection, System.Data.SQLite", false) ??
                Type.GetType("Mono.Data.SQLite.SQLiteConnection, Mono.Data.SQLite", false) ??
                throw new TypeLoadException("Could neither load System.Data.SQLite.SQLiteConnection nor Mono.Data.SQLite.SQLiteConnection!");
            return (IDbConnection)Activator.CreateInstance(type);
        }

        /// <inheritdoc/>
        protected override string GetConnectionString(string database)
        {
            if (string.IsNullOrEmpty(database))
            {
                throw new ArgumentNullException("Database");
            }

            var path = GetFileName(database);
            return string.Format(StaticConnectionString, path);
        }

        /// <summary>
        /// Gets the fileName for the specified database name.
        /// </summary>
        /// <param name="database">Name of the database (file).</param>
        /// <returns>Fullpath to the database file.</returns>
        string GetFileName(string database)
        {
            return Path.GetFullPath(Path.Combine(ConnectionString.Location, database + ".db"));
        }
    }
}
