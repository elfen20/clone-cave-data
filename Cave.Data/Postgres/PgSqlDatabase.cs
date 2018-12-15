using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using Cave.Data.Sql;

namespace Cave.Data.Postgres
{
    /// <summary>
    /// Provides a postgre sql database implementation
    /// </summary>
    public sealed class PgSqlDatabase : SqlDatabase
    {
        PgSqlStorage pgSqlStorage;

        /// <summary>Gets a value indicating whether this instance is using a secure connection to the storage.</summary>
        /// <value>
        /// <c>true</c> if this instance is using a secure connection; otherwise, <c>false</c>.
        /// </value>
        public override bool IsSecure
        {
            get
            {
                bool error = false;
                SqlConnection connection = SqlStorage.GetConnection(Name);
                try { return connection.ConnectionString.ToUpperInvariant().Contains("SSLMODE=REQUIRE"); }
                catch { error = true; throw; }
                finally { SqlStorage.ReturnConnection(ref connection, error); }
            }
        }

        /// <summary>
        /// Creates a new postgre sql database instance
        /// </summary>
        /// <param name="storage">the postgre sql storage engine</param>
        /// <param name="name">the name of the database</param>
        public PgSqlDatabase(PgSqlStorage storage, string name)
            : base(storage, storage.GetObjectName(name))
        {
            pgSqlStorage = storage;
        }

        /// <summary>
        /// Obtains the available table names
        /// </summary>
        public override string[] TableNames
        {
            get
            {
                List<string> result = new List<string>();
                List<Row> rows = SqlStorage.Query(null, Name, "pg_tables", "SELECT tablename FROM pg_tables");// where pg_tables.schemaname = " + SqlStorage.EscapeString(Name));
                foreach (Row row in rows)
                {
                    result.Add((string)row.GetValue(0));
                }
                return result.ToArray();
            }
        }

        /// <summary>
        /// Obtains whether the specified table exists or not
        /// </summary>
        /// <param name="table">The name of the table</param>
        /// <returns></returns>
        public override bool HasTable(string table)
        {
            object value = SqlStorage.QueryValue(Name, "pg_tables", "SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE " + SqlStorage.EscapeString(pgSqlStorage.GetObjectName(table)));
            return Convert.ToInt32(value) > 0;
        }

        /// <summary>
        /// Opens the table with the specified name
        /// </summary>
        /// <param name="table">Name of the table</param>
        /// <returns>Returns an <see cref="ITable"/> instance for the specified table</returns>
        public override ITable GetTable(string table)
        {
            if (!HasTable(table))
            {
                throw new InvalidOperationException(string.Format("Table '{0}' does not exist!", table));
            }

            return new PgSqlTable(this, table);
        }

        /// <summary>
        /// Opens the table with the specified name
        /// </summary>
        protected override ITable<T> OpenTable<T>(RowLayout layout)
        {
            return new PgSqlTable<T>(this, layout);
        }

        /// <summary>
        /// Adds a new table with the specified name
        /// </summary>
        /// <typeparam name="T">The row struct to use for the table</typeparam>
        /// <param name="flags">The flags for table creation</param>
        /// <param name="table">Name of the table to create (optional, use this to overwrite the default table name)</param>
        /// <returns></returns>
        public override ITable<T> CreateTable<T>(TableFlags flags, string table)
        {
            RowLayout layout = RowLayout.CreateTyped(typeof(T), table, Storage);
            CreateTable(layout, flags);
            return OpenTable<T>(layout);
        }

        /// <summary>
        /// Adds a new table with the specified name
        /// </summary>
        /// <param name="layout">Layout of the table</param>
        /// <param name="flags">The flags for table creation</param>
        /// <returns>Returns an <see cref="ITable"/> instance for the specified table</returns>
        public override ITable CreateTable(RowLayout layout, TableFlags flags)
        {
            if (layout == null)
            {
                throw new ArgumentNullException("Layout");
            }

            Trace.TraceInformation(string.Format("Creating table {0}", layout));
            if (layout.Name.HasInvalidChars(ASCII.Strings.SafeName))
            {
                throw new ArgumentException("Table name contains invalid chars!");
            }
            StringBuilder queryText = new StringBuilder();
            queryText.Append("CREATE ");
            if (0 != (flags & TableFlags.InMemory))
            {
                queryText.Append("UNLOGGED ");
            }
            queryText.AppendFormat("TABLE {0} (", SqlStorage.FQTN(Name, layout.Name));
            for (int i = 0; i < layout.FieldCount; i++)
            {
                FieldProperties fieldProperties = layout.GetProperties(i);
                if (i > 0)
                {
                    queryText.Append(",");
                }

                string fieldName = SqlStorage.EscapeFieldName(fieldProperties);
                queryText.Append(fieldName);
                queryText.Append(" ");
                switch (fieldProperties.TypeAtDatabase)
                {
                    case DataType.Binary:
                        if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                        {
                            throw new NotSupportedException(string.Format("AutoIncrement is not supported on data type {0}", fieldProperties.TypeAtDatabase));
                        }

                        queryText.Append("BYTEA");
                        break;
                    case DataType.Bool:
                        if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                        {
                            throw new NotSupportedException(string.Format("AutoIncrement is not supported on data type {0}", fieldProperties.TypeAtDatabase));
                        }

                        queryText.Append("BOOL");
                        break;
                    case DataType.DateTime:
                        if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                        {
                            throw new NotSupportedException(string.Format("AutoIncrement is not supported on data type {0}", fieldProperties.TypeAtDatabase));
                        }

                        queryText.Append("TIMESTAMP WITH TIME ZONE");
                        break;
                    case DataType.TimeSpan:
                        if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                        {
                            throw new NotSupportedException(string.Format("AutoIncrement is not supported on data type {0}", fieldProperties.TypeAtDatabase));
                        }

                        queryText.Append("FLOAT8");
                        break;
                    case DataType.Int8:
                        if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                        {
                            throw new NotSupportedException(string.Format("AutoIncrement is not supported on data type {0}", fieldProperties.TypeAtDatabase));
                        }

                        queryText.Append("SMALLINT");
                        break;
                    case DataType.Int16:
                        if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                        {
                            queryText.Append("SMALLSERIAL");
                        }
                        else
                        {
                            queryText.Append("SMALLINT");
                        }
                        break;
                    case DataType.Int32:
                        if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                        {
                            queryText.Append("SERIAL");
                        }
                        else
                        {
                            queryText.Append("INTEGER");
                        }
                        break;
                    case DataType.Int64:
                        if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                        {
                            queryText.Append("BIGSERIAL");
                        }
                        else
                        {
                            queryText.Append("BIGINT");
                        }
                        break;
                    case DataType.Single:
                        if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                        {
                            throw new NotSupportedException(string.Format("AutoIncrement is not supported on data type {0}", fieldProperties.TypeAtDatabase));
                        }

                        queryText.Append("FLOAT4");
                        break;
                    case DataType.Double:
                        if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                        {
                            throw new NotSupportedException(string.Format("AutoIncrement is not supported on data type {0}", fieldProperties.TypeAtDatabase));
                        }

                        queryText.Append("FLOAT8");
                        break;
                    case DataType.Enum:
                        if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                        {
                            throw new NotSupportedException(string.Format("AutoIncrement is not supported on data type {0}", fieldProperties.TypeAtDatabase));
                        }

                        queryText.Append("BIGINT");
                        break;
                    case DataType.UInt8:
                        if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                        {
                            throw new NotSupportedException(string.Format("AutoIncrement is not supported on data type {0}", fieldProperties.TypeAtDatabase));
                        }

                        queryText.AppendFormat("SMALLINT CHECK ({0} >= 0 AND {0} <= {1})", fieldName, byte.MaxValue);
                        break;
                    case DataType.UInt16:
                        if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                        {
                            throw new NotSupportedException(string.Format("AutoIncrement is not supported on data type {0}", fieldProperties.TypeAtDatabase));
                        }

                        queryText.AppendFormat("INT CHECK ({0} >= 0 AND {0} <= {1})", fieldName, ushort.MaxValue);
                        break;
                    case DataType.UInt32:
                        if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                        {
                            throw new NotSupportedException(string.Format("AutoIncrement is not supported on data type {0}", fieldProperties.TypeAtDatabase));
                        }

                        queryText.AppendFormat("BIGINT CHECK ({0} >= 0 AND {0} <= {1})", fieldName, uint.MaxValue);
                        break;
                    case DataType.UInt64:
                        if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                        {
                            throw new NotSupportedException(string.Format("AutoIncrement is not supported on data type {0}", fieldProperties.TypeAtDatabase));
                        }

                        queryText.AppendFormat("NUMERIC(20,0) CHECK ({0} >= 0 AND {0} <= {1})", fieldName, ulong.MaxValue);
                        break;

                    case DataType.User:
                    case DataType.String:
                        if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                        {
                            throw new NotSupportedException(string.Format("AutoIncrement is not supported on data type {0}", fieldProperties.TypeAtDatabase));
                        }

                        if (fieldProperties.MaximumLength <= 0)
                        {
                            queryText.Append("TEXT");
                        }
                        else
                        {
                            queryText.AppendFormat("VARCHAR({0})", fieldProperties.MaximumLength);
                        }
                        break;

                    case DataType.Decimal:
                        if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                        {
                            throw new NotSupportedException(string.Format("AutoIncrement is not supported on data type {0}", fieldProperties.TypeAtDatabase));
                        }

                        if (fieldProperties.MaximumLength > 0)
                        {
                            int prec = (int)fieldProperties.MaximumLength;
                            float temp = (fieldProperties.MaximumLength - prec) * 100;
                            int scale = (int)temp;
                            if ((scale >= prec) || (scale != temp))
                            {
                                throw new ArgumentOutOfRangeException(string.Format("Field {0} has an invalid MaximumLength of {1},{2}. Correct values range from s,p = 1,0 to 65,30(default value) with 0 < s < p!", fieldProperties.Name, prec, scale));
                            }
                            queryText.AppendFormat("DECIMAL({0},{1})", prec, scale);
                        }
                        else
                        {
                            queryText.Append("DECIMAL(65,30)");
                        }
                        break;

                    default: throw new NotImplementedException(string.Format("Unknown DataType {0}!", fieldProperties.DataType));
                }
                if ((fieldProperties.Flags & FieldFlags.ID) != 0)
                {
                    queryText.Append(" PRIMARY KEY");
                }
                if ((fieldProperties.Flags & FieldFlags.Unique) != 0)
                {
                    queryText.Append(" UNIQUE");
                    switch (fieldProperties.TypeAtDatabase)
                    {
                        case DataType.Bool:
                        case DataType.Char:
                        case DataType.DateTime:
                        case DataType.Decimal:
                        case DataType.Double:
                        case DataType.Enum:
                        case DataType.Int8:
                        case DataType.Int16:
                        case DataType.Int32:
                        case DataType.Int64:
                        case DataType.UInt8:
                        case DataType.UInt16:
                        case DataType.UInt32:
                        case DataType.UInt64:
                        case DataType.Single:
                        case DataType.TimeSpan:
                            break;
                        case DataType.String:
                            if (fieldProperties.MaximumLength <= 0)
                            {
                                throw new NotSupportedException(string.Format("Unique string fields without length are not supported! Please define Field.MaxLength at table {0} field {1}", layout.Name, fieldProperties.Name));
                            }
                            break;
                        default: throw new NotSupportedException(string.Format("Uniqueness for table {0} field {1} is not supported!", layout.Name, fieldProperties.Name));
                    }
                }
                if (fieldProperties.Description != null)
                {
                    if (fieldProperties.Description.HasInvalidChars(ASCII.Strings.Printable))
                    {
                        throw new ArgumentException("Description of field '{0}' contains invalid chars!", fieldProperties.Name);
                    }
                    queryText.Append(" COMMENT '" + fieldProperties.Description.Substring(0, 60) + "'");
                }
            }
            queryText.Append(")");
            SqlStorage.Execute(Name, layout.Name, queryText.ToString());
            for (int i = 0; i < layout.FieldCount; i++)
            {
                FieldProperties fieldProperties = layout.GetProperties(i);
                if ((fieldProperties.Flags & FieldFlags.ID) != 0)
                {
                    continue;
                }

                if ((fieldProperties.Flags & FieldFlags.Index) != 0)
                {
                    string command = string.Format("CREATE INDEX {0} ON {1} ({2})", pgSqlStorage.GetObjectName("idx_" + layout.Name + "_" + fieldProperties.Name),
                        SqlStorage.FQTN(Name, layout.Name), SqlStorage.EscapeFieldName(fieldProperties));
                    SqlStorage.Execute(Name, layout.Name, command);
                }
            }
            return GetTable(layout, TableFlags.None);
        }
    }
}
