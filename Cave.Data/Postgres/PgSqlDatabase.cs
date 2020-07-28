using System;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Cave.Data.Sql;

namespace Cave.Data.Postgres
{
    /// <summary>Provides a postgre sql database implementation.</summary>
    public sealed class PgSqlDatabase : SqlDatabase
    {
        /// <summary>Initializes a new instance of the <see cref="PgSqlDatabase" /> class.</summary>
        /// <param name="storage">the postgre sql storage engine.</param>
        /// <param name="name">the name of the database.</param>
        public PgSqlDatabase(PgSqlStorage storage, string name)
            : base(storage, PgSqlStorage.GetObjectName(name))
        {
        }

        /// <inheritdoc />
        public override bool IsSecure
        {
            get
            {
                var error = false;
                var connection = SqlStorage.GetConnection(Name);
                try
                {
                    return connection.ConnectionString.ToUpperInvariant().Contains("SSLMODE=REQUIRE");
                }
                catch
                {
                    error = true;
                    throw;
                }
                finally
                {
                    SqlStorage.ReturnConnection(ref connection, error);
                }
            }
        }

        /// <inheritdoc />
        public override ITable GetTable(string table, TableFlags flags = default) => PgSqlTable.Connect(this, flags, table);

        /// <summary>Adds a new table with the specified name.</summary>
        /// <param name="layout">Layout of the table.</param>
        /// <param name="flags">The flags for table creation.</param>
        /// <returns>Returns an <see cref="ITable" /> instance for the specified table.</returns>
        public override ITable CreateTable(RowLayout layout, TableFlags flags = default)
        {
            if (layout == null)
            {
                throw new ArgumentNullException(nameof(layout));
            }

            Trace.TraceInformation($"Creating table {layout}");
            if (layout.Name.HasInvalidChars(ASCII.Strings.SafeName))
            {
                throw new ArgumentException($"Table name {layout.Name} contains invalid chars!");
            }

            var queryText = new StringBuilder();
            queryText.Append("CREATE ");
            if ((flags & TableFlags.InMemory) != 0)
            {
                queryText.Append("UNLOGGED ");
            }

            queryText.Append($"TABLE {SqlStorage.FQTN(Name, layout.Name)} (");
            for (var i = 0; i < layout.FieldCount; i++)
            {
                var fieldProperties = layout[i];
                if (i > 0)
                {
                    queryText.Append(",");
                }

                var fieldName = SqlStorage.EscapeFieldName(fieldProperties);
                queryText.Append(fieldName);
                queryText.Append(" ");
                switch (fieldProperties.TypeAtDatabase)
                {
                    case DataType.Binary:
                        if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                        {
                            throw new NotSupportedException($"AutoIncrement is not supported on data type {fieldProperties.TypeAtDatabase}");
                        }

                        queryText.Append("BYTEA");
                        break;
                    case DataType.Bool:
                        if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                        {
                            throw new NotSupportedException($"AutoIncrement is not supported on data type {fieldProperties.TypeAtDatabase}");
                        }

                        queryText.Append("BOOL");
                        break;
                    case DataType.DateTime:
                        if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                        {
                            throw new NotSupportedException($"AutoIncrement is not supported on data type {fieldProperties.TypeAtDatabase}");
                        }

                        queryText.Append("TIMESTAMP WITH TIME ZONE");
                        break;
                    case DataType.TimeSpan:
                        if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                        {
                            throw new NotSupportedException($"AutoIncrement is not supported on data type {fieldProperties.TypeAtDatabase}");
                        }

                        queryText.Append("FLOAT8");
                        break;
                    case DataType.Int8:
                        if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                        {
                            throw new NotSupportedException($"AutoIncrement is not supported on data type {fieldProperties.TypeAtDatabase}");
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
                            throw new NotSupportedException($"AutoIncrement is not supported on data type {fieldProperties.TypeAtDatabase}");
                        }

                        queryText.Append("FLOAT4");
                        break;
                    case DataType.Double:
                        if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                        {
                            throw new NotSupportedException($"AutoIncrement is not supported on data type {fieldProperties.TypeAtDatabase}");
                        }

                        queryText.Append("FLOAT8");
                        break;
                    case DataType.Enum:
                        if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                        {
                            throw new NotSupportedException($"AutoIncrement is not supported on data type {fieldProperties.TypeAtDatabase}");
                        }

                        queryText.Append("BIGINT");
                        break;
                    case DataType.UInt8:
                        if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                        {
                            throw new NotSupportedException($"AutoIncrement is not supported on data type {fieldProperties.TypeAtDatabase}");
                        }

                        queryText.AppendFormat("SMALLINT CHECK ({0} >= 0 AND {0} <= {1})", fieldName, byte.MaxValue);
                        break;
                    case DataType.UInt16:
                        if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                        {
                            throw new NotSupportedException($"AutoIncrement is not supported on data type {fieldProperties.TypeAtDatabase}");
                        }

                        queryText.AppendFormat("INT CHECK ({0} >= 0 AND {0} <= {1})", fieldName, ushort.MaxValue);
                        break;
                    case DataType.UInt32:
                        if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                        {
                            throw new NotSupportedException($"AutoIncrement is not supported on data type {fieldProperties.TypeAtDatabase}");
                        }

                        queryText.AppendFormat("BIGINT CHECK ({0} >= 0 AND {0} <= {1})", fieldName, uint.MaxValue);
                        break;
                    case DataType.UInt64:
                        if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                        {
                            throw new NotSupportedException($"AutoIncrement is not supported on data type {fieldProperties.TypeAtDatabase}");
                        }

                        queryText.AppendFormat("NUMERIC(20,0) CHECK ({0} >= 0 AND {0} <= {1})", fieldName, ulong.MaxValue);
                        break;
                    case DataType.User:
                    case DataType.String:
                        if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                        {
                            throw new NotSupportedException($"AutoIncrement is not supported on data type {fieldProperties.TypeAtDatabase}");
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
                            throw new NotSupportedException($"AutoIncrement is not supported on data type {fieldProperties.TypeAtDatabase}");
                        }

                        if (fieldProperties.MaximumLength > 0)
                        {
                            var prec = (int) fieldProperties.MaximumLength;
                            var temp = (fieldProperties.MaximumLength - prec) * 100;
                            var scale = (int) temp;
                            if ((scale >= prec) || (scale != temp))
                            {
                                throw new ArgumentOutOfRangeException(
                                    $"Field {fieldProperties.Name} has an invalid MaximumLength of {prec},{scale}. Correct values range from s,p = 1,0 to 65,30(default value) with 0 < s < p!");
                            }

                            queryText.AppendFormat("DECIMAL({0},{1})", prec, scale);
                        }
                        else
                        {
                            queryText.Append("DECIMAL(65,30)");
                        }

                        break;
                    default: throw new NotImplementedException($"Unknown DataType {fieldProperties.DataType}!");
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
                                throw new NotSupportedException(
                                    $"Unique string fields without length are not supported! Please define Field.MaxLength at table {layout.Name} field {fieldProperties.Name}");
                            }

                            break;
                        default: throw new NotSupportedException($"Uniqueness for table {layout.Name} field {fieldProperties.Name} is not supported!");
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
            SqlStorage.Execute(database: Name, table: layout.Name, cmd: queryText.ToString());
            for (var i = 0; i < layout.FieldCount; i++)
            {
                var fieldProperties = layout[i];
                if ((fieldProperties.Flags & FieldFlags.ID) != 0)
                {
                    continue;
                }

                if ((fieldProperties.Flags & FieldFlags.Index) != 0)
                {
                    var name = PgSqlStorage.GetObjectName($"idx_{layout.Name}_{fieldProperties.Name}");
                    var cmd = $"CREATE INDEX {name} ON {SqlStorage.FQTN(Name, layout.Name)} ({SqlStorage.EscapeFieldName(fieldProperties)})";
                    SqlStorage.Execute(database: Name, table: layout.Name, cmd: cmd);
                }
            }

            return GetTable(layout);
        }

        /// <inheritdoc />
        protected override string[] GetTableNames() =>
            SqlStorage.Query(database: Name, table: "pg_tables", cmd: "SELECT tablename FROM pg_tables").Select(r => r[0].ToString()).ToArray();
    }
}
