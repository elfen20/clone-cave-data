using System;
using System.Collections.Generic;
using System.Text;
using Cave.Data.Sql;
using Cave.IO;

namespace Cave.Data.Microsoft
{
    /// <summary>
    /// Provides a MsSql database implementation.
    /// </summary>
    public sealed class MsSqlDatabase : SqlDatabase
    {
        /// <summary>Returns true assuming that no one else accesses the system memory.</summary>
        /// <value><c>true</c>.</value>
        public override bool IsSecure
        {
            get
            {
                bool l_Error = false;
                SqlConnection connection = SqlStorage.GetConnection(Name);
                try { return MsSqlStorage.RequireSSL && connection.ConnectionString.Contains("Encrypt=true"); }
                catch { l_Error = true; throw; }
                finally { SqlStorage.ReturnConnection(ref connection, l_Error); }
            }
        }

        /// <summary>
        /// Creates a new MsSql database instance.
        /// </summary>
        /// <param name="storage">the MsSql storage engine.</param>
        /// <param name="name">the name of the database.</param>
        public MsSqlDatabase(MsSqlStorage storage, string name)
            : base(storage, name)
        {
        }

        /// <summary>
        /// Obtains the available table names.
        /// </summary>
        public override string[] TableNames
        {
            get
            {
                List<string> result = new List<string>();
                List<Row> rows = SqlStorage.Query(null, Name, "stables", "EXEC stables @table_owner='dbo',@table_qualifier='" + Name + "';");
                foreach (Row row in rows)
                {
                    string tableName = (string)row.GetValue(2);
                    result.Add(tableName);
                }
                return result.ToArray();
            }
        }

        /// <summary>
        /// Obtains whether the specified table exists or not.
        /// </summary>
        /// <param name="table">The name of the table.</param>
        /// <returns></returns>
        public override bool HasTable(string table)
        {
            if (table.HasInvalidChars(ASCII.Strings.SafeName))
            {
                throw new ArgumentException("Table name contains invalid chars!");
            }
            foreach (string name in TableNames)
            {
                if (string.Equals(table, name, StringComparison.CurrentCultureIgnoreCase))
                {
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// Opens and retrieves the existing table with the given layout.
        /// </summary>
        /// <typeparam name="T">Row structure type.</typeparam>
        /// <param name="layout">Layout and name of the table.</param>
        /// <returns>Returns a table instance.</returns>
        protected override ITable<T> OpenTable<T>(RowLayout layout)
        {
            return new MsSqlTable<T>(this, layout);
        }

        /// <summary>
        /// Opens the table with the specified name.
        /// </summary>
        /// <param name="table">TableName of the table.</param>
        /// <returns>Returns an <see cref="ITable"/> instance for the specified table.</returns>
        public override ITable GetTable(string table)
        {
            if (!HasTable(table))
            {
                throw new InvalidOperationException(string.Format("Table '{0}' does not exist!", table));
            }

            return new MsSqlTable(this, table);
        }

        /// <summary>
        /// Adds a new table with the specified type.
        /// </summary>
        /// <typeparam name="T">The row struct to use for the table.</typeparam>
        /// <param name="flags">The table creation flags.</param>
        /// <param name="table">Name of the table to create (optional, use this to overwrite the default table name).</param>
        /// <returns></returns>
        public override ITable<T> CreateTable<T>(TableFlags flags, string table)
        {
            RowLayout layout = RowLayout.CreateTyped(typeof(T), table, Storage);
            CreateTable(layout, flags);
            return OpenTable<T>(layout);
        }

        /// <summary>
        /// Adds a new table with the specified name.
        /// </summary>
        /// <param name="layout">Layout of the table.</param>
        /// <param name="flags">The table creation flags.</param>
        /// <returns>Returns an <see cref="ITable"/> instance for the specified table.</returns>
        public override ITable CreateTable(RowLayout layout, TableFlags flags)
        {
            if (layout == null)
            {
                throw new ArgumentNullException("Layout");
            }

            LogCreateTable(layout);
            if (0 != (flags & TableFlags.InMemory))
            {
                throw new NotSupportedException(string.Format("Table '{0}' does not support TableFlags.{1}", layout.Name, TableFlags.InMemory));
            }
            if (layout.Name.HasInvalidChars(ASCII.Strings.SafeName))
            {
                throw new ArgumentException("Table name contains invalid chars!");
            }
            StringBuilder queryText = new StringBuilder();
            queryText.AppendFormat("CREATE TABLE {0} (", SqlStorage.FQTN(Name, layout.Name));
            for (int i = 0; i < layout.FieldCount; i++)
            {
                FieldProperties fieldProperties = layout.GetProperties(i);
                if (i > 0)
                {
                    queryText.Append(",");
                }

                queryText.Append(fieldProperties.NameAtDatabase + " ");
                switch (fieldProperties.DataType)
                {
                    case DataType.Binary: queryText.Append("VARBINARY(MAX)"); break;

                    case DataType.Bool: queryText.Append("BIT"); break;

                    case DataType.DateTime:
                        switch (fieldProperties.DateTimeType)
                        {
                            case DateTimeType.Undefined:
                            case DateTimeType.Native:
                                queryText.Append("DATETIME");
                                break;
                            case DateTimeType.DoubleSeconds:
                                queryText.Append("FLOAT(53)");
                                break;
                            case DateTimeType.DecimalSeconds:
                                queryText.Append("NUMERIC(28,8)");
                                break;
                            case DateTimeType.BigIntHumanReadable:
                            case DateTimeType.BigIntTicks:
                                queryText.Append("BIGINT");
                                break;
                            default: throw new NotImplementedException();
                        }
                        break;

                    case DataType.TimeSpan:
                        switch (fieldProperties.DateTimeType)
                        {
                            case DateTimeType.Undefined:
                            case DateTimeType.Native:
                                queryText.Append("TIMESPAN");
                                break;
                            case DateTimeType.DoubleSeconds:
                                queryText.Append("FLOAT(53)");
                                break;
                            case DateTimeType.DecimalSeconds:
                                queryText.Append("NUMERIC(28,8)");
                                break;
                            case DateTimeType.BigIntHumanReadable:
                            case DateTimeType.BigIntTicks:
                                queryText.Append("BIGINT");
                                break;
                            default: throw new NotImplementedException();
                        }
                        break;

                    case DataType.Int8:
                        queryText.Append("SMALLINT");
                        break;

                    case DataType.Int16:
                        queryText.Append("SMALLINT");
                        break;

                    case DataType.Int32:
                        queryText.Append("INTEGER");
                        break;

                    case DataType.Int64:
                        queryText.Append("BIGINT");
                        break;

                    case DataType.Single:
                        queryText.Append("REAL");
                        break;

                    case DataType.Double:
                        queryText.Append("FLOAT(53)");
                        break;

                    case DataType.Enum:
                        queryText.Append("BIGINT");
                        break;

                    case DataType.User:
                    case DataType.String:
                        switch (fieldProperties.StringEncoding)
                        {
                            case StringEncoding.ASCII:
                                if ((fieldProperties.MaximumLength > 0) && (fieldProperties.MaximumLength <= 255))
                                {
                                    queryText.AppendFormat("VARCHAR({0})", fieldProperties.MaximumLength);
                                }
                                else
                                {
                                    queryText.Append("VARCHAR(MAX)");
                                }
                                break;
                            case StringEncoding.UTF16:
                            case StringEncoding.UTF8:
                                if ((fieldProperties.MaximumLength > 0) && (fieldProperties.MaximumLength <= 255))
                                {
                                    queryText.AppendFormat("NVARCHAR({0})", fieldProperties.MaximumLength);
                                }
                                else
                                {
                                    queryText.Append("NVARCHAR(MAX)");
                                }
                                break;
                            default: throw new NotSupportedException(string.Format("MSSQL Server does not support {0}!", fieldProperties.StringEncoding));
                        }
                        break;

                    case DataType.Decimal:
                        if (fieldProperties.MaximumLength > 0)
                        {
                            int l_PreDecimal = (int)fieldProperties.MaximumLength;
                            float l_Temp = (fieldProperties.MaximumLength - l_PreDecimal) * 100;
                            int l_Decimal = (int)l_Temp;
                            if ((l_Decimal >= l_PreDecimal) || (l_Decimal != l_Temp))
                            {
                                throw new ArgumentOutOfRangeException(string.Format("Field {0} has an invalid MaximumLength of {1},{2}. Correct values range from s,p = 1,0 to 28,27 with 0 < s < p!", fieldProperties.Name, l_PreDecimal, l_Decimal));
                            }
                            queryText.AppendFormat("NUMERIC({0},{1})", l_PreDecimal, l_Decimal);
                        }
                        else
                        {
                            queryText.Append("NUMERIC(28,8)");
                        }
                        break;

                    default: throw new NotImplementedException(string.Format("Unknown DataType {0}!", fieldProperties.DataType));
                }
                if ((fieldProperties.Flags & FieldFlags.ID) != 0)
                {
                    queryText.Append(" PRIMARY KEY");
                }
                if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                {
                    queryText.Append(" IDENTITY");
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
                    string command = string.Format("CREATE INDEX {0} ON {1} ({2})", SqlStorage.EscapeString("idx_" + layout.Name + "_" + fieldProperties.Name), SqlStorage.FQTN(Name, layout.Name), SqlStorage.EscapeFieldName(fieldProperties));
                    SqlStorage.Execute(Name, layout.Name, command);
                }
            }
            return GetTable(layout, TableFlags.None);
        }
    }
}
