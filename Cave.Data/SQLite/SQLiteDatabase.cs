using System;
using System.Collections.Generic;
using System.Text;
using Cave.Data.Sql;

namespace Cave.Data.SQLite
{
    /// <summary>
    /// Provides a sqlite database implementation.
    /// </summary>
    public sealed class SQLiteDatabase : SqlDatabase
    {
        /// <summary>Returns true assuming that no one else accesses the database file.</summary>
        /// <value><c>true</c>.</value>
        public override bool IsSecure => true;

        /// <summary>
        /// Creates a new sqlite database instance.
        /// </summary>
        /// <param name="storage">The storage engine.</param>
        /// <param name="name">The name of the database.</param>
        public SQLiteDatabase(SQLiteStorage storage, string name)
            : base(storage, name)
        {
            List<FieldProperties> fields = new List<FieldProperties>
            {
                new FieldProperties(name, FieldFlags.None, DataType.String, "type"),
                new FieldProperties(name, FieldFlags.None, DataType.String, "name"),
                new FieldProperties(name, FieldFlags.None, DataType.String, "tbname"),
                new FieldProperties(name, FieldFlags.None, DataType.Int64, "rootpage"),
                new FieldProperties(name, FieldFlags.None, DataType.String, "sql")
            };
            RowLayout l_Expected = RowLayout.CreateUntyped(name, fields.ToArray());
            RowLayout schema = SqlStorage.QuerySchema(Name, "sqlite_master");
            SqlStorage.CheckLayout(name, schema, l_Expected);
        }

        /// <summary>
        /// Obtains the available table names.
        /// </summary>
        public override string[] TableNames
        {
            get
            {
                List<string> result = new List<string>();
                List<Row> rows = SqlStorage.Query(null, Name, "sqlite_master", "SELECT name, type FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'");
                foreach (Row row in rows)
                {
                    result.Add((string)row.GetValue(0));
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
            object value = SqlStorage.QueryValue(Name, "sqlite_master", "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=" + SqlStorage.EscapeString(table));
            return Convert.ToInt32(value) > 0;
        }

        /// <summary>
        /// Opens and retrieves the existing table with the given layout.
        /// </summary>
        /// <typeparam name="T">Row structure type.</typeparam>
        /// <param name="layout">Layout and name of the table.</param>
        /// <returns>Returns a table instance.</returns>
        protected override ITable<T> OpenTable<T>(RowLayout layout)
        {
            return new SQLiteTable<T>(this, layout);
        }

        /// <summary>
        /// Opens the table with the specified name.
        /// </summary>
        /// <param name="table">Name of the table.</param>
        /// <returns>Returns an <see cref="ITable"/> instance for the specified table.</returns>
        public override ITable GetTable(string table)
        {
            if (!HasTable(table))
            {
                throw new InvalidOperationException(string.Format("Table '{0}' does not exist!", table));
            }

            return new SQLiteTable(this, table);
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
            RowLayout layout = RowLayout.CreateTyped(typeof(T), null, Storage);
            if (0 != (flags & TableFlags.InMemory))
            {
                throw new NotSupportedException(string.Format("Table '{0}' does not support TableFlags.{1}", layout.Name, flags));
            }
            LogCreateTable(layout);
            StringBuilder queryText = new StringBuilder();
            queryText.AppendFormat("CREATE TABLE {0} (", SqlStorage.FQTN(Name, layout.Name));
            for (int i = 0; i < layout.FieldCount; i++)
            {
                FieldProperties fieldProperties = layout.GetProperties(i);
                if (i > 0)
                {
                    queryText.Append(",");
                }

                queryText.Append(fieldProperties.NameAtDatabase);
                queryText.Append(" ");
                SQLiteValueType valueType = SQLiteStorage.GetValueType(fieldProperties.DataType);
                switch (valueType)
                {
                    case SQLiteValueType.BLOB:
                        queryText.Append("BLOB");
                        break;

                    case SQLiteValueType.INTEGER:
                        queryText.Append("INTEGER");
                        break;

                    case SQLiteValueType.REAL:
                        queryText.Append("REAL");
                        break;

                    case SQLiteValueType.TEXT:
                        queryText.Append("TEXT");
                        break;

                    default: throw new NotImplementedException(string.Format("Unknown ValueType {0}!", valueType));
                }
                if ((fieldProperties.Flags & FieldFlags.ID) != 0)
                {
                    queryText.Append(" PRIMARY KEY");
                }
                if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                {
                    queryText.Append(" AUTOINCREMENT");
                }
                if ((fieldProperties.Flags & FieldFlags.Unique) != 0)
                {
                    queryText.Append(" UNIQUE");
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
                    string command = string.Format("CREATE INDEX {0} ON {1} ({2})", "idx_" + layout.Name + "_" + fieldProperties.Name, layout.Name, fieldProperties.Name);
                    SqlStorage.Execute(Name, layout.Name, command);
                }
            }
            return GetTable<T>();
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

            if (0 != (flags & TableFlags.InMemory))
            {
                throw new NotSupportedException(string.Format("Table '{0}' does not support TableFlags.{1}", layout.Name, flags));
            }
            if (layout.Name.HasInvalidChars(ASCII.Strings.SafeName))
            {
                throw new ArgumentException("Table name contains invalid chars!");
            }
            StringBuilder l_QueryText = new StringBuilder();
            l_QueryText.AppendFormat("CREATE TABLE {0} (", SqlStorage.FQTN(Name, layout.Name));
            for (int i = 0; i < layout.FieldCount; i++)
            {
                FieldProperties fieldProperties = layout.GetProperties(i);
                if (i > 0)
                {
                    l_QueryText.Append(",");
                }

                l_QueryText.Append(fieldProperties.NameAtDatabase);
                l_QueryText.Append(" ");
                SQLiteValueType valueType = SQLiteStorage.GetValueType(fieldProperties.DataType);
                switch (valueType)
                {
                    case SQLiteValueType.BLOB:
                        l_QueryText.Append("BLOB");
                        break;

                    case SQLiteValueType.INTEGER:
                        l_QueryText.Append("INTEGER");
                        break;

                    case SQLiteValueType.REAL:
                        l_QueryText.Append("REAL");
                        break;

                    case SQLiteValueType.TEXT:
                        l_QueryText.Append("TEXT");
                        break;

                    default: throw new NotImplementedException(string.Format("Unknown ValueType {0}!", valueType));
                }
                if ((fieldProperties.Flags & FieldFlags.ID) != 0)
                {
                    l_QueryText.Append(" PRIMARY KEY");
                }
                if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                {
                    l_QueryText.Append(" AUTOINCREMENT");
                }
                if ((fieldProperties.Flags & FieldFlags.Unique) != 0)
                {
                    l_QueryText.Append(" UNIQUE");
                }
            }
            l_QueryText.Append(")");
            SqlStorage.Execute(Name, layout.Name, l_QueryText.ToString());
            for (int i = 0; i < layout.FieldCount; i++)
            {
                FieldProperties fieldProperties = layout.GetProperties(i);
                if ((fieldProperties.Flags & FieldFlags.ID) != 0)
                {
                    continue;
                }

                if ((fieldProperties.Flags & FieldFlags.Index) != 0)
                {
                    string command = string.Format("CREATE INDEX {0} ON {1} ({2})", "idx_" + layout.Name + "_" + fieldProperties.Name, layout.Name, fieldProperties.Name);
                    SqlStorage.Execute(Name, layout.Name, command);
                }
            }
            return GetTable(layout, TableFlags.None);
        }
    }
}
