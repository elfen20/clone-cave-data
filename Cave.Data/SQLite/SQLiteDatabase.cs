using System;
using System.Collections.Generic;
using System.Text;
using Cave.Data.Sql;

namespace Cave.Data.SQLite
{
    /// <summary>Provides a sqlite database implementation.</summary>
    public sealed class SQLiteDatabase : SqlDatabase
    {
        /// <summary>Initializes a new instance of the <see cref="SQLiteDatabase" /> class.</summary>
        /// <param name="storage">The storage engine.</param>
        /// <param name="name">The name of the database.</param>
        public SQLiteDatabase(SQLiteStorage storage, string name)
            : base(storage, name)
        {
            var fields = new List<FieldProperties>
            {
                new FieldProperties { Index = 0, DataType = DataType.String, Name = "type" },
                new FieldProperties { Index = 1, DataType = DataType.String, Name = "name" },
                new FieldProperties { Index = 2, DataType = DataType.String, Name = "tbname" },
                new FieldProperties { Index = 3, DataType = DataType.Int64, Name = "rootpage" },
                new FieldProperties { Index = 4, DataType = DataType.String, Name = "sql" }
            };
            foreach (var field in fields)
            {
                field.NameAtDatabase = field.Name;
                field.TypeAtDatabase = field.DataType;
                field.Validate();
            }

            var expected = RowLayout.CreateUntyped(name, fields.ToArray());
            var schema = SqlStorage.QuerySchema(Name, "sqlite_master");
            SqlStorage.CheckLayout(expected, schema);
        }

        /// <inheritdoc />
        public override bool IsSecure => true;

        /// <inheritdoc />
        public override ITable GetTable(string table, TableFlags flags) => SQLiteTable.Connect(this, flags, table);

        /// <inheritdoc />
        public override ITable CreateTable(RowLayout layout, TableFlags flags)
        {
            if (layout == null)
            {
                throw new ArgumentNullException(nameof(layout));
            }

            if ((flags & TableFlags.InMemory) != 0)
            {
                throw new NotSupportedException($"Table '{layout.Name}' does not support TableFlags.{flags}");
            }

            if (layout.Name.HasInvalidChars(ASCII.Strings.SafeName))
            {
                throw new ArgumentException($"Table name {layout.Name} contains invalid chars!");
            }

            var queryText = new StringBuilder();
            queryText.Append($"CREATE TABLE {SqlStorage.FQTN(Name, layout.Name)} (");
            for (var i = 0; i < layout.FieldCount; i++)
            {
                var fieldProperties = layout[i];
                if (i > 0)
                {
                    queryText.Append(",");
                }

                queryText.Append(fieldProperties.NameAtDatabase);
                queryText.Append(" ");
                var valueType = SQLiteStorage.GetValueType(fieldProperties.DataType);
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
                    default: throw new NotImplementedException($"Unknown ValueType {valueType}!");
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
                    var command = $"CREATE INDEX {"idx_" + layout.Name + "_" + fieldProperties.Name} ON {layout.Name} ({fieldProperties.Name})";
                    SqlStorage.Execute(database: Name, table: layout.Name, cmd: command);
                }
            }

            return GetTable(layout);
        }

        /// <inheritdoc />
        protected override string[] GetTableNames()
        {
            var result = new List<string>();
            var rows = SqlStorage.Query(database: Name, table: "sqlite_master",
                cmd: "SELECT name, type FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'");
            foreach (var row in rows)
            {
                result.Add((string) row[0]);
            }

            return result.ToArray();
        }
    }
}
