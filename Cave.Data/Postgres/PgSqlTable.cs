using System.Linq;
using Cave.Data.Sql;

namespace Cave.Data.Postgres
{
    /// <summary>
    /// Provides a postgre sql table implementation.
    /// </summary>
    public class PgSqlTable : SqlTable
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="PgSqlTable"/> class.
        /// </summary>
        protected PgSqlTable()
        {
        }

        /// <summary>
        /// Connects to the specified database and tablename.
        /// </summary>
        /// <param name="database">Database to connect to.</param>
        /// <param name="flags">Flags used to connect to the table.</param>
        /// <param name="tableName">The table to connect to.</param>
        /// <returns>Returns a new <see cref="PgSqlTable"/> instance.</returns>
        public static PgSqlTable Connect(PgSqlDatabase database, TableFlags flags, string tableName)
        {
            var table = new PgSqlTable();
            table.Initialize(database, flags, tableName);
            return table;
        }

        /// <summary>
        /// Connects to the specified database and tablename.
        /// </summary>
        /// <param name="database">Database to connect to.</param>
        /// <param name="flags">Flags used to connect to the table.</param>
        /// <param name="layout">The table layout.</param>
        /// <returns>Returns a new <see cref="PgSqlTable"/> instance.</returns>
        public static PgSqlTable Connect(PgSqlDatabase database, TableFlags flags, RowLayout layout)
        {
            var table = new PgSqlTable();
            table.Connect((IDatabase)database, flags, layout);
            return table;
        }

        /// <inheritdoc/>
        public override Row GetRowAt(int index) => QueryRow("SELECT * FROM " + FQTN + " LIMIT " + index + ",1");

        /// <inheritdoc/>
        protected override void CreateLastInsertedRowCommand(SqlCommandBuilder commandBuilder, Row row)
        {
            var idField = Layout.Identifier.Single();
            commandBuilder.AppendLine($"SELECT * FROM {FQTN} WHERE {Storage.EscapeFieldName(idField)} = LASTVAL();");
        }
    }
}
