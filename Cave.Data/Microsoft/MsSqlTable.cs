using System.Linq;
using Cave.Data.Sql;

namespace Cave.Data.Microsoft
{
    /// <summary>Provides a MsSql table implementation.</summary>
    public class MsSqlTable : SqlTable
    {
        /// <summary>Initializes a new instance of the <see cref="MsSqlTable" /> class.</summary>
        protected MsSqlTable() { }

        /// <summary>Connects to the specified database and tablename.</summary>
        /// <param name="database">Database to connect to.</param>
        /// <param name="flags">Flags used to connect to the table.</param>
        /// <param name="tableName">The table to connect to.</param>
        /// <returns>Returns a new <see cref="MsSqlTable" /> instance.</returns>
        public static MsSqlTable Connect(MsSqlDatabase database, TableFlags flags, string tableName)
        {
            var table = new MsSqlTable();
            table.Initialize(database, flags, tableName);
            return table;
        }

        /// <inheritdoc />
        public override Row GetRowAt(int index)
        {
            var cmd = $"WITH TempOrderedData AS (SELECT *, ROW_NUMBER() AS 'RowNumber' FROM {FQTN}) SELECT * FROM TempOrderedData WHERE RowNumber={index}";
            return QueryRow(cmd);
        }

        /// <inheritdoc />
        protected override void CreateLastInsertedRowCommand(SqlCommandBuilder commandBuilder, Row row)
        {
            var idField = Layout.Identifier.Single();
            commandBuilder.AppendLine($"SELECT * FROM {FQTN} WHERE {Storage.EscapeFieldName(idField)} = (SELECT SCOPE_IDENTITY() AS [SCOPE_IDENTITY]);");
        }
    }
}
