using System;

namespace Cave.Data
{
    /// <summary>
    /// Connects to different database types.
    /// </summary>
    public static class Connector
    {
        /// <summary>Connects to a database storage.</summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="options">The options.</param>
        /// <returns>Returns a new storage connection.</returns>
        /// <exception cref="NotSupportedException">Unknown database provider '{connectionString.Protocol}'!.</exception>
        public static IStorage ConnectStorage(ConnectionString connectionString, ConnectionFlags options = 0)
        {
            switch (connectionString.ConnectionType)
            {
                case ConnectionType.MEMORY: return new MemoryStorage();
                case ConnectionType.MYSQL: return new Cave.Data.Mysql.MySqlStorage(connectionString, options);
                case ConnectionType.MSSQL: return new Cave.Data.Microsoft.MsSqlStorage(connectionString, options);
                case ConnectionType.SQLITE: return new Cave.Data.SQLite.SQLiteStorage(connectionString, options);
                case ConnectionType.PGSQL: return new Cave.Data.Postgres.PgSqlStorage(connectionString, options);
                default: throw new NotSupportedException($"Unknown database provider '{connectionString.Protocol}'!");
            }
        }

        /// <summary>Connects to a database using the specified <see cref="ConnectionString" />.</summary>
        /// <param name="connection">The ConnectionString.</param>
        /// <param name="options">The database connection options.</param>
        /// <returns>Returns a new database connection.</returns>
        /// <exception cref="ArgumentException">Missing database name at connection string!.</exception>
        public static IDatabase ConnectDatabase(ConnectionString connection, ConnectionFlags options = ConnectionFlags.None)
        {
            IStorage storage = ConnectStorage(connection, options);
            if (connection.Location == null)
            {
                throw new ArgumentOutOfRangeException(nameof(connection), "Database name not specified!");
            }

            var parts = connection.Location.Split(new char[] { '/' }, StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length < 1)
            {
                throw new ArgumentException("Missing database name at connection string!");
            }

            return storage.GetDatabase(parts[0], (options & ConnectionFlags.AllowCreate) != 0);
        }
    }
}
