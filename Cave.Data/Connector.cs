using System;
using Cave.Data.Microsoft;
using Cave.Data.Mysql;
using Cave.Data.Postgres;
using Cave.Data.SQLite;

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
        /// <exception cref="NotSupportedException"></exception>
        public static IStorage ConnectStorage(ConnectionString connectionString, DbConnectionOptions options)
        {
            switch (connectionString.ConnectionType)
            {
                case ConnectionType.MEMORY: return new MemoryStorage();
                case ConnectionType.FILE: return new DatStorage(connectionString, options);
                case ConnectionType.MYSQL: return new MySqlStorage(connectionString, options);
                case ConnectionType.MSSQL: return new MsSqlStorage(connectionString, options);
                case ConnectionType.SQLITE: return new SQLiteStorage(connectionString, options);
                case ConnectionType.PGSQL: return new PgSqlStorage(connectionString, options);
                default: throw new NotSupportedException(string.Format("Unknown database provider '{0}'!", connectionString.Protocol));
            }
        }

        /// <summary>Connects to a database using the specified <see cref="ConnectionString" />.</summary>
        /// <param name="connection">The ConnectionString.</param>
        /// <param name="options">The database connection options.</param>
        /// <returns>Returns a new database connection.</returns>
        /// <exception cref="ArgumentException">Missing database name at connection string!.</exception>
        /// <exception cref="Exception">Missing database name at connection string!.</exception>
        public static IDatabase ConnectDatabase(ConnectionString connection, DbConnectionOptions options = DbConnectionOptions.None)
        {
            IStorage storage = ConnectStorage(connection, options);
            if (connection.Location == null)
            {
                throw new ArgumentOutOfRangeException("connection", "Database name not specified!");
            }

            var parts = connection.Location.Split(new char[] { '/' }, StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length < 1)
            {
                throw new ArgumentException("Missing database name at connection string!");
            }

            return storage.GetDatabase(parts[0], (options & DbConnectionOptions.AllowCreate) != 0);
        }

        /// <summary>Connects to a database using the specified <see cref="ISettings" />.</summary>
        /// <param name="settings">The settings.</param>
        /// <param name="databaseName">Name of the database.</param>
        /// <param name="options">The options.</param>
        /// <returns>Returns a new database connection.</returns>
        public static IDatabase ConnectDatabase(ISettings settings, string databaseName = null, DbConnectionOptions options = DbConnectionOptions.None)
        {
            // use [Database]Connection if present
            var connectionString = settings.ReadSetting("Database", "Connection");
            if (connectionString != null)
            {
                return ConnectDatabase(connectionString);
            }

            // not present, read database name
            if (databaseName == null)
            {
                databaseName = settings.ReadString("Database", "Database");
            }

            // prepare database name if none specified
            if (databaseName == null)
            {
                var serviceName = AssemblyVersionInfo.Program.Product;
                var service = serviceName.GetValidChars(ASCII.Strings.SafeName).ToLower();
                var programID = Base32.Safe.Encode(AppDom.ProgramID);
                var machine = Environment.MachineName.Split('.')[0].GetValidChars(ASCII.Strings.SafeName).ToLower();
                databaseName = $"{service}_{machine}_{programID}";
            }

            // read the [Database] section
            var type = settings.ReadString("Database", "Type");
            var user = settings.ReadString("Database", "Username");
            var pass = settings.ReadString("Database", "Password");
            var server = settings.ReadString("Database", "Server");
            return ConnectDatabase($"{type}://{user}:{pass}@{server}/{databaseName}", options);
        }
    }
}
