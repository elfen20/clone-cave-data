using System;
using System.IO;

namespace Cave.Data
{
    /// <summary>
    /// Provides a simple directory based storage engine
    /// </summary>
    public sealed class DatStorage : FileStorage
    {
        /// <summary>
        /// Opens a file storage.
        /// <para>
        /// Following formats are supported:<br />
        /// file://server/relativepath<br />
        /// file:absolutepath<br /></para>
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="options">The options.</param>
        public DatStorage(string connectionString, DbConnectionOptions options)
            : this(ConnectionString.Parse(connectionString), options)
        {
        }

        /// <summary>
        /// Opens a file storage.
        /// <para>
        /// Following formats are supported:<br />
        /// file://server/relativepath<br />
        /// file:absolutepath<br /></para>
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="options"></param>
        public DatStorage(ConnectionString connectionString, DbConnectionOptions options)
            : base(connectionString, options)
        {
        }

        /// <summary>
        /// Obtains the database with the specified name
        /// </summary>
        /// <param name="database">Name of the database</param>
        /// <returns></returns>
        public override IDatabase GetDatabase(string database)
        {
            if (Closed)
            {
                throw new ObjectDisposedException(ToString());
            }

            return new DatDatabase(this, Path.Combine(Folder, database));
        }

        /// <summary>
        /// Gets a value indicating whether the storage engine supports native transactions with faster execution than single commands.
        /// </summary>
        /// <value>
        /// <c>true</c> if supports native transactions; otherwise, <c>false</c>.
        /// </value>
        public override bool SupportsNativeTransactions { get; } = false;
    }
}
