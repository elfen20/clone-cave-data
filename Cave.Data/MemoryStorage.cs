using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Cave.Data
{
    /// <summary>
    /// Provides a memory based storage engine for databases, tables and rows.
    /// </summary>
    public sealed class MemoryStorage : Storage
    {
        readonly Dictionary<string, IDatabase> databases = new Dictionary<string, IDatabase>();

        /// <summary>
        /// Initializes a new instance of the <see cref="MemoryStorage"/> class.
        /// </summary>
        /// <param name="options">Options for the database.</param>
        public MemoryStorage(ConnectionFlags options = ConnectionFlags.None)
            : base("memory://", options)
        {
        }

        /// <summary>Gets the default memory storage.</summary>
        /// <value>The default memory storage.</value>
        public static MemoryStorage Default { get; } = new MemoryStorage();

        /// <inheritdoc/>
        public override bool SupportsNativeTransactions { get; } = false;

        /// <inheritdoc/>
        public override string[] DatabaseNames
        {
            get
            {
                if (Closed)
                {
                    throw new ObjectDisposedException(ToString());
                }

                return databases.Keys.ToArray();
            }
        }

        /// <inheritdoc/>
        public override bool HasDatabase(string database)
        {
            return databases.ContainsKey(database);
        }

        /// <inheritdoc/>
        public override IDatabase GetDatabase(string database)
        {
            if (Closed)
            {
                throw new ObjectDisposedException(ToString());
            }

            if (!HasDatabase(database))
            {
                throw new ArgumentException($"The requested database '{database}' was not found!");
            }

            return databases[database];
        }

        /// <inheritdoc/>
        public override IDatabase CreateDatabase(string databaseName)
        {
            if (Closed)
            {
                throw new ObjectDisposedException(ToString());
            }

            if (HasDatabase(databaseName))
            {
                throw new InvalidOperationException($"Database '{databaseName}' already exists!");
            }

            IDatabase database = new MemoryDatabase(this, databaseName);
            databases.Add(databaseName, database);
            return database;
        }

        /// <inheritdoc/>
        public override void DeleteDatabase(string database)
        {
            if (Closed)
            {
                throw new ObjectDisposedException(ToString());
            }

            if (!databases.Remove(database))
            {
                throw new ArgumentException($"The requested database '{database}' was not found!");
            }
        }
    }
}
