using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Cave.Compression.Tar;

namespace Cave.Data
{
    /// <summary>
    /// Provides a memory based storage engine for databases, tables and rows
    /// </summary>
    public sealed class MemoryStorage : Storage
    {
        /// <summary>Gets the default memory storage.</summary>
        /// <value>The default memory storage.</value>
        public static MemoryStorage Default { get; } = new MemoryStorage();

        readonly Dictionary<string, IDatabase> m_Databases = new Dictionary<string, IDatabase>();

        /// <summary>
        /// Creates a new memory storage.
        /// </summary>
        public MemoryStorage(DbConnectionOptions options = DbConnectionOptions.None) : base("memory://", options) { }

        /// <summary>
        /// Gets a value indicating whether the storage engine supports native transactions with faster execution than single commands.
        /// </summary>
        /// <value>
        /// <c>true</c> if supports native transactions; otherwise, <c>false</c>.
        /// </value>
        public override bool SupportsNativeTransactions { get; } = false;

        #region IStorage Member

        /// <summary>
        /// Saves the whole Storage to a tgz file
        /// </summary>
        /// <param name="fileName"></param>
        public void Save(string fileName)
        {
            using (TarWriter file = TarWriter.CreateTGZ(fileName))
            {
                file.AddFile("# MemoryDataBase 1.0.0.0 #", new byte[0]);
                foreach (MemoryDatabase database in m_Databases.Values)
                {
                    foreach (string tableName in database.TableNames)
                    {
                        MemoryTable table = (MemoryTable)database.GetTable(tableName);
                        table.SaveTo(file);
                    }
                }
            }
        }

        /// <summary>
        /// Loads a previously saved storage
        /// </summary>
        /// <param name="fileName"></param>
        public void Load(string fileName)
        {
            using (TarReader file = TarReader.ReadTGZ(fileName))
            {
                file.ReadNext(out TarEntry entry, out byte[] data);
                if (entry.FileName != "# MemoryDataBase 1.0.0.0 #")
                {
                    throw new FormatException();
                }

                IDatabase database = null;
                while (file.ReadNext(out entry, out data))
                {
                    string databaseName = Path.GetDirectoryName(entry.FileName);
                    string tableName = Path.GetFileName(entry.FileName);

                    using (Stream stream = new MemoryStream(data))
                    {
                        DatReader reader = new DatReader(stream);
                        IMemoryTable table = (IMemoryTable)database.GetTable(reader.Layout, TableFlags.CreateNew);
                        reader.ReadTable(table);
                    }
                }
            }
        }

        /// <summary>
        /// Checks whether the database with the specified name exists at the database or not
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public override bool HasDatabase(string database)
        {
            return m_Databases.ContainsKey(database);
        }

        /// <summary>
        /// Obtains all available database names
        /// </summary>
        public override string[] DatabaseNames
        {
            get
            {
                if (Closed)
                {
                    throw new ObjectDisposedException(ToString());
                }

                return m_Databases.Keys.ToArray();
            }
        }

        /// <summary>
        /// Obtains the database with the specified name
        /// </summary>
        /// <param name="database">The name of the database</param>
        /// <returns></returns>
        public override IDatabase GetDatabase(string database)
        {
            if (Closed)
            {
                throw new ObjectDisposedException(ToString());
            }

            if (!HasDatabase(database))
            {
                throw new ArgumentException(string.Format("The requested database '{0}' was not found!", database));
            }

            return m_Databases[database];
        }

        /// <summary>
        /// Adds a new database with the specified name
        /// </summary>
        /// <param name="databaseName">The name of the database</param>
        /// <returns></returns>
        public override IDatabase CreateDatabase(string databaseName)
        {
            if (Closed)
            {
                throw new ObjectDisposedException(ToString());
            }

            if (HasDatabase(databaseName))
            {
                throw new InvalidOperationException(string.Format("Database '{0}' already exists!", databaseName));
            }

            IDatabase database = new MemoryDatabase(this, databaseName);
            m_Databases.Add(databaseName, database);
            return database;
        }

        /// <summary>
        /// Removes the specified database
        /// </summary>
        /// <param name="database"></param>
        public override void DeleteDatabase(string database)
        {
            if (Closed)
            {
                throw new ObjectDisposedException(ToString());
            }

            if (!m_Databases.Remove(database))
            {
                throw new ArgumentException(string.Format("The requested database '{0}' was not found!", database));
            }
        }

        #endregion
    }
}
