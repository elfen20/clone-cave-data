using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Cave.Compression.Tar;

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

        /// <summary>
        /// Saves the whole Storage to a tgz file.
        /// </summary>
        /// <param name="fileName">Filename to save to.</param>
        /// <param name="csv">If set the tables will be saved as csv files.</param>
        public void Save(string fileName, CsvProperties? csv = null)
        {
            using (var tar = TarWriter.CreateTGZ(fileName))
            {
                tar.AddFile("# MemoryDataBase 1.0.0.0 #", new byte[0]);
                foreach (MemoryDatabase database in databases.Values)
                {
                    foreach (var tableName in database.TableNames)
                    {
                        var table = database.GetTable(tableName);
                        using (var ms = new MemoryStream())
                        {
                            if (csv.HasValue)
                            {
                                CsvWriter.WriteTable(table, ms, csv.Value);
                            }
                            else
                            {
                                table.SaveTo(ms);
                            }
                            tar.AddFile($"{database.Name}/{table.Name}.dat", ms.ToArray());
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Loads a previously saved storage.
        /// </summary>
        /// <param name="fileName">Filename to load from.</param>
        /// <param name="csv">If set the tables will be read from csv files.</param>
        public void Load(string fileName, CsvProperties? csv = null)
        {
            using (var file = TarReader.ReadTGZ(fileName))
            {
                file.ReadNext(out TarEntry entry, out var data);
                if (entry.FileName != "# MemoryDataBase 1.0.0.0 #")
                {
                    throw new FormatException();
                }

                databases.Clear();
                while (file.ReadNext(out entry, out data))
                {
                    var databaseName = Path.GetDirectoryName(entry.FileName);
                    IDatabase database = new MemoryDatabase(this, databaseName);
                    var tableName = Path.GetFileName(entry.FileName);

                    using (Stream stream = new MemoryStream(data))
                    {
                        if (csv.HasValue)
                        {
                            var table = database.GetTable(tableName);
                            using (var reader = new CsvReader(table.Layout, stream, csv.Value))
                            {
                                table.Clear();
                                reader.ReadTable(table);
                            }
                        }
                        else
                        {
                            using (var reader = new DatReader(stream))
                            {
                                var table = database.GetTable(reader.Layout, TableFlags.CreateNew);
                                reader.ReadTable(table);
                            }
                        }
                    }
                }
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
