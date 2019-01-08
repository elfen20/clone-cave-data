using System;
using System.Diagnostics;

namespace Cave.Data
{
    /// <summary>
    /// Provides a base class implementing the <see cref="IDatabase"/> interface.
    /// </summary>
    public abstract class Database : IDatabase
    {
        /// <summary>Gets a value indicating whether this instance is using a secure connection to the storage.</summary>
        /// <value><c>true</c> if this instance is using a secure connection; otherwise, <c>false</c>.</value>
        public abstract bool IsSecure { get; }

        /// <summary>
        /// Creates a new database instance
        /// </summary>
        /// <param name="storage">The storage engine</param>
        /// <param name="name">The name of the database</param>
        protected Database(IStorage storage, string name)
        {
            if (storage == null)
            {
                throw new ArgumentNullException("Storage");
            }

            if (name == null)
            {
                throw new ArgumentNullException("Name");
            }

            Name = name;
            Storage = storage;
        }

        #region IDatabase Member

        /// <summary>
        /// The storage engine the database belongs to
        /// </summary>
        public IStorage Storage { get; private set; }

        /// <summary>
        /// Obtains the name of the database
        /// </summary>
        public string Name { get; private set; }

        /// <summary>
        /// Obtains the available table names
        /// </summary>
        public abstract string[] TableNames { get; }

        /// <summary>
        /// Obtains whether the specified table exists or not
        /// </summary>
        /// <param name="table">The name of the table</param>
        /// <returns></returns>
        public abstract bool HasTable(string table);

        #region GetTable functions
        /// <summary>
        /// Opens the table with the specified type
        /// </summary>
        /// <param name="table">The name of the table</param>
        /// <returns>Returns an <see cref="ITable"/> instance for the specified table</returns>
        public abstract ITable GetTable(string table);

        /// <summary>
        /// Opens the table with the specified layout
        /// </summary>
        /// <returns>Returns an <see cref="ITable"/> instance for the specified table</returns>
        protected abstract ITable<T> OpenTable<T>(RowLayout layout) where T : struct;

        /// <summary>
        /// Opens or creates the table with the specified type
        /// </summary>
        /// <typeparam name="T">The row struct to use for the table</typeparam>
        /// <returns>Returns an <see cref="ITable{T}"/> instance for the specified table</returns>
        public ITable<T> GetTable<T>() where T : struct { return GetTable<T>(0, null); }

        /// <summary>
        /// Opens or creates the table with the specified type
        /// </summary>
        /// <typeparam name="T">The row struct to use for the table</typeparam>
        /// <param name="flags">Flags for table loading</param>
        /// <returns>Returns an <see cref="ITable{T}"/> instance for the specified table</returns>
        public ITable<T> GetTable<T>(TableFlags flags) where T : struct { return GetTable<T>(flags, null); }

        /// <summary>
        /// Opens or creates the table with the specified type
        /// </summary>
        /// <typeparam name="T">The row struct to use for the table</typeparam>
        /// <param name="table">The name of the table or null</param>
        /// <returns>Returns an <see cref="ITable{T}"/> instance for the specified table</returns>
        public ITable<T> GetTable<T>(string table) where T : struct { return GetTable<T>(TableFlags.None, table); }

        /// <summary>
        /// Opens or creates the table with the specified type
        /// </summary>
        /// <typeparam name="T">The row struct to use for the table</typeparam>
        /// <param name="flags">Flags for table loading</param>
        /// <param name="tableName">The name of the table or null</param>
        /// <returns>Returns an <see cref="ITable{T}"/> instance for the specified table</returns>
        public ITable<T> GetTable<T>(TableFlags flags, string tableName) where T : struct
        {
            RowLayout layout = RowLayout.CreateTyped(typeof(T), tableName, Storage);
            if (0 != (flags & TableFlags.CreateNew))
            {
                if (HasTable(layout.Name))
                {
                    DeleteTable(layout.Name);
                }

                return CreateTable<T>(flags, layout.Name);
            }
            if (HasTable(layout.Name))
            {
                ITable<T> table = OpenTable<T>(layout);
                return table;
            }
            if (0 == (flags & TableFlags.AllowCreate))
            {
                throw new InvalidOperationException(string.Format("Table '{0}' does not exist!", layout.Name));
            }

            return CreateTable<T>(flags, tableName);
        }

        /// <summary>
        /// Opens or creates the table with the specified name
        /// </summary>
        /// <param name="layout">Layout of the table</param>
        /// <returns>Returns an <see cref="ITable"/> instance for the specified table</returns>
        public ITable GetTable(RowLayout layout) { return GetTable(layout, TableFlags.None); }

        /// <summary>
        /// Opens or creates the table with the specified name
        /// </summary>
        /// <param name="layout">Layout of the table</param>
        /// <param name="flags">Flags for table loading</param>
        /// <returns>Returns an <see cref="ITable"/> instance for the specified table</returns>
        public ITable GetTable(RowLayout layout, TableFlags flags)
        {
            if (layout == null)
            {
                throw new ArgumentNullException("Layout");
            }

            if (0 != (flags & TableFlags.CreateNew))
            {
                if (HasTable(layout.Name))
                {
                    DeleteTable(layout.Name);
                }

                return CreateTable(layout, flags);
            }
            if (HasTable(layout.Name))
            {
                ITable table = GetTable(layout.Name);
                Storage.CheckLayout(layout, table.Layout);
                return table;
            }
            if (0 == (flags & TableFlags.AllowCreate))
            {
                throw new InvalidOperationException(string.Format("Table '{0}' does not exist!", layout.Name));
            }

            return CreateTable(layout, flags);
        }

        #endregion

        #region CreateTable functions

        /// <summary>Logs the table layout.</summary>
        /// <param name="layout">The layout.</param>
        protected void LogCreateTable(RowLayout layout)
        {
            Trace.TraceInformation("Creating table <cyan>{0}.{1}<default> with <cyan>{2}<default> fields.", Name, layout.Name, layout.FieldCount);
            if (Storage.LogVerboseMessages)
            {
                for (int i = 0; i < layout.FieldCount; i++)
                {
                    Trace.TraceInformation(layout.GetProperties(i).ToString());
                }
            }
        }

        /// <summary>
        /// Adds a new table with the specified layout
        /// </summary>
        /// <param name="layout">Layout of the table</param>
        /// <returns>Returns an <see cref="ITable"/> instance for the specified table</returns>
        public ITable CreateTable(RowLayout layout) { return CreateTable(layout, 0); }

        /// <summary>
        /// Adds a new table with the specified layout
        /// </summary>
        /// <param name="layout">Layout of the table</param>
        /// <param name="flags">The table creation flags</param>
        /// <returns>Returns an <see cref="ITable"/> instance for the specified table</returns>
        public abstract ITable CreateTable(RowLayout layout, TableFlags flags);

        /// <summary>
        /// Adds a new table with the specified type
        /// </summary>
        /// <typeparam name="T">The row struct to use for the table</typeparam>
        /// <returns>Returns an <see cref="ITable{T}"/> instance for the specified table</returns>
        public ITable<T> CreateTable<T>() where T : struct { return CreateTable<T>(0, null); }

        /// <summary>
        /// Adds a new table with the specified type
        /// </summary>
        /// <typeparam name="T">The row struct to use for the table</typeparam>
        /// <param name="flags">The table creation flags</param>
        /// <returns>Returns an <see cref="ITable{T}"/> instance for the specified table</returns>
        public ITable<T> CreateTable<T>(TableFlags flags) where T : struct { return CreateTable<T>(flags, null); }

        /// <summary>
        /// Adds a new table with the specified type
        /// </summary>
        /// <typeparam name="T">The row struct to use for the table</typeparam>
        /// <param name="table">The name of the table or null</param>
        /// <returns>Returns an <see cref="ITable{T}"/> instance for the specified table</returns>
        public ITable<T> CreateTable<T>(string table) where T : struct { return CreateTable<T>(TableFlags.None, table); }

        /// <summary>
        /// Adds a new table with the specified type
        /// </summary>
        /// <typeparam name="T">The row struct to use for the table</typeparam>
        /// <param name="flags">The table creation flags</param>
        /// <param name="table">The name of the table or null</param>
        /// <returns>Returns an <see cref="ITable{T}"/> instance for the specified table</returns>
        public abstract ITable<T> CreateTable<T>(TableFlags flags, string table) where T : struct;

        #endregion

        /// <summary>
        /// Loads a whole table into memory
        /// </summary>
        /// <param name="table">The name of the table</param>
        /// <returns>Returns a new <see cref="MemoryTable"/> instance containing all row of the table</returns>
        public MemoryTable Load(string table)
        {
            return GetTable(table).ToMemory();
        }

        /// <summary>
        /// Loads a whole table into memory
        /// </summary>
        /// <typeparam name="T">The row struct to use for the table</typeparam>
        /// <returns>Returns a new <see cref="MemoryTable{T}"/> instance containing all row of the table</returns>
        public MemoryTable<T> Load<T>() where T : struct { return Load<T>(null); }

        /// <summary>
        /// Loads a whole table into memory
        /// </summary>
        /// <typeparam name="T">The row struct to use for the table</typeparam>
        /// <param name="table">The name of the table or null, default value is read from Table attribute</param>
        /// <returns>Returns a new <see cref="MemoryTable{T}"/> instance containing all row of the table</returns>
        public MemoryTable<T> Load<T>(string table) where T : struct
        {
            return GetTable<T>(TableFlags.None, table).ToTypedMemory();
        }

        /// <summary>
        /// Removes a table from the database
        /// </summary>
        /// <param name="table">The name of the table</param>
        public abstract void DeleteTable(string table);

        /// <summary>
        /// Closes the database instance
        /// </summary>
        public abstract void Close();

        /// <summary>
        /// Obtains whether the database was already closed or not
        /// </summary>
        public abstract bool Closed { get; }
        #endregion

        /// <summary>
        /// Database {Name} [in]secure
        /// </summary>
        /// <returns>Database {Name} [in]secure</returns>
        public override string ToString()
        {
            if (IsSecure)
            {
                return string.Format("Database {0} secure", Name);
            }
            else
            {
                return string.Format("Database {0} insecure", Name);
            }
        }
    }
}
