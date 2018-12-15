using System;
using System.Collections.Generic;
using System.IO;

namespace Cave.Data
{
    /// <summary>
    /// Provides a simple directory based database storage
    /// </summary>
    public sealed class DatDatabase : FileDatabase
    {
        /// <summary>Returns true assuming that no one else accesses the database file</summary>
        /// <value><c>true</c></value>
        public override bool IsSecure => true;

        /// <summary>
        /// Creates a new <see cref="DatDatabase"/> instance
        /// </summary>
        /// <param name="storage">The underlying storage engine</param>
        /// <param name="directory">The path to the database directory</param>
        internal DatDatabase(DatStorage storage, string directory)
            : base(storage, directory)
        {
        }

        #region IDatabase Member

        /// <summary>
        /// Obtains a list with all tablenames
        /// </summary>
        public override string[] TableNames
        {
            get
            {
                if (Closed)
                {
                    throw new ObjectDisposedException(Name);
                }

                List<string> result = new List<string>();
                foreach (string directory in Directory.GetFiles(Folder, "*.dat", SearchOption.TopDirectoryOnly))
                {
                    result.Add(Path.ChangeExtension(directory, ""));
                }
                return result.ToArray();
            }
        }

        /// <summary>
        /// Obtains whether the specified table exists or not
        /// </summary>
        /// <param name="table">The name of the table</param>
        /// <returns></returns>
        public override bool HasTable(string table)
        {
            if (Closed)
            {
                throw new ObjectDisposedException(Name);
            }

            return File.Exists(Path.Combine(Folder, table + ".dat"));
        }

        /// <summary>
        /// Opens and retrieves the existing table with the specified layout
        /// </summary>
        /// <typeparam name="T">Row structure type</typeparam>
        /// <param name="layout">Layout and name of the table</param>
        /// <returns>Returns a table instance</returns>
        protected override ITable<T> OpenTable<T>(RowLayout layout)
        {
            return new DatTable<T>(this, layout, Path.Combine(Folder, layout.Name + ".dat"));
        }

        /// <summary>Opens the table with the specified name</summary>
        /// <param name="table">The name of the table</param>
        /// <returns>Returns an <see cref="ITable" /> instance for the specified table</returns>
        /// <exception cref="InvalidOperationException">Layout not defined and need to call CreateTable!</exception>
        /// <exception cref="Exception"></exception>
        public override ITable GetTable(string table)
        {
            if (!HasTable(table))
            {
                throw new InvalidOperationException(string.Format("Table '{0}' does not exist!", table));
            }

            return new DatTable(this, Path.Combine(Folder, table + ".dat"));
        }


        /// <summary>Adds a new table with the specified name</summary>
        /// <param name="layout">Layout of the table</param>
        /// <param name="flags">The table creation flags</param>
        /// <returns>Returns an <see cref="ITable" /> instance for the specified table</returns>
        /// <exception cref="NotSupportedException"></exception>
        /// <exception cref="Exception"></exception>
        public override ITable CreateTable(RowLayout layout, TableFlags flags)
        {
            if (layout == null)
            {
                throw new ArgumentNullException("Layout");
            }

            if (0 != (flags & TableFlags.InMemory))
            {
                throw new NotSupportedException(string.Format("Table '{0}' does not support TableFlags.{1}", layout.Name, TableFlags.InMemory));
            }
            if (HasTable(layout.Name))
            {
                if (0 == (flags & TableFlags.CreateNew))
                {
                    throw new InvalidOperationException(string.Format("Table '{0}' already exists!", layout.Name));
                }
                else
                {
                    DeleteTable(layout.Name);
                }
            }
            return new DatTable(this, Path.Combine(Folder, layout.Name + ".dat"), layout);
        }

        /// <summary>Adds a new table with the specified type</summary>
        /// <typeparam name="T">The row struct to use for the table</typeparam>
        /// <param name="flags">The table creation flags</param>
        /// <returns>Returns an <see cref="ITable{T}" /> instance for the specified table</returns>
        /// <exception cref="NotSupportedException"></exception>
        /// <exception cref="Exception"></exception>
        /// <param name="table">Tablename to create (optional, use this to overwrite the default table name)</param>
        public override ITable<T> CreateTable<T>(TableFlags flags, string table)
        {
            RowLayout layout = RowLayout.CreateTyped(typeof(T), table, Storage);
            LogCreateTable(layout);
            if (0 != (flags & TableFlags.InMemory))
            {
                throw new NotSupportedException(string.Format("{0} does not support TableFlags.{1}", "DatDatabase", TableFlags.InMemory));
            }

            if (HasTable(layout.Name))
            {
                if (0 == (flags & TableFlags.CreateNew))
                {
                    throw new InvalidOperationException(string.Format("Table '{0}' already exists!", layout.Name));
                }
                else
                {
                    DeleteTable(layout.Name);
                }
            }
            return new DatTable<T>(this, layout, Path.Combine(Folder, layout.Name + ".dat"));
        }

        /// <summary>
        /// Removes a table from the database
        /// </summary>
        /// <param name="table"></param>
        public override void DeleteTable(string table)
        {
            if (!HasTable(table))
            {
                throw new InvalidOperationException(string.Format("Table '{0}' does not exist!", table));
            }

            File.Delete(Path.Combine(Folder, table + ".dat"));
            string indexFile = Path.Combine(Folder, table + ".dat.idx");
            if (File.Exists(indexFile))
            {
                File.Delete(indexFile);
            }
        }

        #endregion
    }
}
