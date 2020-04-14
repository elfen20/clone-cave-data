using System;
using System.Collections.Generic;
using System.Linq;

namespace Cave.Data
{
    /// <summary>
    /// Provides a memory based database.
    /// </summary>
    public sealed class MemoryDatabase : Database
    {
        Dictionary<string, ITable> tables = new Dictionary<string, ITable>();

        /// <summary>
        /// Initializes a new instance of the <see cref="MemoryDatabase"/> class.
        /// </summary>
        /// <param name="storage">The storage engine.</param>
        /// <param name="name">The name of the database.</param>
        public MemoryDatabase(MemoryStorage storage, string name)
            : base(storage, name)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MemoryDatabase"/> class.
        /// </summary>
        public MemoryDatabase()
            : base(MemoryStorage.Default, "Default")
        {
        }

        /// <summary>Gets the default memory database.</summary>
        /// <value>The default memory database.</value>
        public static MemoryDatabase Default { get; } = new MemoryDatabase();

        /// <inheritdoc/>
        public override bool IsSecure => true;

        /// <inheritdoc/>
        public override bool IsClosed => tables == null;

        /// <inheritdoc/>
        public override ITable GetTable(string table, TableFlags flags = default) => tables[table];

        /// <inheritdoc/>
        public override ITable CreateTable(RowLayout layout, TableFlags flags = default)
        {
            if (layout == null)
            {
                throw new ArgumentNullException("Layout");
            }

            if (tables.ContainsKey(layout.Name))
            {
                throw new InvalidOperationException(string.Format("Table '{0}' already exists!", layout.Name));
            }

            var table = MemoryTable.Create(layout);
            tables[layout.Name] = table;
            return table;
        }

        /// <inheritdoc/>
        public override void DeleteTable(string table)
        {
            if (!tables.Remove(table))
            {
                throw new ArgumentException(string.Format("Table '{0}' does not exist!", table));
            }
        }

        /// <inheritdoc/>
        public override void Close()
        {
            if (IsClosed)
            {
                throw new ObjectDisposedException(Name);
            }

            tables.Clear();
            tables = null;
        }

        /// <inheritdoc/>
        protected override string[] GetTableNames() => !IsClosed ? tables.Keys.ToArray() : throw new ObjectDisposedException(Name);
    }
}
