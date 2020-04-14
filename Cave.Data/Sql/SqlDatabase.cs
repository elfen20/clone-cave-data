using System;

namespace Cave.Data.Sql
{
    /// <summary>
    /// Provides a <see cref="IDatabase"/> implementation for sql92 databases.
    /// </summary>
    public abstract class SqlDatabase : Database
    {
        bool closed = false;

        /// <summary>
        /// Initializes a new instance of the <see cref="SqlDatabase"/> class.
        /// </summary>
        /// <param name="storage">The storage engine the database belongs to.</param>
        /// <param name="name">The name of the database.</param>
        protected SqlDatabase(SqlStorage storage, string name)
            : base(storage, name)
        {
            SqlStorage = storage;
            if (name.HasInvalidChars(ASCII.Strings.SafeName))
            {
                throw new ArgumentException("Name contains invalid chars!");
            }
        }

        /// <summary>
        /// Gets a value indicating whether this instance was closed.
        /// </summary>
        public override bool IsClosed => closed;

        /// <summary>
        /// Gets the underlying SqlStorage engine.
        /// </summary>
        protected SqlStorage SqlStorage { get; private set; }

        #region IDatabase Member

        /// <inheritdoc/>
        public override void DeleteTable(string table)
        {
            SqlStorage.Execute(database: Name, table: table, cmd: "DROP TABLE " + SqlStorage.FQTN(Name, table));
        }

        /// <inheritdoc/>
        public override void Close()
        {
            if (IsClosed)
            {
                throw new ObjectDisposedException(Name);
            }

            closed = true;
        }

        #endregion
    }
}
