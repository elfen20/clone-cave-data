using System;

namespace Cave.Data.Sql
{
    /// <summary>
    /// Provides a <see cref="IDatabase"/> implementation for sql92 databases
    /// </summary>
    public abstract class SqlDatabase : Database
    {
        bool m_Closed = false;

        /// <summary>
        /// Obtains whether this instance was closed
        /// </summary>
        public override bool Closed => m_Closed;

        /// <summary>
        /// Creates a new SqlDatabase instance
        /// </summary>
        /// <param name="storage">The storage engine the database belongs to</param>
        /// <param name="name">The name of the database</param>
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
        /// Obtains the underlying SqlStorage engine
        /// </summary>
        protected SqlStorage SqlStorage { get; private set; }

        #region IDatabase Member

        /// <summary>
        /// Removes a table from the database
        /// </summary>
        /// <param name="table"></param>
        public override void DeleteTable(string table)
        {
            SqlStorage.Execute(Name, table, "DROP TABLE " + SqlStorage.FQTN(Name, table));
        }

        /// <summary>
        /// Closes the instance and flushes all cached data
        /// </summary>
        public override void Close()
        {
            if (Closed)
            {
                throw new ObjectDisposedException(Name);
            }

            m_Closed = true;
        }

        #endregion

        /// <summary>
        /// Obtains the name database
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return Name;
        }
    }
}
