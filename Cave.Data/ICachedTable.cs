namespace Cave.Data
{
    /// <summary>
    /// Provides an interface for tables caching data
    /// </summary>
    public interface ICachedTable : IMemoryTable
    {
        /// <summary>Gets the base table.</summary>
        /// <value>The base table.</value>
        ITable BaseTable { get; }

        /// <summary>Reloads this instance from the database.</summary>
        void Reload();

        /// <summary>Flushes all changes done to this instance.</summary>
        void Flush();

        /// <summary>Closes this instance after flushing all data.</summary>
        void Close();
    }

    /// <summary>
    /// Provides an interface for tables caching data
    /// </summary>
    public interface ICachedTable<T> : IMemoryTable<T>, ICachedTable where T : struct { }
}
