using System;

namespace Cave.Data
{
    /// <summary>
    /// Provides an interface for read table cache
    /// </summary>
    /// <seealso cref="ITable" />
    public interface IReadCachedTable : ITable
    {
        /// <summary>
        /// Obtains the current cache generation (this will increase on each update)
        /// </summary>
        int Generation { get; }

        /// <summary>
        /// Updates the whole table.
        /// </summary>
        void UpdateCache();

        /// <summary>Gets the last update date and time.</summary>
        /// <value>The last update date and time.</value>
        DateTime LastUpdate { get; }
    }

    /// <summary>
    /// Provides an interface for read table cache
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <seealso cref="ITable" />
    public interface IReadCachedTable<T> : ITable<T>, IReadCachedTable where T : struct
    {
    }
}
