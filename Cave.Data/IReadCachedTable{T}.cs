using System;

namespace Cave.Data
{
    /// <summary>
    /// Provides an interface for read table cache.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <seealso cref="ITable" />
    public interface IReadCachedTable<T> : ITable<T>, IReadCachedTable
        where T : struct
    {
    }
}
