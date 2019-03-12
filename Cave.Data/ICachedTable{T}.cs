namespace Cave.Data
{
    /// <summary>
    /// Provides an interface for tables caching data.
    /// </summary>
    public interface ICachedTable<T> : IMemoryTable<T>, ICachedTable
        where T : struct
    {
    }
}
