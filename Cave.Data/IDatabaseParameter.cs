namespace Cave.Data
{
    /// <summary>
    /// Provides an interface to database parameters.
    /// </summary>
    public interface IDatabaseParameter
    {
        /// <summary>
        /// Gets/sets the name of the <see cref="IDatabaseParameter"/>.
        /// </summary>
        string Name { get; }

        /// <summary>
        /// Gets/sets the value of the <see cref="IDatabaseParameter"/>.
        /// </summary>
        object Value { get; }
    }
}
