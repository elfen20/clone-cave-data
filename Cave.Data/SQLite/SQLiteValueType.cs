namespace Cave.Data.SQLite
{
    /// <summary>
    /// Provides the available SQLite value types.
    /// </summary>
    public enum SQLiteValueType
    {
        /// <summary>undefined type value - do not use</summary>
        Undefined = 0,

        /// <summary>
        /// Numeric signed integer values
        /// </summary>
        INTEGER = 1,

        /// <summary>
        /// Text
        /// </summary>
        TEXT = 2,

        /// <summary>
        /// Binary data
        /// </summary>
        BLOB = 3,

        /// <summary>
        /// Floating point values
        /// </summary>
        REAL = 4,
    }
}
