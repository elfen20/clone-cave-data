namespace Cave
{
    /// <summary>
    /// Provides available encodings for the date time values in databases
    /// </summary>
    public enum DateTimeType
    {
        /// <summary>
        /// Undefined date time type. This will throw an error on any interop.
        /// </summary>
        Undefined = 0,

        /// <summary>
        /// Use the native datetime field at the database
        /// </summary>
        Native = 1,

        /// <summary>
        /// Use a human readable bigint field
        /// </summary>
        BigIntHumanReadable = 2,

        /// <summary>
        /// Use ticks
        /// </summary>
        BigIntTicks = 3,

        /// <summary>
        /// Use a decimal field with full available decimal precision (default 65,30)
        /// </summary>
        DecimalSeconds = 4,
    }
}
