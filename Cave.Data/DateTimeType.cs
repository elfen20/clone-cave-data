namespace Cave.Data
{
    /// <summary>Provides available encodings for the date time values in databases.</summary>
    public enum DateTimeType
    {
        /// <summary>Undefined date time type. This will throw an error on any interop.</summary>
        Undefined = 0,

        /// <summary>Native datetime field at the database</summary>
        Native = 1,

        /// <summary>Human readable bigint field. <see cref="Storage.BigIntDateTimeFormat" />.</summary>
        BigIntHumanReadable = 2,

        /// <summary>Use dotnet ticks.</summary>
        /// <remarks>
        ///     A single tick represents one hundred nanoseconds or one ten-millionth of a second. There are 10,000 ticks in a
        ///     millisecond, or 10 million ticks in a second.
        /// </remarks>
        BigIntTicks = 3,

        /// <summary>Use a decimal field with full available decimal precision to store the seconds since year 0, day 1.</summary>
        DecimalSeconds = 4,

        /// <summary>Use a double field to store the seconds since year 0, day 1.</summary>
        DoubleSeconds = 5,

        /// <summary>Use a double field to store the seconds since year 1970, day 1 (unix epoch).</summary>
        DoubleEpoch = 6
    }
}
