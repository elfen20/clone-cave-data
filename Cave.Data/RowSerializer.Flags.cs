namespace Cave.Data
{
    /// <summary>
    /// Provides Row based serialization.
    /// </summary>
    public static partial class RowSerializer
    {
        /// <summary>
        /// Settings used during de/serialization.
        /// </summary>
        public enum Flags
        {
            /// <summary>No flags</summary>
            None = 0,

            /// <summary>Serialize the layout first, then the data. This adds type safety to the stream but costs a lot of bandwith and time.</summary>
            WithLayout = 1,
        }
    }
}
