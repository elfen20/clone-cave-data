using System;

namespace Cave
{
    /// <summary>
    /// Provides available field flags.
    /// </summary>
    [Flags]
    public enum FieldFlags
    {
        /// <summary>
        /// Provides the mask for field equals checks (index and unique presence may not be detected on some database)
        /// </summary>
        MatchMask = ~(Index | Unique),

        /// <summary>
        /// Default field type = no flags
        /// </summary>
        None = 0,

        /// <summary>
        /// Field is an ID field (This is an exclusive flag!)
        /// </summary>
        ID = 1,

        /// <summary>
        /// Field contains an unique value
        /// </summary>
        Unique = 2,

        /// <summary>
        /// Field is autoincrement
        /// </summary>
        AutoIncrement = 4,

        /// <summary>
        /// Field has an index
        /// </summary>
        Index = 8,
    }
}
