using System;

namespace Cave.Data
{
    /// <summary>
    /// Provides available field flags.
    /// </summary>
    [Flags]
    public enum FieldFlags
    {
        /// <summary>
        /// Default field type = no flags
        /// </summary>
        None = 0,

        /// <summary>
        /// Field is an ID field
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
