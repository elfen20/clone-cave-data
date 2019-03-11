using System;

namespace Cave.Data
{
    /// <summary>
    /// Table creation / loading flags.
    /// </summary>
    [Flags]
    public enum TableFlags
    {
        /// <summary>
        /// The default mode:<br />
        /// - Do not allow table creation<br />
        /// - Use storage engine defaults<br />
        /// </summary>
        None = 0,

        /// <summary>Allows creation of the table if it does not exist</summary>
        AllowCreate = 0x1,

        /// <summary>Always create the table</summary>
        CreateNew = 0x2,

        /// <summary>Tell the storage engine to use in memory table. This will throw an exception if the storage engine cannot store to memory.</summary>
        InMemory = 0x4,
    }
}
