using System;

namespace Cave.Data
{
    /// <summary>Transaction flags.</summary>
    [Flags]
    public enum TransactionFlags
    {
        /// <summary>No settings</summary>
        None = 0,

        /// <summary>Throw exceptions</summary>
        ThrowExceptions = 1,

        /// <summary>The default settings</summary>
        Default = 0xFFFF
    }
}
