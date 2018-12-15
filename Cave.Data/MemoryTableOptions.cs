using System;

namespace Cave.Data
{
    /// <summary>
    /// Provides options for memory tables
    /// </summary>
    [Flags]
    public enum MemoryTableOptions
    {
        /// <summary>Disable in memory indices. This will reduce memory consuption but requires full table scans for each search!</summary>
        DisableIndex = 1,
    }
}
