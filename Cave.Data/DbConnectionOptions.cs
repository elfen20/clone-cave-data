using System;

namespace Cave.Data
{
    /// <summary>
    /// Database connection options
    /// </summary>
    [Flags]
    public enum DbConnectionOptions
    {
        /// <summary>No options</summary>
        None = 0,

        /// <summary>The allow unsafe connections without ssl/tls/encryption</summary>
        /// <remarks>All data and the credentials of the database user may be transmitted without any security!</remarks>
        AllowUnsafeConnections = 1,

        /// <summary>Allow to create the database if it does not exists</summary>
        AllowCreate = 2,

        /// <summary>Enable verbose logging</summary>
        VerboseLogging = 4,
    }
}
