namespace Cave.Data
{
    /// <summary>
    /// Types for table connections.
    /// </summary>
    public enum TableConnectorMode
    {
        /// <summary>undefined operation mode</summary>
        Undefined = 0,

        /// <summary>memory tables. this is the default.</summary>
        Memory,

        /// <summary>direct table connection</summary>
        Direct,

        /// <summary>background writer tables connection</summary>
        BackgroundWriter,

        /// <summary>read cache table connection</summary>
        ReadCache,
    }
}
