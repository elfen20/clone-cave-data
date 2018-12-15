namespace Cave.Data
{
    /// <summary>
    /// Provides a generic table connector interface
    /// </summary>
    public interface ITableConnector
    {
        /// <summary>Gets the database connection used.</summary>
        /// <value>The database.</value>
        IDatabase Database { get; }

        /// <summary>Gets all connected tables of this instance.</summary>
        /// <value>The tables.</value>
        ITable[] Tables { get; }

        /// <summary>Gets the current mode of the connector.</summary>
        /// <value>The <see cref="TableConnectorMode"/>.</value>
        TableConnectorMode Mode { get; }

        /// <summary>Connects to the specified database using the specified <see cref="TableConnectorMode"/>.</summary>
        /// <param name="mode">The <see cref="TableConnectorMode"/></param>
        /// <param name="database">The database.</param>
        void Connect(TableConnectorMode mode, IDatabase database);

        /// <summary>Closes all tables, the database and the storage engine.</summary>
        void Close(bool closeStorage);
    }
}
