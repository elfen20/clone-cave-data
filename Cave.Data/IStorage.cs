using System;

namespace Cave.Data
{
    /// <summary>
    /// Provides an interface for Database storage functions
    /// </summary>
    public interface IStorage
    {
        /// <summary>Gets or sets a value indicating whether [log verbose messages].</summary>
        /// <value><c>true</c> if [log verbose messages]; otherwise, <c>false</c>.</value>
        bool LogVerboseMessages { get; set; }

        /// <summary>Gets a value indicating whether the storage engine supports native transactions with faster execution than single commands.</summary>
        /// <value>
        /// <c>true</c> if supports native transactions; otherwise, <c>false</c>.
        /// </value>
        bool SupportsNativeTransactions { get; }

        /// <summary>
        /// Obtains the connection string used to connect to the storage engine
        /// </summary>
        ConnectionString ConnectionString { get; }

        /// <summary>
        /// Obtains whether the storage was already closed or not
        /// </summary>
        bool Closed { get; }

        /// <summary>
        /// closes the connection to the storage engine
        /// </summary>
        void Close();

        /// <summary>
        /// Checks whether the database with the specified name exists at the database or not
        /// </summary>
        /// <param name="database">The name of the database</param>
        /// <returns></returns>
        bool HasDatabase(string database);

        /// <summary>
        /// Use delayed inserts, updates and deletes
        /// </summary>
        /// <remarks>(this may result in a long delay between the command and the execution on the database.
        /// Any lookup during this time will return old values!
        /// Do only use this on write large transactions if no current data has to be retrieved.
        /// A better way to insert/update/delete large amounts of data is to use the TableWriter class!</remarks>
        bool UseDelayedWrites { get; set; }

        /// <summary>
        /// Obtains all available database names
        /// </summary>
        string[] DatabaseNames { get; }

        /// <summary>
        /// Obtains the database with the specified name
        /// </summary>
        /// <param name="database">The name of the database</param>
        /// <returns></returns>
        IDatabase GetDatabase(string database);

        /// <summary>
        /// Obtains the database with the specified name
        /// </summary>
        /// <param name="database">The name of the database</param>
        /// <param name="createIfNotExists">Create the database if its not already present</param>
        /// <returns></returns>
        IDatabase GetDatabase(string database, bool createIfNotExists);

        /// <summary>
        /// Adds a new database with the specified name
        /// </summary>
        /// <param name="database">The name of the database</param>
        /// <returns></returns>
        IDatabase CreateDatabase(string database);

        /// <summary>
        /// Removes the specified database
        /// </summary>
        /// <param name="database">The name of the database</param>
        void DeleteDatabase(string database);

        /// <summary>
        /// Obtains FieldProperties for the Database based on requested FieldProperties
        /// </summary>
        /// <param name="field"></param>
        /// <returns></returns>
        FieldProperties GetDatabaseFieldProperties(FieldProperties field);

        #region precision members
        /// <summary>
        /// Obtains the maximum <see cref="float"/> precision at the value of 1.0f of this storage engine.
        /// </summary>
        float FloatPrecision { get; }

        /// <summary>
        /// Obtains the maximum <see cref="double"/> precision at the value of 1.0d of this storage engine
        /// </summary>
        double DoublePrecision { get; }

        /// <summary>
        /// Obtains the maximum <see cref="decimal"/> value precision (absolute) for the specified field length
        /// </summary>
        /// <param name="count">The length (0 = default)</param>
        decimal GetDecimalPrecision(float count);

        /// <summary>
        /// Obtains the maximum <see cref="DateTime"/> value precision (absolute) of this storage engine
        /// </summary>
        TimeSpan DateTimePrecision { get; }

        /// <summary>
        /// Obtains the maximum <see cref="TimeSpan"/> value precision (absolute) of this storage engine
        /// </summary>
        TimeSpan TimeSpanPrecision { get; }

        /// <summary>
        /// Checks two layouts for equality using the database field type conversion and throws an error if the layouts do not match
        /// </summary>
        /// <param name="expected">The expected layout</param>
        /// <param name="current">The layout to check</param>
        void CheckLayout(RowLayout expected, RowLayout current);
        #endregion
    }
}
