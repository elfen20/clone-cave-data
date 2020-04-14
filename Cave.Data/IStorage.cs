using System;

namespace Cave.Data
{
    /// <summary>
    /// Provides an interface for Database storage functions.
    /// </summary>
    public interface IStorage
    {
        /// <summary>Gets or sets a value indicating whether [log verbose messages].</summary>
        /// <value><c>true</c> if [log verbose messages]; otherwise, <c>false</c>.</value>
        bool LogVerboseMessages { get; set; }

        /// <summary>Gets a value indicating whether [unsafe connections are allowed].</summary>
        /// <value>
        /// <c>true</c> if [unsafe connections are allowed]; otherwise, <c>false</c>.
        /// </value>
        bool AllowUnsafeConnections { get; }

        /// <summary>Gets a value indicating whether the storage engine supports native transactions with faster execution than single commands.</summary>
        /// <value>
        /// <c>true</c> if supports native transactions; otherwise, <c>false</c>.
        /// </value>
        bool SupportsNativeTransactions { get; }

        /// <summary>
        /// Gets the date time format for big int date time values.
        /// </summary>
        string BigIntDateTimeFormat { get; }

        /// <summary>
        /// Gets or sets number of rows per chunk on big data operations.
        /// </summary>
        int TransactionRowCount { get; set; }

        /// <summary>
        /// Gets the connection string used to connect to the storage engine.
        /// </summary>
        ConnectionString ConnectionString { get; }

        /// <summary>
        /// Gets a value indicating whether the storage was already closed or not.
        /// </summary>
        bool Closed { get; }

        /// <summary>
        /// Gets all available database names.
        /// </summary>
        string[] DatabaseNames { get; }

        /// <summary>
        /// Gets the maximum <see cref="float"/> precision at the value of 1.0f of this storage engine.
        /// </summary>
        float FloatPrecision { get; }

        /// <summary>
        /// Gets the maximum <see cref="double"/> precision at the value of 1.0d of this storage engine.
        /// </summary>
        double DoublePrecision { get; }

        /// <summary>
        /// Gets the maximum <see cref="DateTime"/> value precision (absolute) of this storage engine.
        /// </summary>
        TimeSpan DateTimePrecision { get; }

        /// <summary>
        /// Gets the maximum <see cref="TimeSpan"/> value precision (absolute) of this storage engine.
        /// </summary>
        TimeSpan TimeSpanPrecision { get; }

        /// <summary>
        /// Gets the database with the specified name.
        /// </summary>
        /// <param name="database">The name of the database.</param>
        /// <returns>A new <see cref="IDatabase"/> instance for the requested database.</returns>
        IDatabase this[string database] { get; }

        /// <summary>
        /// closes the connection to the storage engine.
        /// </summary>
        void Close();

        /// <summary>
        /// Checks whether the database with the specified name exists at the database or not.
        /// </summary>
        /// <param name="database">The name of the database.</param>
        /// <returns>True if the database exists, false otherwise.</returns>
        bool HasDatabase(string database);

        /// <summary>
        /// Gets the database with the specified name.
        /// </summary>
        /// <param name="database">The name of the database.</param>
        /// <returns>A new <see cref="IDatabase"/> instance for the requested database.</returns>
        IDatabase GetDatabase(string database);

        /// <summary>
        /// Gets the database with the specified name.
        /// </summary>
        /// <param name="database">The name of the database.</param>
        /// <param name="createIfNotExists">Create the database if its not already present.</param>
        /// <returns>A new <see cref="IDatabase"/> instance for the requested database.</returns>
        IDatabase GetDatabase(string database, bool createIfNotExists);

        /// <summary>
        /// Adds a new database with the specified name.
        /// </summary>
        /// <param name="database">The name of the database.</param>
        /// <returns>A new <see cref="IDatabase"/> instance for the created database.</returns>
        IDatabase CreateDatabase(string database);

        /// <summary>
        /// Removes the specified database.
        /// </summary>
        /// <param name="database">The name of the database.</param>
        void DeleteDatabase(string database);

        /// <summary>
        /// Gets converted field properties for this storage instance based on requested field properties.
        /// </summary>
        /// <param name="field">The field properties to convert.</param>
        /// <returns>A new <see cref="FieldProperties"/> instance.</returns>
        IFieldProperties GetDatabaseFieldProperties(IFieldProperties field);

        /// <summary>
        /// Gets the maximum <see cref="decimal"/> value precision (absolute) for the specified field length.
        /// </summary>
        /// <param name="count">The length (0 = default).</param>
        /// <returns>The precision at the database.</returns>
        decimal GetDecimalPrecision(float count);

        /// <summary>
        /// Checks two layouts for equality using the database field type conversion and throws an error if the layouts do not match.
        /// </summary>
        /// <param name="expected">The expected layout.</param>
        /// <param name="current">The layout to check.</param>
        void CheckLayout(RowLayout expected, RowLayout current);
    }
}
