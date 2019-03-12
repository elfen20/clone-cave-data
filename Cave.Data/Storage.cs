using System;
using System.Diagnostics;
using System.IO;

namespace Cave.Data
{
    /// <summary>
    /// Provides access to database storage engines.
    /// </summary>
    public abstract class Storage : IStorage
    {
        ConnectionString connectionString;
        bool closed;

        /// <summary>Creates a new storage instance with the specified <see cref="ConnectionString" />.</summary>
        /// <param name="connectionString">ConnectionString of the storage.</param>
        /// <param name="options">The options.</param>
        protected Storage(ConnectionString connectionString, DbConnectionOptions options)
        {
            this.connectionString = connectionString;
            LogVerboseMessages = options.HasFlag(DbConnectionOptions.VerboseLogging);
            if (LogVerboseMessages)
            {
                Trace.TraceInformation("Verbose logging <green>enabled!");
            }
        }

        #region abstract IStorage Member

        /// <summary>
        /// Checks whether the database with the specified name exists at the database or not.
        /// </summary>
        /// <param name="database">The name of the database.</param>
        /// <returns></returns>
        public abstract bool HasDatabase(string database);

        /// <summary>
        /// Gets all available database names.
        /// </summary>
        public abstract string[] DatabaseNames { get; }

        /// <summary>
        /// Gets the database with the specified name.
        /// </summary>
        /// <param name="database">The name of the database.</param>
        /// <returns></returns>
        public abstract IDatabase GetDatabase(string database);

        /// <summary>
        /// Adds a new database with the specified name.
        /// </summary>
        /// <param name="database">The name of the database.</param>
        /// <returns></returns>
        public abstract IDatabase CreateDatabase(string database);

        /// <summary>
        /// Removes the specified database.
        /// </summary>
        /// <param name="database">The name of the database.</param>
        public abstract void DeleteDatabase(string database);

        #endregion

        #region implemented IStorage Member

        /// <summary>
        /// Allow delayed inserts, updates and deletes.
        /// </summary>
        public bool UseDelayedWrites { get; set; }

        /// <summary>
        /// Gets FieldProperties for the Database based on requested FieldProperties.
        /// </summary>
        /// <param name="field"></param>
        /// <returns></returns>
        public virtual FieldProperties GetDatabaseFieldProperties(FieldProperties field)
        {
            return field;
        }

        /// <summary>
        /// Gets/sets the <see cref="ConnectionString"/> used to connect to the database server.
        /// </summary>
        public virtual ConnectionString ConnectionString => connectionString;

        /// <summary>
        /// Gets wether the storage was already closed or not.
        /// </summary>
        public virtual bool Closed => closed;

        /// <summary>
        /// closes the connection to the storage engine.
        /// </summary>
        public virtual void Close()
        {
            closed = true;
        }

        /// <summary>
        /// Gets the database with the specified name.
        /// </summary>
        /// <param name="database">The name of the database.</param>
        /// <param name="createIfNotExists">Create the database if its not already present.</param>
        /// <returns></returns>
        public virtual IDatabase GetDatabase(string database, bool createIfNotExists)
        {
            if (HasDatabase(database))
            {
                return GetDatabase(database);
            }

            if (createIfNotExists)
            {
                return CreateDatabase(database);
            }

            throw new ArgumentException(string.Format("The requested database '{0}' was not found!", database));
        }

        /// <summary>
        /// Checks two layouts for equality using the database field type conversion and throws an error if the layouts do not match.
        /// </summary>
        /// <param name="expected">The expected layout.</param>
        /// <param name="current">The layout to check.</param>
        public virtual void CheckLayout(RowLayout expected, RowLayout current)
        {
            if (expected == null)
            {
                throw new ArgumentNullException(nameof(expected));
            }

            if (current == null)
            {
                throw new ArgumentNullException(nameof(current));
            }

            if (expected.FieldCount != current.FieldCount)
            {
                throw new InvalidDataException(string.Format("Fieldcount of table {0} differs (found {1} expected {2})!", current.Name, current.FieldCount, expected.FieldCount));
            }
            for (var i = 0; i < expected.FieldCount; i++)
            {
                FieldProperties expectedField = GetDatabaseFieldProperties(expected.GetProperties(i));
                FieldProperties currentField = GetDatabaseFieldProperties(current.GetProperties(i));
                if (!expectedField.Equals(currentField))
                {
                    throw new InvalidDataException(string.Format("Fieldproperties of table {0} differ! (found {1} expected {2})!", current.Name, currentField, expectedField));
                }
            }
        }

        #endregion

        #region precision members

        /// <summary>
        /// Gets the maximum <see cref="float"/> precision at the value of 1.0f of this storage engine.
        /// </summary>
        public virtual float FloatPrecision => 0;

        /// <summary>
        /// Gets the maximum <see cref="double"/> precision at the value of 1.0d of this storage engine.
        /// </summary>
        public virtual double DoublePrecision => 0;

        /// <summary>
        /// Gets the maximum <see cref="DateTime"/> value precision of this storage engine.
        /// </summary>
        public virtual TimeSpan DateTimePrecision => TimeSpan.FromMilliseconds(0);

        /// <summary>
        /// Gets the maximum <see cref="TimeSpan"/> value precision of this storage engine.
        /// </summary>
        public virtual TimeSpan TimeSpanPrecision => new TimeSpan(0);

        /// <summary>
        /// Gets the maximum <see cref="decimal"/> value precision of this storage engine.
        /// </summary>
        public virtual decimal GetDecimalPrecision(float count)
        {
            if (count == 0)
            {
                return 0;
            }

            var l_PreDecimal = Math.Truncate(count);
            var l_Decimal = (int)Math.Round((count - l_PreDecimal) * 100);
            decimal result = 1;
            while (l_Decimal-- > 0)
            {
                result *= 0.1m;
            }
            return result;
        }
        #endregion

        /// <summary>Gets or sets a value indicating whether [log verbose messages].</summary>
        /// <value><c>true</c> if [log verbose messages]; otherwise, <c>false</c>.</value>
        public bool LogVerboseMessages { get; set; }

        /// <summary>Gets the name of the log source.</summary>
        /// <value>The name of the log source.</value>
        public string LogSourceName => connectionString.ToString(ConnectionStringPart.NoCredentials);

        /// <summary>
        /// Gets a value indicating whether the storage engine supports native transactions with faster execution than single commands.
        /// </summary>
        /// <value>
        /// <c>true</c> if supports native transactions; otherwise, <c>false</c>.
        /// </value>
        public abstract bool SupportsNativeTransactions { get; }
    }
}
