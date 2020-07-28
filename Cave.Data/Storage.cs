using System;
using System.Diagnostics;
using System.IO;

namespace Cave.Data
{
    /// <summary>Provides access to database storage engines.</summary>
    public abstract class Storage : IStorage
    {
        /// <summary>Epoch DateTime in Ticks.</summary>
        public const long EpochTicks = 621355968000000000;

        bool closed;

        /// <summary>Initializes a new instance of the <see cref="Storage" /> class.</summary>
        /// <param name="connectionString">ConnectionString of the storage.</param>
        /// <param name="flags">The connection flags.</param>
        protected Storage(ConnectionString connectionString, ConnectionFlags flags)
        {
            ConnectionString = connectionString;
            AllowUnsafeConnections = flags.HasFlag(ConnectionFlags.AllowUnsafeConnections);
            LogVerboseMessages = flags.HasFlag(ConnectionFlags.VerboseLogging);
            if (LogVerboseMessages)
            {
                Trace.TraceInformation("Verbose logging <green>enabled!");
            }
        }

        /// <summary>Gets or sets the date time format for big int date time values.</summary>
        public static string BigIntDateTimeFormat { get; set; } = "yyyyMMddHHmmssfff";

        #region IStorage Member

        #region properties

        /// <inheritdoc />
        public abstract string[] DatabaseNames { get; }

        /// <inheritdoc />
        public virtual ConnectionString ConnectionString { get; }

        /// <inheritdoc />
        public virtual bool Closed => closed;

        /// <inheritdoc />
        public virtual float FloatPrecision => 0;

        /// <inheritdoc />
        public virtual double DoublePrecision => 0;

        /// <inheritdoc />
        public virtual TimeSpan DateTimePrecision => TimeSpan.FromMilliseconds(0);

        /// <inheritdoc />
        public virtual TimeSpan TimeSpanPrecision => new TimeSpan(0);

        /// <inheritdoc />
        public bool LogVerboseMessages { get; set; }

        /// <inheritdoc />
        public bool AllowUnsafeConnections { get; }

        /// <inheritdoc />
        public abstract bool SupportsNativeTransactions { get; }

        /// <inheritdoc />
        public int TransactionRowCount { get; set; } = 5000;

        /// <inheritdoc />
        string IStorage.BigIntDateTimeFormat => BigIntDateTimeFormat;

        /// <inheritdoc />
        public IDatabase this[string databaseName] => GetDatabase(databaseName);

        #endregion

        #region functions

        /// <inheritdoc />
        public virtual IFieldProperties GetDatabaseFieldProperties(IFieldProperties field) => field;

        /// <inheritdoc />
        public virtual void Close() => closed = true;

        /// <inheritdoc />
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

            throw new ArgumentException($"The requested database '{database}' was not found!");
        }

        /// <inheritdoc />
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
                throw new InvalidDataException(
                    $"Field.Count of table {current.Name} != {expected.Name} differs (found {current.FieldCount} expected {expected.FieldCount})!");
            }

            for (var i = 0; i < expected.FieldCount; i++)
            {
                var expectedField = GetDatabaseFieldProperties(expected[i]);
                var currentField = GetDatabaseFieldProperties(current[i]);
                if (!expectedField.Equals(currentField))
                {
                    throw new Exception($"FieldProperties of table {current.Name} != {expected.Name} differ! (found {currentField} expected {expectedField})!");
                }

                if (currentField.Flags != expectedField.Flags)
                {
                    Trace.TraceWarning($"Field.Flags of table {current.Name} != {expected.Name} differ! (found {currentField} expected {expectedField})!");
                }
            }
        }

        /// <inheritdoc />
        public abstract bool HasDatabase(string database);

        /// <inheritdoc />
        public abstract IDatabase GetDatabase(string database);

        /// <inheritdoc />
        public abstract IDatabase CreateDatabase(string database);

        /// <inheritdoc />
        public abstract void DeleteDatabase(string database);

        /// <inheritdoc />
        public virtual decimal GetDecimalPrecision(float count)
        {
            if (count == 0)
            {
                return 0;
            }

            var value = Math.Truncate(count);
            var decimalValue = (int) Math.Round((count - value) * 100);
            decimal result = 1;
            while (decimalValue-- > 0)
            {
                result *= 0.1m;
            }

            return result;
        }

        /// <inheritdoc />
        public override string ToString() => ConnectionString.ToString(ConnectionStringPart.NoCredentials);

        #endregion

        #endregion
    }
}
