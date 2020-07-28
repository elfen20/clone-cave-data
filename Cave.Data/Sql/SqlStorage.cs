using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;

namespace Cave.Data.Sql
{
    /// <summary>Provides a base class for sql 92 <see cref="IStorage" /> implementations.</summary>
    public abstract class SqlStorage : Storage, IDisposable
    {
        /// <summary>Gets or sets the native date time format.</summary>
        /// <value>The native date time format.</value>
        public string NativeDateTimeFormat = "yyyy'-'MM'-'dd' 'HH':'mm':'ss";

        SqlConnectionPool pool;
        bool disposedValue;

        #region constructors

#pragma warning disable CA2214 // Constructor calls virtual method to retrieve required database connection class

        /// <summary>Initializes a new instance of the <see cref="SqlStorage" /> class.</summary>
        /// <param name="connectionString">the connection details.</param>
        /// <param name="flags">The connection flags.</param>
        protected SqlStorage(ConnectionString connectionString, ConnectionFlags flags = ConnectionFlags.None)
            : base(connectionString, flags)
        {
            Trace.TraceInformation("Initializing native interop assemblies.");
            using (var dbConnection = GetDbConnectionType())
            using (var cmd = dbConnection.CreateCommand())
            {
                DbConnectionType = dbConnection.GetType();
            }

            pool = new SqlConnectionPool(this);
            WarnUnsafe();
        }

#pragma warning restore CA2214

        #endregion

        #region public properties

        /// <summary>
        ///     Gets a value indicating whether the storage engine supports native transactions with faster execution than
        ///     single commands.
        /// </summary>
        /// <value><c>true</c> if supports native transactions; otherwise, <c>false</c>.</value>
        public override bool SupportsNativeTransactions { get; } = true;

        /// <summary>
        ///     Gets or sets a value indicating whether a result schema check on each query is done. (This impacts performance
        ///     very badly if you query large amounts of single rows). A common practice is to use this while developing the
        ///     application and unittest, running the unittests and set this to false on release builds.
        /// </summary>
#if DEBUG
        public bool DoSchemaCheckOnQuery { get; set; } = Debugger.IsAttached;
#else
        public bool DoSchemaCheckOnQuery { get; set; }
#endif

        /// <summary>Gets or sets the command(s) to be run for each newly created connection.</summary>
        /// <remarks>After changing this you can use <see cref="ClearCachedConnections()" /> to force reconnecting.</remarks>
        public SqlCmd NewConnectionCommand { get; set; }

        /// <summary>
        ///     Gets or sets a value indicating whether we throw an <see cref="InvalidDataException" /> on date time field
        ///     conversion errors.
        /// </summary>
        public bool ThrowDateTimeFieldExceptions { get; set; }

        /// <summary>Gets or sets the maximum error retries.</summary>
        /// <remarks>
        ///     If set to &lt; 1 only a single try is made to execute a query. If set to any number &gt; 0 this values
        ///     indicates the number of retries that are made after the first try and subsequent tries fail.
        /// </remarks>
        /// <value>The maximum error retries.</value>
        public int MaxErrorRetries { get; set; } = 3;

        /// <summary>Gets or sets the connection close timeout.</summary>
        /// <value>The connection close timeout.</value>
        public TimeSpan ConnectionCloseTimeout { get => pool.ConnectionCloseTimeout; set => pool.ConnectionCloseTimeout = value; }

        /// <summary>Gets a value indicating whether the connection supports named parameters or not.</summary>
        public abstract bool SupportsNamedParameters { get; }

        /// <summary>Gets a value indicating whether the connection supports select * groupby.</summary>
        public abstract bool SupportsAllFieldsGroupBy { get; }

        /// <summary>Gets the parameter prefix for this storage.</summary>
        public abstract string ParameterPrefix { get; }

        /// <summary>Gets or sets command timeout for all sql commands.</summary>
        public TimeSpan CommandTimeout { get; set; } = TimeSpan.FromMinutes(1);

        #endregion

        #region protected assembly interface properties

        /// <summary>
        ///     Gets a value indicating whether the db connections can change the database with the Sql92 "USE Database"
        ///     command.
        /// </summary>
        protected internal abstract bool DBConnectionCanChangeDataBase { get; }

        /// <summary>Gets the <see cref="IDbConnection" /> type.</summary>
        protected internal Type DbConnectionType { get; }

        #endregion

        /// <summary>closes the connection to the storage engine.</summary>
        public override void Close() { Dispose(); }

        /// <summary>Releases all resources used by the SqlConnection.</summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        #region public database specific conversions

        /// <summary>Escapes a field name for direct use in a query.</summary>
        /// <param name="field">The field.</param>
        /// <returns>The escaped field.</returns>
        public abstract string EscapeFieldName(IFieldProperties field);

        /// <summary>Escapes a string value for direct use in a query (whenever possible use parameters instead!).</summary>
        /// <param name="text">Text to escape.</param>
        /// <returns>The escaped text.</returns>
        public virtual string EscapeString(string text)
        {
            if (text == null)
            {
                throw new ArgumentNullException(nameof(text));
            }

            // escape escape char
            if (text.IndexOf('\\') != -1)
            {
                text = text.Replace("\\", "\\\\");
            }

            // escape invalid chars
            if (text.IndexOf('\0') != -1)
            {
                text = text.Replace("\0", "\\0");
            }

            if (text.IndexOf('\'') != -1)
            {
                text = text.Replace("'", "\\'");
            }

            if (text.IndexOf('"') != -1)
            {
                text = text.Replace("\"", "\\\"");
            }

            if (text.IndexOf('\b') != -1)
            {
                text = text.Replace("\b", "\\b");
            }

            if (text.IndexOf('\n') != -1)
            {
                text = text.Replace("\n", "\\n");
            }

            if (text.IndexOf('\r') != -1)
            {
                text = text.Replace("\r", "\\r");
            }

            if (text.IndexOf('\t') != -1)
            {
                text = text.Replace("\t", "\\t");
            }

            return "'" + text + "'";
        }

        /// <summary>Escapes the given binary data.</summary>
        /// <param name="data">The data.</param>
        /// <returns>The escaped binary data.</returns>
        public virtual string EscapeBinary(byte[] data) => "X'" + data.ToHexString() + "'";

        /// <summary>Escapes a field value for direct use in a query (whenever possible use parameters instead!).</summary>
        /// <param name="properties">FieldProperties.</param>
        /// <param name="value">Value to escape.</param>
        /// <returns>The escaped field value.</returns>
        public virtual string EscapeFieldValue(IFieldProperties properties, object value)
        {
            if (properties == null) { throw new ArgumentNullException(nameof(properties)); }

            switch (value)
            {
                case null:
                    return "NULL";
                case byte[] bytes:
                    return EscapeBinary(bytes);
                case byte _:
                case sbyte _:
                case ushort _:
                case short _:
                case uint _:
                case int _:
                case long _:
                case ulong _:
                case decimal _:
                    return value.ToString();
                case double d:
                    return d.ToString("R", CultureInfo.InvariantCulture);
                case float f:
                    return f.ToString("R", CultureInfo.InvariantCulture);
                case bool b:
                    return b ? "1" : "0";
                case TimeSpan timeSpan:
                    return timeSpan.TotalSeconds.ToString("R", CultureInfo.InvariantCulture);
                case DateTime dateTime:
                {
                    DateTime dt = properties.DateTimeKind switch
                    {
                        DateTimeKind.Unspecified => dateTime,
                        DateTimeKind.Utc => dateTime.ToUniversalTime(),
                        DateTimeKind.Local => dateTime.ToLocalTime(),
                        _ => throw new NotSupportedException($"DateTimeKind {properties.DateTimeKind} not supported!")
                    };
                    switch (properties.DateTimeType)
                    {
                        case DateTimeType.Undefined:
                        case DateTimeType.Native: return $"'{dt:NativeDateTimeFormat}'";
                        case DateTimeType.BigIntHumanReadable: return $"{dt:BigIntDateTimeFormat}";
                        case DateTimeType.BigIntTicks: return $"{dt.Ticks}";
                        case DateTimeType.DecimalSeconds: return $"{(dt.Ticks / (decimal) TimeSpan.TicksPerSecond)}";
                        case DateTimeType.DoubleSeconds: return $"{(dt.Ticks / (double) TimeSpan.TicksPerSecond)}";
                        case DateTimeType.DoubleEpoch: return $"{((dt.Ticks - EpochTicks) / (double) TimeSpan.TicksPerSecond)}";
                        default: throw new NotImplementedException();
                    }
                }
                default:
                    return value.GetType().IsEnum ? $"{Convert.ToInt64(value, CultureInfo.InvariantCulture)}" : EscapeString(value.ToString());
            }
        }

        /// <summary>Obtains the local <see cref="DataType" /> for the specified database fieldtype.</summary>
        /// <param name="fieldType">The field type at the database.</param>
        /// <param name="fieldSize">The field size at the database.</param>
        /// <returns>The local csharp datatype.</returns>
        public virtual DataType GetLocalDataType(Type fieldType, uint fieldSize) => RowLayout.DataTypeFromType(fieldType);

        /// <summary>Converts a local value into a database value.</summary>
        /// <param name="field">The <see cref="FieldProperties" /> of the affected field.</param>
        /// <param name="localValue">The local value to be encoded for the database.</param>
        /// <returns>The value as database value type.</returns>
        public virtual object GetDatabaseValue(IFieldProperties field, object localValue)
        {
            try
            {
                if (field == null)
                {
                    throw new ArgumentNullException(nameof(field));
                }

                if (localValue == null)
                {
                    return null;
                }

                switch (field.DataType)
                {
                    case DataType.Enum:
                        return Convert.ToInt64(localValue);
                    case DataType.User:
                        return localValue.ToString();
                    case DataType.TimeSpan:
                    {
                        var value = (TimeSpan) localValue;
                        switch (field.DateTimeType)
                        {
                            case DateTimeType.Undefined:
                            case DateTimeType.Native: return value;
                            case DateTimeType.BigIntHumanReadable: return long.Parse(new DateTime(value.Ticks).ToString(BigIntDateTimeFormat));
                            case DateTimeType.BigIntTicks: return value.Ticks;
                            case DateTimeType.DecimalSeconds: return (decimal) value.Ticks / TimeSpan.TicksPerSecond;
                            case DateTimeType.DoubleSeconds: return (double) value.Ticks / TimeSpan.TicksPerSecond;
                            default: throw new NotImplementedException($"DateTimeType {field.DateTimeType} not implemented!");
                        }
                    }
                    case DataType.DateTime:
                    {
                        if ((DateTime) localValue == default)
                        {
                            return null;
                        }

                        var value = (DateTime) localValue;
                        switch (field.DateTimeKind)
                        {
                            case DateTimeKind.Unspecified: break;
                            case DateTimeKind.Local:
                                if (value.Kind == DateTimeKind.Utc)
                                {
                                    value = value.ToLocalTime();
                                }
                                else
                                {
                                    value = new DateTime(value.Ticks, DateTimeKind.Local);
                                }

                                break;
                            case DateTimeKind.Utc:
                                if (value.Kind == DateTimeKind.Local)
                                {
                                    value = value.ToUniversalTime();
                                }
                                else
                                {
                                    value = new DateTime(value.Ticks, DateTimeKind.Utc);
                                }

                                break;
                            default:
                                throw new NotSupportedException($"DateTimeKind {field.DateTimeKind} not supported!");
                        }

                        switch (field.DateTimeType)
                        {
                            case DateTimeType.Undefined:
                            case DateTimeType.Native: return value;
                            case DateTimeType.BigIntHumanReadable: return long.Parse(value.ToString(BigIntDateTimeFormat));
                            case DateTimeType.BigIntTicks: return value.Ticks;
                            case DateTimeType.DecimalSeconds: return value.Ticks / (decimal) TimeSpan.TicksPerSecond;
                            case DateTimeType.DoubleSeconds: return value.Ticks / (double) TimeSpan.TicksPerSecond;
                            case DateTimeType.DoubleEpoch: return (value.Ticks - EpochTicks) / (double) TimeSpan.TicksPerSecond;
                            default: throw new NotImplementedException();
                        }
                    }
                }

                return localValue;
            }
            catch
            {
                throw new ArgumentException($"Invalid value at field {field}!");
            }
        }

        /// <summary>
        ///     Gets or sets the default <see cref="DateTimeKind" /> used when reading date fields without explicit
        ///     definition.
        /// </summary>
        public DateTimeKind DefaultDateTimeKind { get; set; } = DateTimeKind.Local;

        /// <summary>Converts a database value into a local value.</summary>
        /// <param name="field">The <see cref="FieldProperties" /> of the affected field.</param>
        /// <param name="reader">The reader to read values from.</param>
        /// <param name="databaseValue">The value at the database.</param>
        /// <returns>Returns a value as local csharp value type.</returns>
        public virtual object GetLocalValue(IFieldProperties field, IDataReader reader, object databaseValue)
        {
            if (databaseValue is DBNull || databaseValue is null)
            {
                return null;
            }

            switch (field.DataType)
            {
                case DataType.Double: return (double) databaseValue;
                case DataType.Single: return (float) databaseValue;
                case DataType.User:
                {
                    return field.ParseValue((string) databaseValue, null, CultureInfo.InvariantCulture);
                }
                case DataType.Enum:
                {
                    return Enum.ToObject(field.ValueType, reader.GetInt64(field.Index));
                }
                case DataType.DateTime:
                {
                    long ticks = 0;
                    switch (field.DateTimeType)
                    {
                        default: throw new NotSupportedException($"DateTimeType {field.DateTimeType} is not supported");
                        case DateTimeType.BigIntHumanReadable:
                        {
                            var text = ((long) databaseValue).ToString();
                            ticks = DateTime.ParseExact(text, BigIntDateTimeFormat, CultureInfo.InvariantCulture).Ticks;
                            break;
                        }
                        case DateTimeType.Undefined:
                        case DateTimeType.Native:
                            try
                            {
                                if (databaseValue is DateTime dt)
                                {
                                    ticks = dt.Ticks;
                                }
                                else
                                {
                                    ticks = reader.GetDateTime(field.Index).Ticks;
                                }
                            }
                            catch (Exception ex)
                            {
                                var msg = $"Invalid datetime value {reader.GetValue(field.Index)} at {field}.";
                                Trace.WriteLine(msg);
                                if (ThrowDateTimeFieldExceptions)
                                {
                                    throw new InvalidDataException(msg, ex);
                                }
                            }

                            break;
                        case DateTimeType.BigIntTicks:
                            ticks = (long) databaseValue;
                            break;
                        case DateTimeType.DecimalSeconds:
                            ticks = (long) decimal.Round((decimal) databaseValue * TimeSpan.TicksPerSecond);
                            break;
                        case DateTimeType.DoubleSeconds:
                            ticks = (long) Math.Round((double) databaseValue * TimeSpan.TicksPerSecond);
                            break;
                        case DateTimeType.DoubleEpoch:
                            ticks = (long) Math.Round(((double) databaseValue * TimeSpan.TicksPerSecond) + EpochTicks);
                            break;
                    }

                    var kind = field.DateTimeKind != 0 ? field.DateTimeKind : DefaultDateTimeKind;
                    return new DateTime(ticks, kind);
                }
                case DataType.TimeSpan:
                {
                    long ticks = 0;
                    switch (field.DateTimeType)
                    {
                        default: throw new NotSupportedException($"DateTimeType {field.DateTimeType} is not supported");
                        case DateTimeType.BigIntHumanReadable:
                        {
                            var text = ((long) databaseValue).ToString();
                            ticks = DateTime.ParseExact(text, BigIntDateTimeFormat, CultureInfo.InvariantCulture).Ticks;
                            break;
                        }
                        case DateTimeType.Undefined:
                        case DateTimeType.Native:
                            ticks = ((TimeSpan) Convert.ChangeType(databaseValue, typeof(TimeSpan), CultureInfo.InvariantCulture)).Ticks;
                            break;
                        case DateTimeType.BigIntTicks:
                            ticks = (long) databaseValue;
                            break;
                        case DateTimeType.DecimalSeconds:
                            ticks = (long) decimal.Round((decimal) databaseValue * TimeSpan.TicksPerSecond);
                            break;
                        case DateTimeType.DoubleSeconds:
                            ticks = (long) Math.Round((double) databaseValue * TimeSpan.TicksPerSecond);
                            break;
                        case DateTimeType.DoubleEpoch:
                            ticks = (long) Math.Round(((double) databaseValue * TimeSpan.TicksPerSecond) + EpochTicks);
                            break;
                    }

                    return new TimeSpan(ticks);
                }
                case DataType.Int8: return (sbyte) databaseValue;
                case DataType.Int16: return (short) databaseValue;
                case DataType.Int32: return (int) databaseValue;
                case DataType.Int64: return (long) databaseValue;
                case DataType.UInt8: return (byte) databaseValue;
                case DataType.UInt16: return (ushort) databaseValue;
                case DataType.UInt32: return (uint) databaseValue;
                case DataType.UInt64: return (ulong) databaseValue;
                case DataType.String: return (string) databaseValue;
                case DataType.Binary: return (byte[]) reader.GetValue(field.Index);
                case DataType.Bool: return (bool) databaseValue;
                case DataType.Char: return (char) databaseValue;
                case DataType.Decimal: return (decimal) databaseValue;
            }

            // fallback
            {
                return databaseValue;
            }
        }

        #endregion

        #region public functions

        /// <summary>Gets a full qualified table name.</summary>
        /// <param name="database">A database name.</param>
        /// <param name="table">A table name.</param>
        /// <returns>The full qualified table name.</returns>
        public abstract string FQTN(string database, string table);

        /// <summary>Closes and clears all cached connections.</summary>
        /// <exception cref="ObjectDisposedException">SqlConnection.</exception>
        public void ClearCachedConnections()
        {
            if (pool == null)
            {
                throw new ObjectDisposedException("SqlConnection");
            }

            pool.Clear();
        }

        /// <summary>Retrieves a connection (from the cache) or creates a new one if needed.</summary>
        /// <param name="databaseName">Name of the database.</param>
        /// <returns>A connection for the specified database.</returns>
        public SqlConnection GetConnection(string databaseName) => pool.GetConnection(databaseName);

        /// <summary>Returns a connection to the connection pool for reuse.</summary>
        /// <param name="connection">The connection to return to the queue.</param>
        /// <param name="close">Force close of the connection.</param>
        public void ReturnConnection(ref SqlConnection connection, bool close)
        {
            if (connection == null)
            {
                return;
            }

            pool.ReturnConnection(ref connection, close);
        }

        /// <summary>Creates a new database connection.</summary>
        /// <param name="databaseName">Name of the database.</param>
        /// <returns>A new <see cref="IDbConnection" /> instance.</returns>
        public virtual IDbConnection CreateNewConnection(string databaseName)
        {
            var connection = (IDbConnection) Activator.CreateInstance(DbConnectionType);
            connection.ConnectionString = GetConnectionString(databaseName);
            connection.Open();
            WarnUnsafe();
            if (NewConnectionCommand != null)
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = NewConnectionCommand;
                    command.ExecuteNonQuery();
                }
            }

            if (DBConnectionCanChangeDataBase)
            {
                if ((databaseName != null) && (connection.Database != databaseName))
                {
                    connection.ChangeDatabase(databaseName);
                }
            }

            return connection;
        }

        /// <summary>Gets FieldProperties for the Database based on requested FieldProperties.</summary>
        /// <param name="field">The field.</param>
        /// <returns>A new <see cref="FieldProperties" /> instance.</returns>
        public override IFieldProperties GetDatabaseFieldProperties(IFieldProperties field)
        {
            if (field == null)
            {
                throw new ArgumentNullException(nameof(field));
            }

            // check if datatype is replacement for missing sql type
            switch (field.DataType)
            {
                case DataType.Enum:
                {
                    var result = field.Clone();
                    result.TypeAtDatabase = DataType.Int64;
                    return result;
                }
                case DataType.User:
                {
                    var result = field.Clone();
                    result.TypeAtDatabase = DataType.String;
                    return result;
                }
                case DataType.DateTime:
                case DataType.TimeSpan:
                    switch (field.DateTimeType)
                    {
                        case DateTimeType.Undefined:
                        case DateTimeType.Native:
                        {
                            return field;
                        }
                        case DateTimeType.BigIntHumanReadable:
                        case DateTimeType.BigIntTicks:
                        {
                            var result = field.Clone();
                            result.TypeAtDatabase = DataType.Int64;
                            return result;
                        }
                        case DateTimeType.DecimalSeconds:
                        {
                            var result = field.Clone();
                            result.TypeAtDatabase = DataType.Decimal;
                            result.MaximumLength = 65.3f;
                            return result;
                        }
                        case DateTimeType.DoubleSeconds:
                        {
                            var result = field.Clone();
                            result.TypeAtDatabase = DataType.Double;
                            return result;
                        }
                        default: throw new NotImplementedException();
                    }
            }

            return field;
        }

        #endregion

        #region public Query/Execute functions

        #region public Execute(SqlCmd cmd, ...)

        /// <summary>Executes a database dependent sql statement silently.</summary>
        /// <param name="cmd">the database dependent sql statement.</param>
        /// <param name="database">The affected database (optional, used to get a cached connection).</param>
        /// <param name="table">The affected table (optional, used to get a cached connection).</param>
        /// <returns>Number of affected rows (if supported by the database).</returns>
        public virtual int Execute(SqlCmd cmd, string database = null, string table = null)
        {
            if (Closed)
            {
                throw new ObjectDisposedException(ToString());
            }

            for (var i = 1;; i++)
            {
                var connection = GetConnection(database);
                var error = false;
                try
                {
                    using (var command = CreateCommand(connection, cmd))
                    {
                        return command.ExecuteNonQuery();
                    }
                }
                catch (Exception ex)
                {
                    error = true;
                    if ((connection.State == ConnectionState.Open) || (i > MaxErrorRetries))
                    {
                        throw;
                    }

                    Trace.TraceError("<red>{3}<default> Error during Execute(<cyan>{0}<default>, <cyan>{1}<default>) -> <yellow>retry {2}\n{4}", database,
                        table, i, ex.Message, ex);
                }
                finally
                {
                    ReturnConnection(ref connection, error);
                }
            }
        }

        #endregion

        #region QuerySchema()

        /// <summary>Gets the <see cref="RowLayout" /> of the specified database table.</summary>
        /// <param name="database">The affected database (dependent on the storage engine this may be null).</param>
        /// <param name="table">The affected table (dependent on the storage engine this may be null).</param>
        /// <returns>A layout for the specified table.</returns>
        public virtual RowLayout QuerySchema(string database, string table)
        {
            if (database == null)
            {
                throw new ArgumentNullException(nameof(database));
            }

            if (table == null)
            {
                throw new ArgumentNullException(nameof(table));
            }

            if (Closed)
            {
                throw new ObjectDisposedException(ToString());
            }

            for (var i = 1;; i++)
            {
                var connection = GetConnection(database);
                var error = false;
                try
                {
                    var fqtn = FQTN(database, table);
                    using (var cmd = CreateCommand(connection, $"SELECT * FROM {fqtn} WHERE FALSE"))
                    using (var reader = cmd.ExecuteReader(CommandBehavior.KeyInfo))
                    {
                        return ReadSchema(reader, table);
                    }
                }
                catch (Exception ex)
                {
                    error = true;
                    if (i > MaxErrorRetries)
                    {
                        throw;
                    }

                    Trace.TraceError("<red>{3}<default> Error during QuerySchema(<cyan>{0}<default>, <cyan>{1}<default>) -> <yellow>retry {2}\n{4}", database,
                        table, i, ex.Message, ex);
                }
                finally
                {
                    ReturnConnection(ref connection, error);
                }
            }
        }

        #endregion

        #region QueryValue(SqlCmd cmd, ...

        /// <summary>Querys a single value with a database dependent sql statement.</summary>
        /// <param name="cmd">The database dependent sql statement.</param>
        /// <param name="value">The value read from database.</param>
        /// <param name="database">The affected database (optional, used with cached connections).</param>
        /// <param name="table">The affected table (optional, used with cached connections).</param>
        /// <param name="fieldName">Name of the field (optional, only needed if multiple columns are returned).</param>
        /// <returns>The result value or null.</returns>
        /// <typeparam name="TValue">Result type.</typeparam>
        public bool QueryValue<TValue>(SqlCmd cmd, out TValue value, string database = null, string table = null, string fieldName = null)
            where TValue : struct
        {
            var result = QueryValue(cmd, database, table, fieldName);
            if (result == null)
            {
                value = default;
                return false;
            }

            value = (TValue) result;
            return true;
        }

        /// <summary>Querys a single value with a database dependent sql statement.</summary>
        /// <param name="cmd">The database dependent sql statement.</param>
        /// <param name="database">The affected database (dependent on the storage engine this may be null).</param>
        /// <param name="table">The affected table (dependent on the storage engine this may be null).</param>
        /// <param name="fieldName">Name of the field (optional, only needed if multiple columns are returned).</param>
        /// <returns>The result value or null.</returns>
        public virtual object QueryValue(SqlCmd cmd, string database = null, string table = null, string fieldName = null)
        {
            if (Closed)
            {
                throw new ObjectDisposedException(ToString());
            }

            for (var i = 1;; i++)
            {
                var connection = GetConnection(database);
                var error = false;
                try
                {
                    using (var command = CreateCommand(connection, cmd))
                    using (var reader = command.ExecuteReader(CommandBehavior.KeyInfo))
                    {
                        var name = table ?? cmd.Text.GetValidChars(ASCII.Strings.SafeName);

                        // read schema
                        var layout = ReadSchema(reader, name);

                        // load rows
                        if (!reader.Read())
                        {
                            return null;
                        }

                        var fieldIndex = 0;
                        if (fieldName == null)
                        {
                            if (layout.FieldCount != 1)
                            {
                                throw new InvalidDataException(
                                    $"Error while reading row data: More than one field returned!\n\tDatabase: {database}\n\tTable: {table}\n\tCommand: {cmd}");
                            }
                        }
                        else
                        {
                            fieldIndex = layout.GetFieldIndex(fieldName, true);
                        }

                        var result = GetLocalValue(layout[fieldIndex], reader, reader.GetValue(fieldIndex));
                        if (reader.Read())
                        {
                            throw new InvalidDataException(
                                $"Error while reading row data: Additional data available (expected only one row of data)!\n\tDatabase: {database}\n\tTable: {table}\n\tCommand: {cmd}");
                        }

                        return result;
                    }
                }
                catch (Exception ex)
                {
                    error = true;
                    if (i > MaxErrorRetries)
                    {
                        throw;
                    }

                    Trace.TraceError("<red>{3}<default> Error during QueryValue(<cyan>{0}<default>, <cyan>{1}<default>) -> <yellow>retry {2}\n{4}", database,
                        table, i, ex.Message, ex);
                }
                finally
                {
                    ReturnConnection(ref connection, error);
                }
            }
        }

        #endregion

        #region QueryRow(SqlCmd cmd, ...)

        /// <summary>Queries for a dataset (selected fields, one row).</summary>
        /// <param name="cmd">the database dependent sql statement.</param>
        /// <param name="layout">The expected layout.</param>
        /// <param name="database">The affected database (dependent on the storage engine this may be null).</param>
        /// <param name="table">The affected table (dependent on the storage engine this may be null).</param>
        /// <returns>The result row.</returns>
        public Row QueryRow(SqlCmd cmd, ref RowLayout layout, string database = null, string table = null) => Query(cmd, ref layout, database, table).Single();

        #endregion

        #region Query(SqlCmd cmd, ...)

        /// <summary>Queries for all matching datasets.</summary>
        /// <param name="cmd">The database dependent sql statement.</param>
        /// <param name="database">The databasename (optional, used with cached connections).</param>
        /// <param name="table">The tablename (optional, used with cached connections).</param>
        /// <returns>The result rows.</returns>
        /// <typeparam name="TStruct">Result row type.</typeparam>
        public IList<TStruct> QueryStructs<TStruct>(SqlCmd cmd, string database = null, string table = null)
            where TStruct : struct
        {
            var layout = RowLayout.CreateTyped(typeof(TStruct));
            return QueryStructs<TStruct>(cmd, layout, database, table);
        }

        /// <summary>Queries for all matching datasets.</summary>
        /// <param name="cmd">The database dependent sql statement.</param>
        /// <param name="layout">The expected schema layout.</param>
        /// <param name="database">The databasename (optional, used with cached connections).</param>
        /// <param name="table">The tablename (optional, used with cached connections).</param>
        /// <returns>The result rows.</returns>
        /// <typeparam name="TStruct">Result row type.</typeparam>
        public IList<TStruct> QueryStructs<TStruct>(SqlCmd cmd, RowLayout layout, string database = null, string table = null)
            where TStruct : struct
        {
            if (layout == null)
            {
                throw new ArgumentNullException(nameof(layout));
            }

            if (table == null)
            {
                table = layout.Name;
            }

            var rows = Query(cmd, ref layout, database, table);
            return rows.Select(r => r.GetStruct<TStruct>(layout)).ToList();
        }

        /// <summary>Queries for all matching datasets.</summary>
        /// <param name="cmd">The database dependent sql statement.</param>
        /// <param name="database">The databasename (optional, used with cached connections).</param>
        /// <param name="table">The tablename (optional, used with cached connections).</param>
        /// <returns>The result rows.</returns>
        public IList<Row> Query(SqlCmd cmd, string database = null, string table = null)
        {
            RowLayout layout = null;
            return Query(cmd, ref layout, database, table);
        }

        /// <summary>Queries for all matching datasets.</summary>
        /// <param name="cmd">The database dependent sql statement.</param>
        /// <param name="layout">The expected schema layout (if unset the layout is returned).</param>
        /// <param name="database">The databasename (optional, used with cached connections).</param>
        /// <param name="table">The tablename (optional, used with cached connections).</param>
        /// <returns>The result rows.</returns>
        public virtual IList<Row> Query(SqlCmd cmd, ref RowLayout layout, string database = null, string table = null)
        {
            if (Closed)
            {
                throw new ObjectDisposedException(ToString());
            }

            if (table == null)
            {
                table = "result";
            }

            // get command
            for (var i = 1;; i++)
            {
                var connection = GetConnection(database);
                var error = false;
                try
                {
                    using (var command = CreateCommand(connection, cmd))
                    using (var reader = command.ExecuteReader(CommandBehavior.KeyInfo))
                    {
                        // load schema
                        var schema = ReadSchema(reader, table);

                        // layout specified ?
                        if (layout == null)
                        {
                            // no: use schema
                            layout = schema;
                        }
                        else if (DoSchemaCheckOnQuery)
                        {
                            // yes: check schema
                            CheckLayout(layout, schema);
                        }

                        // load rows
                        var result = new List<Row>();
                        while (reader.Read())
                        {
                            var row = ReadRow(layout, reader);
                            result.Add(row);
                        }

                        return result;
                    }
                }
                catch (Exception ex)
                {
                    error = true;
                    if (i > MaxErrorRetries)
                    {
                        throw;
                    }

                    Trace.TraceError("<red>{3}<default> Error during Query(<cyan>{0}<default>, <cyan>{1}<default>) -> <yellow>retry {2}\n{4}", database, table,
                        i, ex.Message, ex);
                }
                finally
                {
                    ReturnConnection(ref connection, error);
                }
            }
        }

        #endregion

        #endregion Query/Execute functions

        #region protected helper LogQuery()

        /// <summary>Logs the query in verbose mode.</summary>
        /// <param name="command">The command.</param>
        protected internal void LogQuery(IDbCommand command)
        {
            if (command.Parameters.Count > 0)
            {
                var paramText = new StringBuilder();
                foreach (IDataParameter dp in command.Parameters)
                {
                    if (paramText.Length > 0)
                    {
                        paramText.Append(',');
                    }

                    paramText.Append(dp.Value);
                }

                Trace.TraceInformation("Execute sql statement:\n<cyan>{0}\nParameters:\n<magenta>{1}", command.CommandText, paramText);
            }
            else
            {
                Trace.TraceInformation("Execute sql statement: <cyan>{0}", command.CommandText);
            }
        }

        #endregion

        #region protected assembly interface functionality

        /// <summary>Initializes the needed interop assembly and type and returns an instance.</summary>
        /// <returns>Returns an appropriate <see cref="IDbConnection" /> for this database engine.</returns>
        protected abstract IDbConnection GetDbConnectionType();

        #endregion

        #region protected database connection and command

        /// <summary>Gets a connection string for the <see cref="DbConnectionType" />.</summary>
        /// <param name="database">The database to connect to.</param>
        /// <returns>The connection string for the specified database.</returns>
        protected abstract string GetConnectionString(string database);

        /// <summary>Generates an command for the databaseconnection.</summary>
        /// <param name="connection">The connection the command will be executed at.</param>
        /// <param name="cmd">The sql command.</param>
        /// <returns>A new <see cref="IDbCommand" /> instance.</returns>
        protected virtual IDbCommand CreateCommand(SqlConnection connection, SqlCmd cmd)
        {
            if (connection == null)
            {
                throw new ArgumentNullException(nameof(connection));
            }

            if (cmd == null)
            {
                throw new ArgumentNullException(nameof(cmd));
            }

            var command = connection.CreateCommand();
            command.CommandText = cmd.Text;
            foreach (var parameter in cmd.Parameters)
            {
                var dataParameter = command.CreateParameter();
                if (SupportsNamedParameters)
                {
                    dataParameter.ParameterName = parameter.Name;
                }

                dataParameter.Value = parameter.Value;
                command.Parameters.Add(dataParameter);
            }

            command.CommandTimeout = Math.Max(1, (int) CommandTimeout.TotalSeconds);
            if (LogVerboseMessages)
            {
                LogQuery(command);
            }

            return command;
        }

        #endregion

        #region protected Schema reader

        /// <summary>Reads the <see cref="RowLayout" /> from an <see cref="IDataReader" /> containing a query result.</summary>
        /// <param name="reader">The reader to read from.</param>
        /// <param name="source">Name of the source.</param>
        /// <returns>A layout generated from the specified <paramref name="reader" /> using <paramref name="source" /> as name.</returns>
        protected virtual RowLayout ReadSchema(IDataReader reader, string source)
        {
            if (reader == null)
            {
                throw new ArgumentNullException(nameof(reader));
            }

            // check columns (name, number and type)
            var schemaTable = reader.GetSchemaTable();
            var fieldCount = reader.FieldCount;
            var fields = new IFieldProperties[fieldCount];

            // check fieldcount
            if (fieldCount != schemaTable.Rows.Count)
            {
                throw new InvalidDataException($"Invalid field count at {"SchemaTable"}!");
            }

            for (var i = 0; i < fieldCount; i++)
            {
                var row = schemaTable.Rows[i];
                var isHidden = row["IsHidden"];
                if ((isHidden != DBNull.Value) && (bool) isHidden)
                {
                    // continue;
                }

                var fieldName = (string) row["ColumnName"];
                if (string.IsNullOrEmpty(fieldName))
                {
                    fieldName = i.ToString();
                }

                var fieldSize = (uint) (int) row["ColumnSize"];
                var valueType = reader.GetFieldType(i);
                var dataType = GetLocalDataType(valueType, fieldSize);
                var fieldFlags = FieldFlags.None;
                var isKey = row["IsKey"];
                if ((isKey != DBNull.Value) && (bool) isKey)
                {
                    fieldFlags |= FieldFlags.ID;
                }

                var isAutoIncrement = row["IsAutoIncrement"];
                if ((isAutoIncrement != DBNull.Value) && (bool) isAutoIncrement)
                {
                    fieldFlags |= FieldFlags.AutoIncrement;
                }

                var isUnique = row["IsUnique"];
                if ((isUnique != DBNull.Value) && (bool) isUnique)
                {
                    fieldFlags |= FieldFlags.Unique;
                }

                // TODO detect bigint timestamps
                // TODO detect string encoding
                var properties = new FieldProperties
                {
                    Index = i,
                    Flags = fieldFlags,
                    DataType = dataType,
                    ValueType = valueType,
                    MaximumLength = fieldSize,
                    Name = fieldName,
                    TypeAtDatabase = dataType,
                    NameAtDatabase = fieldName
                };
                fields[i] = GetDatabaseFieldProperties(properties);
            }

            return RowLayout.CreateUntyped(source, fields);
        }

        #endregion

        /// <summary>Releases unmanaged and - optionally - managed resources.</summary>
        /// <param name="disposing">
        ///     <c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only
        ///     unmanaged resources.
        /// </param>
        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    if (pool != null)
                    {
                        pool.Close();
                        pool = null;
                    }
                }

                disposedValue = true;
            }
        }

        /// <summary>Reads a row from a DataReader.</summary>
        /// <param name="layout">The layout.</param>
        /// <param name="reader">The reader.</param>
        /// <returns>A row read from the reader.</returns>
        Row ReadRow(RowLayout layout, IDataReader reader)
        {
            var values = new object[layout.FieldCount];
            var read = reader.GetValues(values);
            if (reader.FieldCount != layout.FieldCount)
            {
                throw new InvalidDataException($"Error while reading row data at table {layout}!" + "\n" + "Invalid field count!");
            }

            try
            {
                for (var fieldNumber = 0; fieldNumber < layout.FieldCount; fieldNumber++)
                {
                    values[fieldNumber] = GetLocalValue(layout[fieldNumber], reader, values[fieldNumber]);
                }
            }
            catch (Exception ex)
            {
                throw new InvalidDataException($"Error while reading row data at table {layout}!", ex);
            }

            return new Row(layout, values, false);
        }

        /// <summary>Warns on unsafe connection.</summary>
        void WarnUnsafe()
        {
            if (AllowUnsafeConnections)
            {
                Trace.TraceWarning(
                    "<red>AllowUnsafeConnections is enabled!\nConnection details {0} including password and any transmitted data may be seen by any eavesdropper!",
                    ConnectionString.ToString(ConnectionStringPart.NoCredentials));
            }
        }
    }
}
