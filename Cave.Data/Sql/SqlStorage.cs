using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Reflection;
using System.Text;
using Cave.Collections.Generic;
using Cave.IO;

namespace Cave.Data.Sql
{
    /// <summary>
    /// Provides a base class for sql 92 <see cref="IStorage"/> implementations
    /// </summary>
    public abstract class SqlStorage : Storage, IDisposable
    {
        class SqlConnectionPool
        {
            TimeSpan? m_Timeout = TimeSpan.FromMinutes(5);
            SqlStorage m_Storage;
            LinkedList<SqlConnection> m_Queue = new LinkedList<SqlConnection>();
            Set<SqlConnection> m_Used = new Set<SqlConnection>();

            /// <summary>Gets or sets the connection close timeout.</summary>
            /// <value>The connection close timeout.</value>
            public TimeSpan ConnectionCloseTimeout { get => m_Timeout.Value; set => m_Timeout = value; }

            /// <summary>Gets the name of the log source.</summary>
            /// <value>The name of the log source.</value>
            public string LogSourceName => "SqlConnectionPool " + ((m_Storage != null) ? m_Storage.LogSourceName : "");

            /// <summary>Initializes a new instance of the <see cref="SqlConnectionPool"/> class.</summary>
            /// <param name="storage">The storage.</param>
            public SqlConnectionPool(SqlStorage storage)
            {
                m_Storage = storage;
            }

            /// <summary>Clears the whole connection pool (forced, including connections in use).</summary>
            public void Clear()
            {
                lock (this)
                {
                    foreach (SqlConnection connection in m_Used)
                    {
                        Trace.TraceInformation(string.Format("Closing connection {0} (pool clearing)", connection));
                        connection.Close();
                    }
                    foreach (SqlConnection connection in m_Queue)
                    {
                        Trace.TraceInformation(string.Format("Closing connection {0} (pool clearing)", connection));
                        connection.Close();
                    }
                    m_Queue.Clear();
                    m_Used.Clear();
                }
            }

            SqlConnection GetQueuedConnection(string database)
            {
                LinkedListNode<SqlConnection> nextNode = m_Queue.First;
                LinkedListNode<SqlConnection> selectedNode = null;
                while (nextNode != null)
                {
                    //get current and next node
                    LinkedListNode<SqlConnection> currentNode = nextNode;
                    nextNode = currentNode.Next;
                    //remove dead and old connections
                    if ((currentNode.Value.State != ConnectionState.Open) || (DateTime.UtcNow > currentNode.Value.LastUsed + m_Timeout.Value))
                    {
                        Trace.TraceInformation(string.Format("Closing connection {0} (livetime exceeded) (Idle:{1} Used:{2})", currentNode.Value, m_Queue.Count, m_Used.Count));
                        currentNode.Value.Dispose();
                        m_Queue.Remove(currentNode);
                        continue;
                    }
                    //allow only connection with matching db name ?
                    if (!m_Storage.DBConnectionCanChangeDataBase)
                    {
                        //check if database name matches
                        if (currentNode.Value.Database != database) { continue; }
                    }
                    //set selected node 
                    selectedNode = currentNode;
                    //break if we found a perfect match
                    if (currentNode.Value.Database == database) { break; }
                }
                if (selectedNode != null)
                {
                    //remove node
                    m_Queue.Remove(selectedNode);
                    m_Used.Add(selectedNode.Value);
                    return selectedNode.Value;
                }
                //nothing found
                return null;
            }

            /// <summary>Gets the connection.</summary>
            /// <param name="databaseName">Name of the database.</param>
            /// <returns></returns>
            public SqlConnection GetConnection(string databaseName)
            {
                lock (this)
                {
                    SqlConnection connection = GetQueuedConnection(databaseName);
                    if (connection == null)
                    {
                        Trace.TraceInformation("Creating new connection for Database {0} (Idle:{1} Used:{2})", databaseName, m_Queue.Count, m_Used.Count);
                        IDbConnection iDbConnection = m_Storage.CreateNewConnection(databaseName);
                        connection = new SqlConnection(databaseName, iDbConnection);
                        m_Used.Add(connection);
                        Trace.TraceInformation(string.Format("Created new connection for Database {0} (Idle:{1} Used:{2})", databaseName, m_Queue.Count, m_Used.Count));
                    }
                    else
                    {
                        if (connection.Database != databaseName)
                        {
                            connection.ChangeDatabase(databaseName);
                        }
                    }
                    return connection;
                }
            }

            /// <summary>
            /// Returns a connection to the connection pool for reuse
            /// </summary>
            /// <param name="connection">The connection to return to the queue</param>
            /// <param name="close">Force close of the connection</param>
            public void ReturnConnection(ref SqlConnection connection, bool close = false)
            {
                if (connection == null)
                {
                    throw new ArgumentNullException("Connection");
                }

                lock (this)
                {
                    if (m_Used.Contains(connection))
                    {
                        m_Used.Remove(connection);
                        if (!close && (connection.State == ConnectionState.Open))
                        {
                            m_Queue.AddFirst(connection);
                            connection = null;
                            return;
                        }
                    }
                }
                Trace.TraceInformation(string.Format("Closing connection {0} (sql error)", connection));
                connection.Close();
                connection = null;
            }

            /// <summary>Closes this instance.</summary>
            public void Close()
            {
                Clear();
            }
        }

        SqlConnectionPool m_Pool;
        bool m_WarnedUnsafe;

        /// <summary>
        /// Do a result schema check on each query (this impacts performance very badly if you query large amounts of single rows).
        /// A common practice is to use this while developing the application and unittest, running the unittests and set this to false on release builds.
        /// </summary>
#if DEBUG
		public bool DoSchemaCheckOnQuery { get; set; } = Debugger.IsAttached;
#else
        public bool DoSchemaCheckOnQuery { get; set; }
#endif

        /// <summary>Gets or sets the maximum error retries.</summary>
        /// <remarks>If set to &lt; 1 only a single try is made to execute a query. If set to any number &gt; 0 this values
        /// indicates the number of retries that are made after the first try and subsequent tries fail.</remarks>
        /// <value>The maximum error retries.</value>
        public int MaxErrorRetries { get; set; } = 3;

        /// <summary>Gets or sets the connection close timeout.</summary>
        /// <value>The connection close timeout.</value>
        public TimeSpan ConnectionCloseTimeout { get => m_Pool.ConnectionCloseTimeout; set => m_Pool.ConnectionCloseTimeout = value; }

        #region helper LogQuery        
        /// <summary>Logs the query in verbose mode.</summary>
        /// <param name="command">The command.</param>
        protected internal void LogQuery(IDbCommand command)
        {
            if (command.Parameters.Count > 0)
            {
                StringBuilder paramText = new StringBuilder();
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

        #region constructors

        /// <summary>Creates a new <see cref="SqlStorage" /> with the specified ConnectionString</summary>
        /// <param name="connectionString">the connection details</param>
        /// <param name="options">The options.</param>
        /// <exception cref="TypeLoadException">
        /// </exception>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Usage", "CA2214:DoNotCallOverridableMethodsInConstructors")]
        protected SqlStorage(ConnectionString connectionString, DbConnectionOptions options = DbConnectionOptions.None)
            : base(connectionString, options)
        {
            Trace.TraceInformation("Initializing native interop assemblies.");
            InitializeInterOp(out DbAdapterAssembly, out DbConnectionType);
            if (DbAdapterAssembly == null)
            {
                throw new TypeLoadException(string.Format("{0} did not initialize {1} correctly!", "InitializeInterOp()", "DbAdapterAssembly"));
            }

            if (DbConnectionType == null)
            {
                throw new TypeLoadException(string.Format("{0} did not initialize {1} correctly!", "InitializeInterOp()", "DbConnectionType"));
            }

            AllowUnsafeConnections = options.HasFlag(DbConnectionOptions.AllowUnsafeConnections);
            Trace.TraceInformation("Preparing sql connection to {0} with unsafe connections {1}", connectionString, AllowUnsafeConnections);

            m_Pool = new SqlConnectionPool(this);
            WarnUnsafe();
        }

        /// <summary>Warns on unsafe connection.</summary>
        protected void WarnUnsafe()
        {
            if (!m_WarnedUnsafe && AllowUnsafeConnections)
            {
                m_WarnedUnsafe = true;
                Trace.TraceWarning("<red>AllowUnsafeConnections is enabled!\nConnection details {0} including password and any transmitted data may be seen by any eavesdropper!", ConnectionString);
            }
        }
        #endregion

        #region assembly interface functionality
        /// <summary>
        /// Initializes the needed interop assembly and type
        /// </summary>
        /// <param name="dbAdapterAssembly">Assembly containing all needed types</param>
        /// <param name="dbConnectionType">IDbConnection type used for the database</param>
        protected abstract void InitializeInterOp(out Assembly dbAdapterAssembly, out Type dbConnectionType);

        /// <summary>
        /// Obtains whether the db connections can change the database with the Sql92 "USE Database" command.
        /// </summary>
        protected abstract bool DBConnectionCanChangeDataBase { get; }

        /// <summary>
        /// Obtains the <see cref="IDbConnection"/> type
        /// </summary>
        protected readonly Type DbConnectionType;

        /// <summary>
        /// Obtains the <see cref="Assembly"/> with the name <see cref="DbAdapterAssembly"/> containing the needed <see cref="DbConnectionType"/>
        /// </summary>
        protected readonly Assembly DbAdapterAssembly;

        #endregion

        #region database specific conversions

        /// <summary>Gets or sets the native date time format.</summary>
        /// <value>The native date time format.</value>
        public string NativeDateTimeFormat = "yyyy'-'MM'-'dd' 'HH':'mm':'ss";

        /// <summary>Escapes a field name for direct use in a query</summary>
        /// <param name="field">The field.</param>
        /// <returns></returns>
        public abstract string EscapeFieldName(FieldProperties field);

        /// <summary>
        /// Escapes a string value for direct use in a query (whenever possible use parameters instead!)
        /// </summary>
        /// <param name="text"></param>
        /// <returns></returns>
        public virtual string EscapeString(string text)
        {
            if (text == null)
            {
                throw new ArgumentNullException("String");
            }
            //escape escape char
            if (-1 != text.IndexOf('\\'))
            {
                text = text.Replace("\\", "\\\\");
            }
            //escape invalid chars
            if (-1 != text.IndexOf('\0'))
            {
                text = text.Replace("\0", "\\0");
            }

            if (-1 != text.IndexOf('\''))
            {
                text = text.Replace("'", "\\'");
            }

            if (-1 != text.IndexOf('"'))
            {
                text = text.Replace("\"", "\\\"");
            }

            if (-1 != text.IndexOf('\b'))
            {
                text = text.Replace("\b", "\\b");
            }

            if (-1 != text.IndexOf('\n'))
            {
                text = text.Replace("\n", "\\n");
            }

            if (-1 != text.IndexOf('\r'))
            {
                text = text.Replace("\r", "\\r");
            }

            if (-1 != text.IndexOf('\t'))
            {
                text = text.Replace("\t", "\\t");
            }

            return "'" + text + "'";
        }

        /// <summary>Escapes the given binary data.</summary>
        /// <param name="data">The data.</param>
        /// <returns></returns>
        public virtual string EscapeBinary(byte[] data)
        {
            return "X'" + StringExtensions.ToHexString(data) + "'";
        }

        /// <summary>
        /// Escapes a field value for direct use in a query (whenever possible use parameters instead!)
        /// </summary>
        /// <param name="properties">FieldProperties</param>
        /// <param name="value">Value to escape</param>
        /// <returns></returns>
        public virtual string EscapeFieldValue(FieldProperties properties, object value)
        {
            if (properties == null)
            {
                throw new ArgumentNullException("Properties");
            }

            if (value == null)
            {
                return "NULL";
            }

            if (value is byte[])
            {
                return EscapeBinary((byte[])value);
            }

            if (value is byte || value is sbyte || value is ushort || value is short || value is uint || value is int || value is long || value is ulong || value is decimal)
            {
                return value.ToString();
            }

            if (value is double)
            {
                return ((double)value).ToString("R", CultureInfo.InvariantCulture);
            }

            if (value is float)
            {
                return ((float)value).ToString("R", CultureInfo.InvariantCulture);
            }

            if (value is bool)
            {
                return ((bool)value == true) ? "1" : "0";
            }

            if (value is TimeSpan)
            {
                return ((TimeSpan)value).TotalSeconds.ToString("R", CultureInfo.InvariantCulture);
            }

            if (value is DateTime)
            {
                DateTime dt;
                switch (properties.DateTimeKind)
                {
                    case DateTimeKind.Unspecified: dt = ((DateTime)value); break;
                    case DateTimeKind.Utc: dt = ((DateTime)value).ToUniversalTime(); break;
                    case DateTimeKind.Local: dt = ((DateTime)value).ToLocalTime(); break;
                    default: throw new NotSupportedException(string.Format("DateTimeKind {0} not supported!", properties.DateTimeKind));
                }
                switch (properties.DateTimeType)
                {
                    case DateTimeType.Undefined:
                    case DateTimeType.Native: return "'" + dt.ToString(NativeDateTimeFormat) + "'";
                    case DateTimeType.BigIntHumanReadable: return dt.ToString(CaveSystemData.BigIntDateTimeFormat);
                    case DateTimeType.BigIntTicks: return dt.Ticks.ToString();
                    case DateTimeType.DecimalSeconds: return (dt.Ticks / (decimal)TimeSpan.TicksPerSecond).ToString();
                    case DateTimeType.DoubleSeconds: return (dt.Ticks / (double)TimeSpan.TicksPerSecond).ToString();
                    default: throw new NotImplementedException();
                }
            }

            if (value.GetType().IsEnum)
            {
                return Convert.ToInt64(value).ToString();
            }

            return EscapeString(value.ToString());
        }

        /// <summary>Obtains the local <see cref="DataType" /> for the specified database fieldtype</summary>
        /// <param name="fieldType">The field type at the database</param>
        /// <param name="fieldSize">The field size at the database</param>
        /// <returns></returns>
        protected virtual DataType GetLocalDataType(Type fieldType, uint fieldSize)
        {
            return RowLayout.DataTypeFromType(fieldType);
        }

        /// <summary>Converts a local value into a database value</summary>
        /// <param name="field">The <see cref="FieldProperties" /> of the affected field</param>
        /// <param name="localValue">The local value to be encoded for the database</param>
        /// <returns></returns>
        /// <exception cref="System.ArgumentNullException">Field</exception>
        /// <exception cref="System.NotSupportedException"></exception>
        /// <exception cref="System.NotImplementedException"></exception>
        public virtual object GetDatabaseValue(FieldProperties field, object localValue)
        {
            if (field == null)
            {
                throw new ArgumentNullException("Field");
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
                    TimeSpan value = (TimeSpan)localValue;
                    switch (field.DateTimeType)
                    {
                        case DateTimeType.Undefined:
                        case DateTimeType.Native: return value;

                        case DateTimeType.BigIntHumanReadable: return long.Parse(new DateTime(value.Ticks).ToString(CaveSystemData.BigIntDateTimeFormat));
                        case DateTimeType.BigIntTicks: return value.Ticks;
                        case DateTimeType.DecimalSeconds: return ((decimal)value.Ticks / TimeSpan.TicksPerSecond);
                        case DateTimeType.DoubleSeconds: return ((double)value.Ticks / TimeSpan.TicksPerSecond);
                        default: throw new NotImplementedException();
                    }
                }

                case DataType.DateTime:
                {
                    if ((DateTime)localValue == default(DateTime))
                    {
                        return null;
                    }

                    DateTime value = (DateTime)localValue;
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
                            throw new NotSupportedException(string.Format("DateTimeKind {0} not supported!", field.DateTimeKind));
                    }
                    switch (field.DateTimeType)
                    {
                        case DateTimeType.Undefined:
                        case DateTimeType.Native: return value;

                        case DateTimeType.BigIntHumanReadable: return long.Parse(value.ToString(CaveSystemData.BigIntDateTimeFormat));
                        case DateTimeType.BigIntTicks: return value.Ticks;
                        case DateTimeType.DecimalSeconds: return ((decimal)value.Ticks / TimeSpan.TicksPerSecond);
                        case DateTimeType.DoubleSeconds: return ((double)value.Ticks / TimeSpan.TicksPerSecond);
                        default: throw new NotImplementedException();
                    }
                }
            }
            return localValue;
        }

        /// <summary>
        /// Converts a database value into a local value
        /// </summary>
        /// <param name="field">The <see cref="FieldProperties"/> of the affected field</param>
        /// <param name="databaseValue">The value retrieved from the database</param>
        /// <returns>Returns a value for local use</returns>
        public virtual object GetLocalValue(FieldProperties field, object databaseValue)
        {
            if (field == null)
            {
                throw new ArgumentNullException("Field");
            }

            switch (field.DataType)
            {
                case DataType.User:
                {
                    if (databaseValue == DBNull.Value)
                    {
                        return null;
                    }
                    string text = (string)databaseValue;
                    if (text == null)
                    {
                        return null;
                    }
                    return field.ParseValue(text, null, CultureInfo.InvariantCulture);
                }
                case DataType.Enum:
                {
                    if (databaseValue == null || databaseValue.Equals(DBNull.Value))
                    {
                        databaseValue = 0L;
                    }
                    return Enum.ToObject(field.ValueType, (long)databaseValue);
                }
                case DataType.DateTime:
                {
                    long ticks = 0;
                    if (databaseValue != null && !databaseValue.Equals(DBNull.Value))
                    {
                        switch (field.DateTimeType)
                        {
                            default: throw new NotSupportedException(string.Format("DateTimeType {0} is not supported", field.DateTimeType));

                            case DateTimeType.BigIntHumanReadable:
                                ticks = DateTime.ParseExact(databaseValue.ToString(), CaveSystemData.BigIntDateTimeFormat, CultureInfo.InvariantCulture).Ticks;
                                break;

                            case DateTimeType.Undefined:
                            case DateTimeType.Native:
                                try { ticks = ((DateTime)Convert.ChangeType(databaseValue, typeof(DateTime), CultureInfo.InvariantCulture)).Ticks; }
                                catch { ticks = 0; }
                                break;

                            case DateTimeType.BigIntTicks:
                                ticks = (long)databaseValue;
                                break;

                            case DateTimeType.DecimalSeconds:
                                ticks = (long)decimal.Round((decimal)databaseValue * TimeSpan.TicksPerSecond);
                                break;

                            case DateTimeType.DoubleSeconds:
                                ticks = (long)Math.Round((double)databaseValue * TimeSpan.TicksPerSecond);
                                break;
                        }
                    }
                    return new DateTime(ticks, field.DateTimeKind);
                }
                case DataType.TimeSpan:
                {
                    long ticks = 0;
                    if (databaseValue != null && !databaseValue.Equals(DBNull.Value))
                    {
                        switch (field.DateTimeType)
                        {
                            default: throw new NotSupportedException(string.Format("DateTimeType {0} is not supported", field.DateTimeType));

                            case DateTimeType.BigIntHumanReadable:
                                ticks = DateTime.ParseExact(databaseValue.ToString(), CaveSystemData.BigIntDateTimeFormat, CultureInfo.InvariantCulture).Ticks;
                                break;

                            case DateTimeType.Undefined:
                            case DateTimeType.Native:
                                try { ticks = ((TimeSpan)Convert.ChangeType(databaseValue, typeof(TimeSpan), CultureInfo.InvariantCulture)).Ticks; }
                                catch { ticks = 0; }
                                break;

                            case DateTimeType.BigIntTicks:
                                ticks = (long)databaseValue;
                                break;

                            case DateTimeType.DecimalSeconds:
                                ticks = (long)decimal.Round((decimal)databaseValue * TimeSpan.TicksPerSecond);
                                break;

                            case DateTimeType.DoubleSeconds:
                                ticks = (long)Math.Round((double)databaseValue * TimeSpan.TicksPerSecond);
                                break;
                        }
                    }
                    return new TimeSpan(ticks);
                }
            }
            if (databaseValue == DBNull.Value)
            {
                return null;
            }

            return databaseValue;
        }

        #endregion

        #region database connection and command

        /// <summary>
        /// Closes and clears all cached connections. 
        /// </summary>
        /// <exception cref="ObjectDisposedException">SqlConnection</exception>
        public void ClearCachedConnections()
        {
            if (m_Pool == null)
            {
                throw new ObjectDisposedException("SqlConnection");
            }
            m_Pool.Clear();
        }

        /// <summary>
        /// Obtains a connection string for the <see cref="DbConnectionType"/>
        /// </summary>
        /// <param name="database">The database to connect to</param>
        /// <returns></returns>
        protected abstract string GetConnectionString(string database);

        /// <summary>Retrieves a connection (from the cache) or creates a new one if needed</summary>
        /// <param name="databaseName">Name of the database.</param>
        /// <returns></returns>
        public SqlConnection GetConnection(string databaseName)
        {
            return m_Pool.GetConnection(databaseName);
        }

        /// <summary>
        /// Returns a connection to the connection pool for reuse
        /// </summary>
        /// <param name="connection">The connection to return to the queue</param>
        /// <param name="close">Force close of the connection</param>
        public void ReturnConnection(ref SqlConnection connection, bool close)
        {
            if (connection == null) { return; }
            m_Pool.ReturnConnection(ref connection, close);
        }

        /// <summary>Creates a new database connection</summary>
        /// <param name="databaseName">Name of the database.</param>
        /// <returns></returns>
        public virtual IDbConnection CreateNewConnection(string databaseName)
        {
            IDbConnection connection = (IDbConnection)Activator.CreateInstance(DbConnectionType);
            connection.ConnectionString = GetConnectionString(databaseName);
            connection.Open();
            WarnUnsafe();
            if (DBConnectionCanChangeDataBase)
            {
                if (databaseName != null && connection.Database != databaseName)
                {
                    connection.ChangeDatabase(databaseName);
                }
            }
            return connection;
        }

        /// <summary>
        /// Generates an command for the databaseconnection
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="cmd"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2100:SQL-Abfragen auf Sicherheitsrisiken überprüfen")]
        protected virtual IDbCommand CreateCommand(SqlConnection connection, string cmd, params DatabaseParameter[] parameters)
        {
            if (connection == null)
            {
                throw new ArgumentNullException("Connection");
            }

            if (parameters == null)
            {
                throw new ArgumentNullException("Parameters");
            }

            IDbCommand command = connection.CreateCommand();
            command.CommandText = cmd ?? throw new ArgumentNullException("Command");
            foreach (DatabaseParameter parameter in parameters)
            {
                IDbDataParameter dataParameter = command.CreateParameter();
                if (SupportsNamedParameters)
                {
                    dataParameter.ParameterName = ParameterPrefix + parameter.Name;
                }
                dataParameter.Value = parameter.Value;
                command.Parameters.Add(dataParameter);
            }
            command.CommandTimeout = Math.Max(1, (int)CommandTimeout.TotalSeconds);
            if (LogVerboseMessages)
            {
                LogQuery(command);
            }

            return command;
        }

        #endregion

        #region sql command and query functions

        /// <summary>
        /// Obtains wether the connection supports named parameters or not
        /// </summary>
        public abstract bool SupportsNamedParameters { get; }

        /// <summary>
        /// Obtains wether the connection supports select * groupby
        /// </summary>
        public abstract bool SupportsAllFieldsGroupBy { get; }

        /// <summary>
        /// Obtains the parameter prefix for this storage
        /// </summary>
        public abstract string ParameterPrefix { get; }

        /// <summary>
        /// Command timeout for all sql commands
        /// </summary>
        public TimeSpan CommandTimeout = TimeSpan.FromMinutes(1);

        /// <summary>
        /// Obtains FieldProperties for the Database based on requested FieldProperties
        /// </summary>
        /// <param name="field"></param>
        /// <returns></returns>
        public override FieldProperties GetDatabaseFieldProperties(FieldProperties field)
        {
            if (field == null)
            {
                throw new ArgumentNullException("LocalField");
            }

            //check if datatype is replacement for missing sql type
            switch (field.DataType)
            {
                case DataType.Enum:
                    return new FieldProperties(field, DataType.Int64);

                case DataType.User:
                    return new FieldProperties(field, DataType.String);

                case DataType.DateTime:
                case DataType.TimeSpan:
                    switch (field.DateTimeType)
                    {
                        case DateTimeType.Undefined:
                        case DateTimeType.Native: return field;
                        case DateTimeType.BigIntHumanReadable: return new FieldProperties(field, DataType.Int64);
                        case DateTimeType.BigIntTicks: return new FieldProperties(field, DataType.Int64);
                        case DateTimeType.DecimalSeconds: return new FieldProperties(field, DataType.Decimal, 65.30f);
                        case DateTimeType.DoubleSeconds: return new FieldProperties(field, DataType.Double);
                        default: throw new NotImplementedException();
                    }
            }
            return field;
        }

        #region Schema reader

        /// <summary>
        /// Reads the <see cref="RowLayout"/> from an <see cref="IDataReader"/> containing a query result
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="source">Source</param>
        /// <returns></returns>
        protected virtual RowLayout ReadSchema(IDataReader reader, string source)
        {
            if (reader == null)
            {
                throw new ArgumentNullException("Reader");
            }
            //check columns (name, number and type)
            DataTable schemaTable = reader.GetSchemaTable();

            List<FieldProperties> fields = new List<FieldProperties>();
            //check fieldcount
            int fieldCount = reader.FieldCount;
            if (fieldCount != schemaTable.Rows.Count)
            {
                throw new InvalidDataException(string.Format("Invalid field count at {0}!", "SchemaTable"));
            }

            for (int i = 0; i < fieldCount; i++)
            {
                DataRow row = schemaTable.Rows[i];

                object isHidden = row["IsHidden"];
                if ((isHidden != DBNull.Value) && (bool)isHidden)
                {
                    //continue;
                }

                string fieldName = (string)row["ColumnName"];
                if (string.IsNullOrEmpty(fieldName))
                {
                    fieldName = i.ToString();
                }

                uint fieldSize = (uint)(int)row["ColumnSize"];
                Type fieldType = reader.GetFieldType(i);
                DataType dataType = GetLocalDataType(fieldType, fieldSize);
                FieldFlags fieldFlags = FieldFlags.None;

                object isKey = row["IsKey"];
                if ((isKey != DBNull.Value) && (bool)isKey)
                {
                    fieldFlags |= FieldFlags.ID;
                }

                object isAutoIncrement = row["IsAutoIncrement"];
                if ((isAutoIncrement != DBNull.Value) && (bool)isAutoIncrement)
                {
                    fieldFlags |= FieldFlags.AutoIncrement;
                }

                object isUnique = row["IsUnique"];
                if ((isUnique != DBNull.Value) && (bool)isUnique)
                {
                    fieldFlags |= FieldFlags.Unique;
                }

                //TODO detect bigint timestamps
                //TODO detect string encoding
                FieldProperties properties = new FieldProperties(source, fieldFlags, dataType, fieldType, fieldSize, fieldName, dataType, DateTimeType.Native, DateTimeKind.Utc, StringEncoding.UTF8, fieldName, null, null, null);
                properties = GetDatabaseFieldProperties(properties);
                fields.Add(properties);
            }
            return RowLayout.CreateUntyped(source, fields.ToArray());
        }

        #endregion

        #region execute function
        /// <summary>
        /// Executes a database dependent sql statement silently
        /// </summary>
        /// <param name="database">The affected database (dependent on the storage engine this may be null)</param>
        /// <param name="table">The affected table (dependent on the storage engine this may be null)</param>
        /// <param name="cmd">the database dependent sql statement</param>
        /// <param name="parameters">the parameters for the sql statement</param>
        public virtual int Execute(string database, string table, string cmd, params DatabaseParameter[] parameters)
        {
            if (Closed)
            {
                throw new ObjectDisposedException(ToString());
            }

            for (int i = 1; ; i++)
            {
                SqlConnection connection = GetConnection(database);
                bool error = false;
                try
                {
                    using (IDbCommand command = CreateCommand(connection, cmd, parameters))
                    {
                        return command.ExecuteNonQuery();
                    }
                }
                catch (Exception ex)
                {
                    error = true;
                    if (i > MaxErrorRetries) { throw; }

                    Trace.TraceError("<red>{3}<default> Error during Execute(<cyan>{0}<default>, <cyan>{1}<default>) -> <yellow>retry {2}\n{4}", database, table, i, ex.Message, ex);
                }
                finally { ReturnConnection(ref connection, error); }
            }
        }
        #endregion

        /// <summary>
        /// Checks another RowLayout for equality with this one and thows an Exception on differences
        /// </summary>
        /// <param name="table">Table name to check layout</param>
        /// <param name="databaseLayout">The database layout</param>
        /// <param name="itemLayout">The local struct layout</param>
        public void CheckLayout(string table, RowLayout databaseLayout, RowLayout itemLayout)
        {
            if (databaseLayout == null)
            {
                throw new ArgumentNullException("DatabaseLayout");
            }

            if (itemLayout == null)
            {
                throw new ArgumentNullException("StructLayout");
            }

            if (databaseLayout.FieldCount != itemLayout.FieldCount)
            {
                throw new InvalidDataException(string.Format("Fieldcount of table {0} differs (found {1} expected {2})!", table, databaseLayout.FieldCount, itemLayout.FieldCount));
            }

            for (int i = 0; i < databaseLayout.FieldCount; i++)
            {
                FieldProperties databaseField = databaseLayout.GetProperties(i);
                FieldProperties valueField = GetDatabaseFieldProperties(itemLayout.GetProperties(i));
                if (!databaseField.Equals(valueField))
                {
                    throw new InvalidDataException(string.Format("Fieldproperties of table {0} differ! (found {1} expected {2})!", table, databaseField, valueField));
                }
            }
        }

        #region Query functions
        /// <summary>Reads a row from a DataReader</summary>
        /// <param name="layout">The layout.</param>
        /// <param name="reader">The reader.</param>
        /// <returns></returns>
        /// <exception cref="InvalidDataException">
        /// </exception>
        Row ReadRow(RowLayout layout, IDataReader reader)
        {
            object[] values = new object[layout.FieldCount];
            int count = reader.GetValues(values);
            if (count != layout.FieldCount)
            {
                throw new InvalidDataException(string.Format("Error while reading row data at table {0}!", layout) + "\n" + string.Format("Invalid field count!"));
            }
            int fieldNumber = 0;
            try
            {
                for (; fieldNumber < count; fieldNumber++)
                {
                    values[fieldNumber] = GetLocalValue(layout.GetProperties(fieldNumber), values[fieldNumber]);
                }
            }
            catch (Exception ex)
            {
                object id = 0;
                if (layout.IDFieldIndex > -1)
                {
                    id = values[layout.IDFieldIndex];
                }
                throw new InvalidDataException(string.Format("Error while reading row data at table {0}!", layout) + "\n" + string.Format("Invalid field value at ID / Field: {0} / {1}!", id, layout.GetProperties(fieldNumber)), ex);
            }
            return new Row(values);
        }

        /// <summary>
        /// Obtains the <see cref="RowLayout"/> of the specified database table
        /// </summary>
        /// <param name="database">The affected database (dependent on the storage engine this may be null)</param>
        /// <param name="table">The affected table (dependent on the storage engine this may be null)</param>
        /// <returns></returns>
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

            for (int i = 1; ; i++)
            {
                SqlConnection connection = GetConnection(database);
                bool error = false;
                try
                {
                    using (IDbCommand cmd = CreateCommand(connection, "SELECT * FROM " + FQTN(database, table) + " WHERE FALSE"))
                    using (IDataReader reader = cmd.ExecuteReader(CommandBehavior.KeyInfo))
                    {
                        return ReadSchema(reader, table);
                    }
                }
                catch (Exception ex)
                {
                    error = true;
                    if (i > MaxErrorRetries) { throw; }
                    Trace.TraceError("<red>{3}<default> Error during QuerySchema(<cyan>{0}<default>, <cyan>{1}<default>) -> <yellow>retry {2}\n{4}", database, table, i, ex.Message, ex);
                }
                finally { ReturnConnection(ref connection, error); }
            }
        }

        /// <summary>
        /// querys a single value with a database dependent sql statement
        /// </summary>
        /// <param name="database">The affected database (dependent on the storage engine this may be null)</param>
        /// <param name="table">The affected table (dependent on the storage engine this may be null)</param>
        /// <param name="cmd">the database dependent sql statement</param>
        /// <param name="parameters">the parameters for the sql statement</param>
        /// <returns></returns>
        public virtual object QueryValue(string database, string table, string cmd, params DatabaseParameter[] parameters)
        {
            if (Closed)
            {
                throw new ObjectDisposedException(ToString());
            }

            for (int i = 1; ; i++)
            {
                SqlConnection connection = GetConnection(database);
                bool error = false;
                try
                {
                    using (IDbCommand command = CreateCommand(connection, cmd, parameters))
                    using (IDataReader reader = command.ExecuteReader(CommandBehavior.KeyInfo))
                    {
                        string name = table ?? cmd;
                        //read schema
                        RowLayout layout = ReadSchema(reader, name);
                        //load rows
                        if (!reader.Read())
                        {
                            throw new InvalidDataException(string.Format("Error while reading row data!") + "\n" + string.Format("No result dataset available!") + string.Format("\n Database: {0}\n Table: {1}\n Command: {2}", database, table, cmd));
                        }

                        if (layout.FieldCount != 1)
                        {
                            throw new InvalidDataException(string.Format("Error while reading row data!") + "\n" + string.Format("More than one field returned!") + string.Format("\n Database: {0}\n Table: {1}\n Command: {2}", database, table, cmd));
                        }

                        object result = GetLocalValue(layout.GetProperties(0), reader.GetValue(0));
                        if (reader.Read())
                        {
                            throw new InvalidDataException(string.Format("Error while reading row data!") + "\n" + string.Format("Additional data available (expected only one row of data)!") + string.Format("\n Database: {0}\n Table: {1}\n Command: {2}", database, table, cmd));
                        }

                        return result;
                    }
                }
                catch (Exception ex)
                {
                    error = true;
                    if (i > MaxErrorRetries) { throw; }
                    Trace.TraceError("<red>{3}<default> Error during QueryValue(<cyan>{0}<default>, <cyan>{1}<default>) -> <yellow>retry {2}\n{4}", database, table, i, ex.Message, ex);
                }
                finally { ReturnConnection(ref connection, error); }
            }
        }

        /// <summary>
        /// Queries for a dataset (selected fields, one row) without field type checks
        /// </summary>
        /// <param name="layout">The expected layout. This needs to be given to support GetStruct().</param>
        /// <param name="database">The affected database (dependent on the storage engine this may be null)</param>
        /// <param name="table">The affected table (dependent on the storage engine this may be null)</param>
        /// <param name="cmd">the database dependent sql statement</param>
        /// <param name="parameters">the parameters for the sql statement</param>
        /// <returns></returns>
        public Row QueryRow(RowLayout layout, string database, string table, string cmd, params DatabaseParameter[] parameters)
        {
            List<Row> rows = Query(layout, database, table, cmd, parameters);
            if (rows.Count > 1)
            {
                throw new InvalidDataException(string.Format("Error while reading row data!") + "\n" + string.Format("Additional data available (expected only one row of data)!") + string.Format("\n Database: {0}\n Table: {1}\n Command: {2}", database, table, cmd));
            }
            if (rows.Count < 1)
            {
                throw new InvalidDataException(string.Format("Error while reading row data!") + "\n" + string.Format("No result dataset available!") + string.Format("\n Database: {0}\n Table: {1}\n Command: {2}", database, table, cmd));
            }
            return rows[0];
        }

        /// <summary>
        /// Queries for all matching datasets (selected fields, multiple rows) without field type checks
        /// </summary>
        /// <param name="layout">The expected layout. This needs to be given to support GetStruct().</param>
        /// <param name="database">The affected database (dependent on the storage engine this may be null)</param>
        /// <param name="table">The affected table (dependent on the storage engine this may be null)</param>
        /// <param name="cmd">the database dependent sql statement</param>
        /// <param name="parameters">the parameters for the sql statement</param>
        /// <returns></returns>
        public virtual List<Row> Query(RowLayout layout, string database, string table, string cmd, params DatabaseParameter[] parameters)
        {
            if (Closed)
            {
                throw new ObjectDisposedException(ToString());
            }
            //get command
            for (int i = 1; ; i++)
            {
                SqlConnection connection = GetConnection(database);
                bool error = false;
                try
                {
                    using (IDbCommand command = CreateCommand(connection, cmd, parameters))
                    using (IDataReader reader = command.ExecuteReader(CommandBehavior.KeyInfo))
                    {
                        //set layout
                        RowLayout schema = layout;
                        if (schema == null)
                        {
                            //read layout from schema
                            string name = table ?? cmd;
                            schema = ReadSchema(reader, name);
                            layout = schema;
                        }
                        else if (DoSchemaCheckOnQuery)
                        {
                            //read schema
                            CheckLayout(table, schema, layout);
                        }

                        //load rows
                        List<Row> result = new List<Row>();
                        while (reader.Read())
                        {
                            Row row = ReadRow(layout, reader);
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

                    Trace.TraceError("<red>{3}<default> Error during Query(<cyan>{0}<default>, <cyan>{1}<default>) -> <yellow>retry {2}\n{4}", database, table, i, ex.Message, ex);
                }
                finally { ReturnConnection(ref connection, error); }
            }
        }

        /// <summary>
        /// Queries for a dataset (selected fields, one row) with full field type checks (struct has to match database row)
        /// </summary>
        /// <param name="database">The affected database (dependent on the storage engine this may be null)</param>
        /// <param name="table">The affected table (dependent on the storage engine this may be null)</param>
        /// <param name="cmd">the database dependent sql statement</param>
        /// <param name="parameters">the parameters for the sql statement</param>
        /// <returns></returns>
        public T QueryRow<T>(string database, string table, string cmd, params DatabaseParameter[] parameters) where T : struct
        {
            List<T> rows = Query<T>(database, table, cmd, parameters);
            if (rows.Count > 1)
            {
                throw new InvalidDataException(string.Format("Error while reading row data!") + "\n" + string.Format("Additional data available (expected only one row of data)!") + string.Format("\n Database: {0}\n Table: {1}\n Command: {2}", database, table, cmd));
            }

            foreach (T row in rows)
            {
                return row;
            }

            throw new InvalidDataException(string.Format("Error while reading row data!") + "\n" + string.Format("No result dataset available!") + string.Format("\n Database: {0}\n Table: {1}\n Command: {2}", database, table, cmd));
        }

        /// <summary>
        /// Queries for all matching datasets (selected fields, multiple rows) with full field type checks (struct has to match database row)
        /// </summary>
        /// <param name="database">The affected database (dependent on the storage engine this may be null)</param>
        /// <param name="table">The affected table (dependent on the storage engine this may be null)</param>
        /// <param name="cmd">the database dependent sql statement</param>
        /// <param name="parameters">the parameters for the sql statement</param>
        /// <returns></returns>
        public virtual List<T> Query<T>(string database, string table, string cmd, params DatabaseParameter[] parameters) where T : struct
        {
            if (Closed)
            {
                throw new ObjectDisposedException(ToString());
            }

            for (int i = 1; ; i++)
            {
                SqlConnection connection = GetConnection(database);
                //prepare result
                List<T> result = new List<T>();
                //get reader
                bool error = false;
                try
                {
                    //todo find a way to prevent the recreation of this layout instance all the time
                    RowLayout layout = RowLayout.CreateTyped(typeof(T));
                    using (IDbCommand command = CreateCommand(connection, cmd, parameters))
                    using (IDataReader reader = command.ExecuteReader(CommandBehavior.KeyInfo))
                    {
                        if (DoSchemaCheckOnQuery)
                        {
                            //read schema
                            string name = table ?? cmd;
                            RowLayout schema = ReadSchema(reader, name);
                            //check schema
                            CheckLayout(table, schema, layout);
                        }
                        //load rows
                        while (reader.Read())
                        {
                            Row row = ReadRow(layout, reader);
                            result.Add(row.GetStruct<T>(layout));
                        }
                        return result;
                    }
                }
                catch (Exception ex)
                {
                    error = true;
                    if (i > MaxErrorRetries) { throw; }
                    Trace.TraceError("<red>{3}<default> Error during Query(<cyan>{0}<default>, <cyan>{1}<default>) -> <yellow>retry {2}\n{4}", database, table, i, ex.Message, ex);
                }
                finally { ReturnConnection(ref connection, error); }
            }
        }

        #endregion

        #endregion

        #region FQTN Member

        /// <summary>
        /// Obtains a full qualified table name
        /// </summary>
        /// <param name="database">A database name</param>
        /// <param name="table">A table name</param>
        /// <returns></returns>
        public abstract string FQTN(string database, string table);

        #endregion

        /// <summary>
        /// Gets a value indicating whether the storage engine supports native transactions with faster execution than single commands.
        /// </summary>
        /// <value>
        /// <c>true</c> if supports native transactions; otherwise, <c>false</c>.
        /// </value>
        public override bool SupportsNativeTransactions { get; } = true;

        /// <summary>Gets or sets a value indicating whether [unsafe connections are allowed].</summary>
        /// <value>
        /// <c>true</c> if [unsafe connections are allowed]; otherwise, <c>false</c>.
        /// </value>
        public bool AllowUnsafeConnections { get; }

        /// <summary>closes the connection to the storage engine</summary>
        public override void Close()
        {
            Dispose();
        }

        #region IDisposable Support
        private bool disposedValue = false;

        /// <summary>
        /// Releases all resources used by the SqlConnection
        /// </summary>
        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {

                    if (m_Pool != null)
                    {
                        m_Pool.Close();
                        m_Pool = null;
                    }

                }
                disposedValue = true;
            }
        }

        /// <summary>
        /// Releases all resources used by the SqlConnection
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        #endregion
    }
}