using System;
using System.Data;

namespace Cave.Data.Sql
{
    /// <summary>
    /// Wraps an <see cref="IDbConnection"/> and allows always to obtain the database name of the connection.
    /// This is needed for connection reusing at <see cref="SqlStorage"/>.
    /// </summary>
    public sealed class SqlConnection : IDbConnection
    {
        IDbConnection m_Connection;

        /// <summary>
        /// Gets/sets the last use datetime (local).
        /// </summary>
        public DateTime LastUsed { get; set; }

        /// <summary>Creates a new SqlConnection instance.</summary>
        /// <param name="databaseName">Name of the database.</param>
        /// <param name="connection">The connection object.</param>
        public SqlConnection(string databaseName, IDbConnection connection)
        {
            m_Connection = connection;
            Database = databaseName;
            LastUsed = DateTime.UtcNow;
        }

        /// <summary>
        /// Begins a database transaction with the specified IsolationLevel value.
        /// </summary>
        /// <param name="il">One of the IsolationLevel values. </param>
        /// <returns>An object representing the new transaction.</returns>
        public IDbTransaction BeginTransaction(IsolationLevel il)
        {
            if (m_Connection == null)
            {
                throw new ObjectDisposedException("SqlConnection");
            }

            LastUsed = DateTime.UtcNow;
            return m_Connection.BeginTransaction(il);
        }

        /// <summary>
        /// Begins a database transaction.
        /// </summary>
        /// <returns>An object representing the new transaction.</returns>
        public IDbTransaction BeginTransaction()
        {
            if (m_Connection == null)
            {
                throw new ObjectDisposedException("SqlConnection");
            }

            LastUsed = DateTime.UtcNow;
            return m_Connection.BeginTransaction();
        }

        /// <summary>
        /// Changes the current database for an open Connection object.
        /// </summary>
        /// <param name="databaseName">The name of the database to use in place of the current database. </param>
        public void ChangeDatabase(string databaseName)
        {
            if (m_Connection == null)
            {
                throw new ObjectDisposedException("SqlConnection");
            }

            m_Connection.ChangeDatabase(databaseName);
            Database = databaseName;
            LastUsed = DateTime.UtcNow;
        }

        /// <summary>
        /// Closes the connection to the database.
        /// </summary>
        public void Close()
        {
            lock (this)
            {
                if (m_Connection != null)
                {
                    try { m_Connection.Dispose(); } catch { }
                    m_Connection = null;
                }
            }
        }

        /// <summary>
        /// Obtains the connection string used to open the connection. Setting this value is not supported!.
        /// </summary>
        public string ConnectionString
        {
            get
            {
                if (m_Connection == null)
                {
                    throw new ObjectDisposedException("SqlConnection");
                }

                return m_Connection.ConnectionString;
            }
            set => throw new NotSupportedException();
        }

        /// <summary>
        /// Obtains the connection timeout in milliseconds.
        /// </summary>
        public int ConnectionTimeout
        {
            get
            {
                if (m_Connection == null)
                {
                    throw new ObjectDisposedException("SqlConnection");
                }

                return m_Connection.ConnectionTimeout;
            }
        }

        /// <summary>
        /// Creates a new IDbCommand for this connection.
        /// </summary>
        /// <returns></returns>
        public IDbCommand CreateCommand()
        {
            if (m_Connection == null)
            {
                throw new ObjectDisposedException("SqlConnection");
            }

            LastUsed = DateTime.UtcNow;
            return m_Connection.CreateCommand();
        }

        /// <summary>
        /// Obtains the name of the database this instance is connected to.
        /// </summary>
        public string Database { get; private set; }

        /// <summary>
        /// Opens the connection to the database.
        /// </summary>
        public void Open()
        {
            if (m_Connection == null)
            {
                throw new ObjectDisposedException("SqlConnection");
            }

            m_Connection.Open();
            LastUsed = DateTime.UtcNow;
        }

        /// <summary>
        /// Obtains the current <see cref="ConnectionState"/>.
        /// </summary>
        public ConnectionState State
        {
            get
            {
                return m_Connection == null ? ConnectionState.Closed : m_Connection.State;
            }
        }

        /// <summary>Returns a <see cref="string" /> that represents this instance.</summary>
        /// <returns>A <see cref="string" /> that represents this instance.</returns>
        public override string ToString()
        {
            return m_Connection == null
                ? "SqlConnection Disposed"
                : string.Format("SqlConnection Database:'{0}' State:{1}", Database, State);
        }

        /// <summary>
        /// Disposes the connection (use <see cref="Close"/> first!).
        /// </summary>
        public void Dispose()
        {
            Close();
        }
    }
}
