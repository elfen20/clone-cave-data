using System;
using System.Data;

namespace Cave.Data.Sql
{
    /// <summary>
    ///     Wraps an <see cref="IDbConnection" /> and allows always to obtain the database name of the connection. This is
    ///     needed for connection reusing at <see cref="SqlStorage" />.
    /// </summary>
    public sealed class SqlConnection : IDbConnection
    {
        IDbConnection connection;

        /// <summary>Initializes a new instance of the <see cref="SqlConnection" /> class.</summary>
        /// <param name="databaseName">Name of the database.</param>
        /// <param name="connection">The connection object.</param>
        public SqlConnection(string databaseName, IDbConnection connection)
        {
            this.connection = connection;
            Database = databaseName;
            LastUsed = DateTime.UtcNow;
        }

        /// <summary>Gets or sets the last use datetime (local).</summary>
        public DateTime LastUsed { get; set; }

        /// <summary>Gets the name of the database this instance is connected to.</summary>
        public string Database { get; private set; }

        /// <inheritdoc />
        public string ConnectionString
        {
            get
            {
                if (connection == null)
                {
                    throw new ObjectDisposedException("SqlConnection");
                }

                return connection.ConnectionString;
            }
            set => throw new NotSupportedException();
        }

        /// <summary>Gets the connection timeout in milliseconds.</summary>
        public int ConnectionTimeout
        {
            get
            {
                if (connection == null)
                {
                    throw new ObjectDisposedException("SqlConnection");
                }

                return connection.ConnectionTimeout;
            }
        }

        /// <summary>Gets the current <see cref="ConnectionState" />.</summary>
        public ConnectionState State => connection == null ? ConnectionState.Closed : connection.State;

        /// <summary>Begins a database transaction with the specified IsolationLevel value.</summary>
        /// <param name="il">One of the IsolationLevel values. </param>
        /// <returns>An object representing the new transaction.</returns>
        public IDbTransaction BeginTransaction(IsolationLevel il)
        {
            if (connection == null)
            {
                throw new ObjectDisposedException("SqlConnection");
            }

            LastUsed = DateTime.UtcNow;
            return connection.BeginTransaction(il);
        }

        /// <summary>Begins a database transaction.</summary>
        /// <returns>An object representing the new transaction.</returns>
        public IDbTransaction BeginTransaction()
        {
            if (connection == null)
            {
                throw new ObjectDisposedException("SqlConnection");
            }

            LastUsed = DateTime.UtcNow;
            return connection.BeginTransaction();
        }

        /// <summary>Changes the current database for an open Connection object.</summary>
        /// <param name="databaseName">The name of the database to use in place of the current database. </param>
        public void ChangeDatabase(string databaseName)
        {
            if (connection == null)
            {
                throw new ObjectDisposedException("SqlConnection");
            }

            connection.ChangeDatabase(databaseName);
            Database = databaseName;
            LastUsed = DateTime.UtcNow;
        }

        /// <summary>Closes the connection to the database.</summary>
        public void Close() { Dispose(); }

        /// <summary>Creates a new IDbCommand for this connection.</summary>
        /// <returns>A new <see cref="IDbCommand" /> instance.</returns>
        public IDbCommand CreateCommand()
        {
            if (connection == null)
            {
                throw new ObjectDisposedException("SqlConnection");
            }

            LastUsed = DateTime.UtcNow;
            return connection.CreateCommand();
        }

        /// <summary>Opens the connection to the database.</summary>
        public void Open()
        {
            if (connection == null)
            {
                throw new ObjectDisposedException(nameof(SqlConnection));
            }

            connection.Open();
            LastUsed = DateTime.UtcNow;
        }

        /// <summary>Disposes the connection.</summary>
        public void Dispose()
        {
            if (connection != null)
            {
                try
                {
                    connection.Dispose();
                }
                catch { }

                connection = null;
            }
        }

        /// <summary>Returns a <see cref="string" /> that represents this instance.</summary>
        /// <returns>A <see cref="string" /> that represents this instance.</returns>
        public override string ToString() =>
            connection == null
                ? "SqlConnection Disposed"
                : $"SqlConnection Database:'{Database}' State:{State}";
    }
}
