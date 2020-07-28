using System;
using System.Collections.Generic;

namespace Cave.Data
{
    /// <summary>Provides an interface for system independent database connections.</summary>
    public interface IDatabase
    {
        /// <summary>Gets the table name comparison type.</summary>
        StringComparison TableNameComparison { get; }

        /// <summary>Gets a value indicating whether this instance is using a secure connection to the storage.</summary>
        /// <value><c>true</c> if this instance is using a secure connection; otherwise, <c>false</c>.</value>
        bool IsSecure { get; }

        /// <summary>Gets the storage engine the database belongs to.</summary>
        IStorage Storage { get; }

        /// <summary>Gets the name of the database.</summary>
        string Name { get; }

        /// <summary>Gets the available table names.</summary>
        IList<string> TableNames { get; }

        /// <summary>Gets a value indicating whether the database was already closed or not.</summary>
        bool IsClosed { get; }

        /// <summary>Gets the table with the specified name.</summary>
        /// <param name="tableName">The name of the table.</param>
        /// <returns>Returns an <see cref="ITable" /> instance for the specified table.</returns>
        ITable this[string tableName] { get; }

        /// <summary>Gets a value indicating whether the specified table exists or not.</summary>
        /// <param name="tableName">The name of the table.</param>
        /// <returns>True if the table exists, false otherwise.</returns>
        bool HasTable(string tableName);

        #region CreateTable functions

        /// <summary>Creates a new table with the specified layout.</summary>
        /// <param name="layout">Layout of the table.</param>
        /// <param name="flags">The table creation flags.</param>
        /// <returns>Returns an <see cref="ITable" /> instance for the specified table.</returns>
        ITable CreateTable(RowLayout layout, TableFlags flags = default);

        #endregion

        /// <summary>Removes a table from the database.</summary>
        /// <param name="tableName">The name of the table.</param>
        void DeleteTable(string tableName);

        /// <summary>Closes the database.</summary>
        void Close();

        #region GetTable functions

        /// <summary>Gets the table with the specified name.</summary>
        /// <param name="tableName">The name of the table.</param>
        /// <param name="flags">Flags used during table loading.</param>
        /// <returns>Returns an <see cref="ITable" /> instance for the specified table.</returns>
        ITable GetTable(string tableName, TableFlags flags = default);

        /// <summary>Opens or creates the table with the specified layout.</summary>
        /// <param name="layout">Layout of the table.</param>
        /// <param name="flags">Flags used during table loading.</param>
        /// <returns>Returns an <see cref="ITable" /> instance for the specified table.</returns>
        ITable GetTable(RowLayout layout, TableFlags flags = default);

        #endregion
    }
}
