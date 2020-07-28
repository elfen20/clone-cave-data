using System;
using System.Collections.Generic;

namespace Cave.Data
{
    /// <summary>Provides extensions to the <see cref="IDatabase" /> interface.</summary>
    public static class IDatabaseExtensions
    {
        /// <summary>Creates a new table with the specified layout.</summary>
        /// <typeparam name="TStruct">Row structure type.</typeparam>
        /// <param name="database">The database to create the table at.</param>
        /// <param name="tableName">The name of the table.</param>
        /// <param name="flags">Flags for table creation.</param>
        /// <param name="excludedFields">Fields at <typeparamref name="TStruct" /> to be excluded.</param>
        /// <returns>Returns an <see cref="ITable{TStruct}" /> instance for the specified table.</returns>
        public static ITable<TStruct> CreateTable<TStruct>(this IDatabase database, string tableName = null, TableFlags flags = default,
            params string[] excludedFields)
            where TStruct : struct
        {
            var layout = RowLayout.CreateTyped(typeof(TStruct), tableName, database.Storage, excludedFields);
            var table = database.CreateTable(layout, flags);
            return new Table<TStruct>(table);
        }

        /// <summary>Reads a whole table into memory.</summary>
        /// <param name="database">The database instance.</param>
        /// <param name="tableName">The name of the table.</param>
        /// <param name="flags">Flags for table.</param>
        /// <param name="excludedFields">Fields at <typeparamref name="TStruct" /> to be excluded.</param>
        /// <returns>Returns a new <see cref="IList{TStruct}" /> instance containing all rows of the table.</returns>
        /// <typeparam name="TStruct">Row structure.</typeparam>
        public static IList<TStruct> ReadTable<TStruct>(this IDatabase database, string tableName = null, TableFlags flags = default,
            params string[] excludedFields)
            where TStruct : struct
        {
            var table = GetTable<TStruct>(database, tableName, flags, excludedFields);
            return table.GetStructs();
        }

        /// <summary>Opens or creates the table with the specified type.</summary>
        /// <typeparam name="TStruct">Row structure type.</typeparam>
        /// <param name="database">The database instance.</param>
        /// <param name="tableName">The name of the table.</param>
        /// <param name="flags">Flags for table loading.</param>
        /// <param name="excludedFields">Fields at <typeparamref name="TStruct" /> to be excluded.</param>
        /// <returns>Returns an <see cref="ITable{TValue}" /> instance for the specified table.</returns>
        public static ITable<TStruct> GetTable<TStruct>(this IDatabase database, string tableName = null, TableFlags flags = default,
            params string[] excludedFields)
            where TStruct : struct
        {
            var layout = RowLayout.CreateTyped(typeof(TStruct), tableName, database.Storage, excludedFields);
            if (flags.HasFlag(TableFlags.IgnoreMissingFields))
            {
                return new Table<TStruct>(database.GetTable(tableName ?? layout.Name));
            }

            var table = database.GetTable(layout, flags);
            return new Table<TStruct>(table);
        }

        /// <summary>Creates a new table with the specified layout.</summary>
        /// <typeparam name="TKey">Key identifier type.</typeparam>
        /// <typeparam name="TStruct">Row structure type.</typeparam>
        /// <param name="database">The database to create the table at.</param>
        /// <param name="tableName">The name of the table.</param>
        /// <param name="flags">Flags for table creation.</param>
        /// <param name="excludedFields">Fields at <typeparamref name="TStruct" /> to be excluded.</param>
        /// <returns>Returns an <see cref="ITable{TKey, TStruct}" /> instance for the specified table.</returns>
        public static ITable<TKey, TStruct> CreateTable<TKey, TStruct>(this IDatabase database, string tableName = null, TableFlags flags = default,
            params string[] excludedFields)
            where TKey : IComparable<TKey>
            where TStruct : struct
        {
            var layout = RowLayout.CreateTyped(typeof(TStruct), tableName, database.Storage, excludedFields);
            var table = database.CreateTable(layout, flags);
            return new Table<TKey, TStruct>(table);
        }

        /// <summary>Reads a whole table into memory.</summary>
        /// <param name="database">The database instance.</param>
        /// <param name="tableName">The name of the table.</param>
        /// <param name="flags">Flags for table.</param>
        /// <param name="excludedFields">Fields at <typeparamref name="TStruct" /> to be excluded.</param>
        /// <returns>Returns a new <see cref="IDictionary{TKey, TValue}" /> instance containing all rows of the table.</returns>
        /// <typeparam name="TKey">Identifier field type.</typeparam>
        /// <typeparam name="TStruct">Row structure.</typeparam>
        public static IDictionary<TKey, TStruct> ReadTable<TKey, TStruct>(this IDatabase database, string tableName = null, TableFlags flags = default,
            params string[] excludedFields)
            where TKey : IComparable<TKey>
            where TStruct : struct
        {
            var table = GetTable<TKey, TStruct>(database, tableName, flags, excludedFields);
            return table.GetDictionary();
        }

        /// <summary>Opens or creates the table with the specified type.</summary>
        /// <typeparam name="TKey">Key identifier type.</typeparam>
        /// <typeparam name="TStruct">Row structure type.</typeparam>
        /// <param name="database">The database instance.</param>
        /// <param name="tableName">The name of the table.</param>
        /// <param name="flags">Flags for table loading.</param>
        /// <param name="excludedFields">Fields at <typeparamref name="TStruct" /> to be excluded.</param>
        /// <returns>Returns an <see cref="ITable{TKey, TValue}" /> instance for the specified table.</returns>
        public static ITable<TKey, TStruct> GetTable<TKey, TStruct>(this IDatabase database, string tableName = null, TableFlags flags = default,
            params string[] excludedFields)
            where TKey : IComparable<TKey>
            where TStruct : struct
        {
            var layout = RowLayout.CreateTyped(typeof(TStruct), tableName, database.Storage, excludedFields);
            if (flags.HasFlag(TableFlags.IgnoreMissingFields))
            {
                return new Table<TKey, TStruct>(database.GetTable(tableName ?? layout.Name));
            }

            var table = database.GetTable(layout, flags);
            return new Table<TKey, TStruct>(table);
        }

        /// <summary>Provides a csharp interface generator.</summary>
        /// <param name="database">The database to generate code for.</param>
        /// <param name="className">The class name for the database interface.</param>
        /// <returns>Returns a new <see cref="DatabaseInterfaceGenerator" /> instance.</returns>
        public static DatabaseInterfaceGenerator GenerateInterface(this IDatabase database, string className = null) =>
            new DatabaseInterfaceGenerator(database, className);
    }
}
