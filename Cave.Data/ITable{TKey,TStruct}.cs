using System;
using System.Collections.Generic;

namespace Cave.Data
{
    /// <summary>
    /// Provides an interface for a table of structs (rows).
    /// </summary>
    /// <typeparam name="TKey">Key identifier type.</typeparam>
    /// <typeparam name="TStruct">Row structure type.</typeparam>
    public interface ITable<TKey, TStruct> : ITable
        where TKey : IComparable<TKey>
        where TStruct : struct
    {
        /// <summary>
        /// Gets the row with the specified ID.
        /// </summary>
        /// <param name="id">The identifier.</param>
        /// <returns>The row found.</returns>
        TStruct this[TKey id] { get; }

        /// <summary>
        /// Checks a given ID for existance.
        /// </summary>
        /// <param name="id">The dataset ID to look for.</param>
        /// <returns>Returns whether the dataset exists or not.</returns>
        bool Exist(TKey id);

        /// <summary>
        /// Removes a row from the table.
        /// </summary>
        /// <param name="id">The dataset ID to remove.</param>
        void Delete(TKey id);

        /// <summary>
        /// Removes rows from the table using a transaction.
        /// </summary>
        /// <param name="ids">The dataset IDs to remove.</param>
        void Delete(IEnumerable<TKey> ids);

        /// <summary>
        /// Gets a row from the table.
        /// </summary>
        /// <param name="id">The ID of the row to be fetched.</param>
        /// <returns>Returns the row.</returns>
        Row GetRow(TKey id);

        /// <summary>
        /// Gets the rows with the given identifiers.
        /// </summary>
        /// <param name="ids">Identifiers of the rows to fetch from the table.</param>
        /// <returns>Returns the rows.</returns>
        IList<Row> GetRows(IEnumerable<TKey> ids);

        /// <summary>
        /// Gets a row from the table.
        /// </summary>
        /// <param name="id">The ID of the row to be fetched.</param>
        /// <returns>Returns the row.</returns>
        TStruct GetStruct(TKey id);

        /// <summary>
        /// Inserts a row into the table. If an ID &lt;= 0 is given an automatically generated ID will be used to add the dataset.
        /// </summary>
        /// <param name="row">The row to insert. If an ID &lt;= 0 is given an automatically generated ID will be used to add the dataset.</param>
        /// <returns>Returns the ID of the inserted dataset.</returns>
        TStruct Insert(TStruct row);

        /// <summary>
        /// Inserts rows into the table using a transaction.
        /// </summary>
        /// <param name="rows">The rows to insert.</param>
        void Insert(IEnumerable<TStruct> rows);

        /// <summary>
        /// Updates a row at the table. The row must exist already!.
        /// </summary>
        /// <param name="row">The row to update.</param>
        void Update(TStruct row);

        /// <summary>
        /// Updates rows at the table. The rows must exist already!.
        /// </summary>
        /// <param name="rows">The rows to update.</param>
        void Update(IEnumerable<TStruct> rows);

        /// <summary>
        /// Replaces a row at the table. The ID has to be given. This inserts (if the row does not exist) or updates (if it exists) the row.
        /// </summary>
        /// <param name="row">The row to replace (valid ID needed).</param>
        void Replace(TStruct row);

        /// <summary>
        /// Replaces rows at the table. This inserts (if the row does not exist) or updates (if it exists) each row.
        /// </summary>
        /// <param name="rows">The rows to replace (valid ID needed).</param>
        void Replace(IEnumerable<TStruct> rows);

        /// <summary>
        /// Searches the table for a single row with given search.
        /// </summary>
        /// <param name="search">The search to run.</param>
        /// <param name="resultOption">Options for the search and the result set.</param>
        /// <returns>Returns the structure found.</returns>
        TStruct GetStruct(Search search = default, ResultOption resultOption = default);

        /// <summary>
        /// Searches the table for rows with given search.
        /// </summary>
        /// <param name="search">The search to run.</param>
        /// <param name="resultOption">Options for the search and the result set.</param>
        /// <returns>Returns the rows found.</returns>
        IList<TStruct> GetStructs(Search search = default, ResultOption resultOption = default);

        /// <summary>
        /// Gets the rows with the given ids.
        /// </summary>
        /// <param name="ids">IDs of the rows to fetch from the table.</param>
        /// <returns>Returns the rows.</returns>
        IList<TStruct> GetStructs(IEnumerable<TKey> ids);

        /// <summary>
        /// Gets the row struct with the given index.
        /// This allows a memorytable to be used as virtual list for listviews, ...
        /// Note that indices will change on each update, insert, delete and sorting is not garanteed!.
        /// </summary>
        /// <param name="index">The rows index (0..RowCount-1).</param>
        /// <returns>The row found.</returns>
        TStruct GetStructAt(int index);

        /// <summary>
        /// Gets a dictionary with all datasets.
        /// </summary>
        /// <param name="search">The search to run.</param>
        /// <param name="resultOption">Options for the search and the result set.</param>
        /// <returns>A readonly dictionary.</returns>
        IDictionary<TKey, TStruct> GetDictionary(Search search = null, ResultOption resultOption = null);
    }
}
