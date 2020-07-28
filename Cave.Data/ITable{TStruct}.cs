using System.Collections.Generic;

namespace Cave.Data
{
    /// <summary>Provides an interface for a table of structs (rows).</summary>
    /// <typeparam name="TStruct">Row structure type.</typeparam>
    public interface ITable<TStruct> : ITable
        where TStruct : struct
    {
        /// <summary>Checks a given ID for existance.</summary>
        /// <param name="row">Dataset to check at the table.</param>
        /// <returns>Returns whether the dataset exists or not.</returns>
        bool Exist(TStruct row);

        /// <summary>Removes a row from the table.</summary>
        /// <param name="row">Dataset to delete from the table.</param>
        void Delete(TStruct row);

        /// <summary>Removes rows from the table using a transaction.</summary>
        /// <param name="rows">Datasets to delete from the table.</param>
        void Delete(IEnumerable<TStruct> rows);

        /// <summary>Gets a row from the table.</summary>
        /// <param name="row">Datasets to fetch from the table.</param>
        /// <returns>The row.</returns>
        Row GetRow(TStruct row);

        /// <summary>Gets the rows with the given identifiers.</summary>
        /// <param name="rows">Datasets to fetch from the table.</param>
        /// <returns>The rows.</returns>
        IList<Row> GetRows(IEnumerable<TStruct> rows);

        /// <summary>Searches the table for a single row with given search.</summary>
        /// <param name="row">The row to find.</param>
        /// <returns>The structure found.</returns>
        TStruct GetStruct(TStruct row);

        /// <summary>
        ///     Inserts a row into the table. If an ID &lt;= 0 is given an automatically generated ID will be used to add the
        ///     dataset.
        /// </summary>
        /// <param name="row">
        ///     The row to insert. If an ID &lt;= 0 is given an automatically generated ID will be used to add the
        ///     dataset.
        /// </param>
        /// <returns>The ID of the inserted dataset.</returns>
        TStruct Insert(TStruct row);

        /// <summary>Inserts rows into the table using a transaction.</summary>
        /// <param name="rows">The rows to insert.</param>
        void Insert(IEnumerable<TStruct> rows);

        /// <summary>Updates a row at the table. The row must exist already!.</summary>
        /// <param name="row">The row to update.</param>
        void Update(TStruct row);

        /// <summary>Updates rows at the table. The rows must exist already!.</summary>
        /// <param name="rows">The rows to update.</param>
        void Update(IEnumerable<TStruct> rows);

        /// <summary>
        ///     Replaces a row at the table. The ID has to be given. This inserts (if the row does not exist) or updates (if
        ///     it exists) the row.
        /// </summary>
        /// <param name="row">The row to replace (valid ID needed).</param>
        void Replace(TStruct row);

        /// <summary>Replaces rows at the table. This inserts (if the row does not exist) or updates (if it exists) each row.</summary>
        /// <param name="rows">The rows to replace (valid ID needed).</param>
        void Replace(IEnumerable<TStruct> rows);

        /// <summary>Searches the table for a single row with given search.</summary>
        /// <param name="search">The search to run.</param>
        /// <param name="resultOption">Options for the search and the result set.</param>
        /// <returns>The structure found.</returns>
        TStruct GetStruct(Search search = default, ResultOption resultOption = default);

        /// <summary>Searches the table for rows with given search.</summary>
        /// <param name="search">The search to run.</param>
        /// <param name="resultOption">Options for the search and the result set.</param>
        /// <returns>The rows found.</returns>
        IList<TStruct> GetStructs(Search search = default, ResultOption resultOption = default);

        /// <summary>Gets the rows matching the specified rows (by identifier fields).</summary>
        /// <param name="rows">IDs of the rows to fetch from the table.</param>
        /// <returns>The rows.</returns>
        IList<TStruct> GetStructs(IEnumerable<TStruct> rows);

        /// <summary>
        ///     Gets the row struct with the given index. This allows a memorytable to be used as virtual list for listviews,
        ///     ... Note that indices will change on each update, insert, delete and sorting is not garanteed!.
        /// </summary>
        /// <param name="index">The rows index (0..RowCount-1).</param>
        /// <returns>The row found.</returns>
        TStruct GetStructAt(int index);
    }
}
