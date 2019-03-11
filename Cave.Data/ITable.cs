using System.Collections.Generic;
using Cave.Collections.Generic;

namespace Cave.Data
{
    /// <summary>
    /// Provides an interface for a table of structs (rows).
    /// </summary>
    public interface ITable
    {
        /// <summary>Gets the sequence number (counting write commands on this table).</summary>
        /// <value>The sequence number.</value>
        int SequenceNumber { get; }

        /// <summary>
        /// The storage engine the database belongs to.
        /// </summary>
        IStorage Storage { get; }

        /// <summary>
        /// Obtains the database the table belongs to.
        /// </summary>
        IDatabase Database { get; }

        /// <summary>
        /// Obtains the name of the table.
        /// </summary>
        string Name { get; }

        /// <summary>
        /// Obtains the RowLayout of the table.
        /// </summary>
        RowLayout Layout { get; }

        /// <summary>
        /// Obtains the RowCount.
        /// </summary>
        long RowCount { get; }

        /// <summary>
        /// Counts the results of a given search.
        /// </summary>
        /// <param name="search">The search to run.</param>
        /// <param name="resultOption">Options for the search and the result set.</param>
        /// <returns>Returns the number of rows found matching the criteria given.</returns>
        long Count(Search search = default(Search), ResultOption resultOption = default(ResultOption));

        /// <summary>
        /// Clears all rows of the table.
        /// </summary>
        void Clear();

        /// <summary>
        /// Obtains a row from the table.
        /// </summary>
        /// <param name="id">The ID of the row to be fetched.</param>
        /// <returns>Returns the row.</returns>
        Row GetRow(long id);

        /// <summary>
        /// This function does a lookup on the ids of the table and returns the row with the n-th ID where n is the given index.
        /// Note that indices may change on each update, insert, delete and sorting is not garanteed!.
        /// <param name="index">The index of the row to be fetched</param>
        /// </summary>
        /// <returns>Returns the row.</returns>
        Row GetRowAt(int index);

        /// <summary>
        /// Sets the specified value to the specified fieldname on all rows.
        /// </summary>
        /// <param name="field">The fields name.</param>
        /// <param name="value">The value to set.</param>
        void SetValue(string field, object value);

        /// <summary>
        /// Checks a given ID for existance.
        /// </summary>
        /// <param name="id">The dataset ID to look for.</param>
        /// <returns>Returns whether the dataset exists or not.</returns>
        bool Exist(long id);

        /// <summary>
        /// Checks a given search for any datasets matching.
        /// </summary>
        bool Exist(Search search);

        /// <summary>
        /// Inserts a row into the table. If an ID &lt;= 0 is given an automatically generated ID will be used to add the dataset.
        /// </summary>
        /// <param name="row">The row to insert. If an ID &lt;= 0 is given an automatically generated ID will be used to add the dataset.</param>
        /// <returns>Returns the ID of the inserted dataset.</returns>
        long Insert(Row row);

        /// <summary>
        /// Inserts rows into the table using a transaction. 
        /// </summary>
        /// <param name="rows">The rows to insert.</param>
        void Insert(IEnumerable<Row> rows);

        /// <summary>
        /// Updates a row at the table. The row must exist already!.
        /// </summary>
        /// <param name="row">The row to update.</param>
        /// <returns>Returns the ID of the dataset.</returns>
        void Update(Row row);

        /// <summary>
        /// Updates rows at the table using a transaction. 
        /// </summary>
        /// <param name="rows">The rows to insert.</param>
        void Update(IEnumerable<Row> rows);

        /// <summary>
        /// Removes a row from the table.
        /// </summary>
        /// <param name="id">The dataset ID to remove.</param>
        void Delete(long id);

        /// <summary>
        /// Removes rows from the table using a transaction. 
        /// </summary>
        /// <param name="ids">The dataset IDs to remove.</param>
        void Delete(IEnumerable<long> ids);

        /// <summary>
        /// Removes all rows from the table matching the specified search.
        /// </summary>
        /// <param name="search">The Search used to identify rows for removal.</param>
		/// <returns>Returns the number of dataset deleted.</returns>
        int TryDelete(Search search);

        /// <summary>
        /// Replaces a row at the table. The ID has to be given. This inserts (if the row does not exist) or updates (if it exists) the row.
        /// </summary>
        /// <param name="row">The row to replace (valid ID needed).</param>
        void Replace(Row row);

        /// <summary>
        /// Replaces rows at the table. This inserts (if the row does not exist) or updates (if it exists) each row.
        /// </summary>
        /// <param name="rows">The rows to replace (valid ID needed).</param>
        void Replace(IEnumerable<Row> rows);

        /// <summary>
        /// Searches the table for a row with given field value combinations.
        /// </summary>
        /// <param name="search">The search to run.</param>
        /// <param name="resultOption">Options for the search and the result set.</param>
        /// <returns>Returns the ID of the row found or -1.</returns>
        long FindRow(Search search = default(Search), ResultOption resultOption = default(ResultOption));

        /// <summary>
        /// Searches the table for rows with given field value combinations.
        /// </summary>
        /// <param name="search">The search to run.</param>
        /// <param name="resultOption">Options for the search and the result set.</param>
        /// <returns>Returns the IDs of the rows found.</returns>
        List<long> FindRows(Search search = default(Search), ResultOption resultOption = default(ResultOption));

        /// <summary>
        /// Searches the table for a single row with given search.
        /// </summary>
        /// <param name="search">The search to run.</param>
        /// <param name="resultOption">Options for the search and the result set.</param>
        /// <returns>Returns the row found.</returns>
        Row GetRow(Search search = default(Search), ResultOption resultOption = default(ResultOption));

        /// <summary>
        /// Searches the table for rows with given search.
        /// </summary>
        /// <param name="search">The search to run.</param>
        /// <param name="resultOption">Options for the search and the result set.</param>
        /// <returns>Returns the rows found.</returns>
        List<Row> GetRows(Search search = default(Search), ResultOption resultOption = default(ResultOption));

        /// <summary>
        /// Obtains the rows with the given ids.
        /// </summary>
        /// <param name="ids">IDs of the rows to fetch from the table.</param>
        /// <returns>Returns the rows.</returns>
        List<Row> GetRows(IEnumerable<long> ids);

        /// <summary>
        /// Obtains an array with all rows.
        /// </summary>
        /// <returns></returns>
        List<Row> GetRows();

        /// <summary>Calculates the sum of the specified field name for all matching rows.</summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="search">The search.</param>
        /// <returns></returns>
        double Sum(string fieldName, Search search = null);

        /// <summary>Gets all currently used IDs.</summary>
        /// <value>The IDs.</value>
        List<long> IDs { get; }

        /// <summary>
        /// Obtains the next used ID at the table (positive values are valid, negative ones are invalid, 0 is not defined!).
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        long GetNextUsedID(long id);

        /// <summary>
        /// Obtains the next free ID at the table.
        /// </summary>
        /// <returns></returns>
        long GetNextFreeID();

        /// <summary>Obtains all different field values of a given field.</summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="field">The field.</param>
        /// <param name="includeNull">allow null value to be added to the results.</param>
        /// <param name="ids">The ids to check or null for any.</param>
        /// <returns></returns>
        IItemSet<T> GetValues<T>(string field, bool includeNull = false, IEnumerable<long> ids = null);

        /// <summary>Commits a whole TransactionLog to the table.</summary>
        /// <param name="transactions">The transaction log to read.</param>
        /// <param name="flags">The flags to use.</param>
        /// <param name="count">Number of transactions to combine at one write.</param>
        /// <returns>Returns the number of transactions done or -1 if unknown.</returns>
        int Commit(TransactionLog transactions, TransactionFlags flags = TransactionFlags.Default, int count = -1);
    }

    /// <summary>
    /// Provides an interface for a table of structs (rows).
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface ITable<T> : ITable
        where T : struct
    {
        /// <summary>
        /// Obtains a row from the table.
        /// </summary>
        /// <param name="id">The ID of the row to be fetched.</param>
        /// <returns>Returns the row.</returns>
        T GetStruct(long id);

        /// <summary>
        /// Inserts a row into the table. If an ID &lt;= 0 is given an automatically generated ID will be used to add the dataset.
        /// </summary>
        /// <param name="row">The row to insert. If an ID &lt;= 0 is given an automatically generated ID will be used to add the dataset.</param>
        /// <returns>Returns the ID of the inserted dataset.</returns>
        long Insert(T row);

        /// <summary>
        /// Inserts rows into the table using a transaction. 
        /// </summary>
        /// <param name="rows">The rows to insert.</param>
        void Insert(IEnumerable<T> rows);

        /// <summary>
        /// Updates a row at the table. The row must exist already!.
        /// </summary>
        /// <param name="row">The row to update.</param>
        void Update(T row);

        /// <summary>
        /// Updates rows at the table. The rows must exist already!.
        /// </summary>
        /// <param name="rows">The rows to update.</param>
        void Update(IEnumerable<T> rows);

        /// <summary>
        /// Replaces a row at the table. The ID has to be given. This inserts (if the row does not exist) or updates (if it exists) the row.
        /// </summary>
        /// <param name="row">The row to replace (valid ID needed).</param>
        void Replace(T row);

        /// <summary>
        /// Replaces rows at the table. This inserts (if the row does not exist) or updates (if it exists) each row.
        /// </summary>
        /// <param name="rows">The rows to replace (valid ID needed).</param>
        void Replace(IEnumerable<T> rows);

        /// <summary>
        /// Searches the table for a single row with given search.
        /// </summary>
        /// <param name="search">The search to run.</param>
        /// <param name="resultOption">Options for the search and the result set.</param>
        /// <returns>Returns the row found.</returns>
        T GetStruct(Search search = default(Search), ResultOption resultOption = default(ResultOption));

        /// <summary>
        /// Searches the table for rows with given search.
        /// </summary>
        /// <param name="search">The search to run.</param>
        /// <param name="resultOption">Options for the search and the result set.</param>
        /// <returns>Returns the rows found.</returns>
        List<T> GetStructs(Search search = default(Search), ResultOption resultOption = default(ResultOption));

        /// <summary>
        /// Obtains the rows with the given ids.
        /// </summary>
        /// <param name="ids">IDs of the rows to fetch from the table.</param>
        /// <returns>Returns the rows.</returns>
        List<T> GetStructs(IEnumerable<long> ids);

        /// <summary>
        /// Obtains the row struct with the given index. 
        /// This allows a memorytable to be used as virtual list for listviews, ...
        /// Note that indices will change on each update, insert, delete and sorting is not garanteed!.
        /// </summary>
        /// <param name="index">The rows index (0..RowCount-1).</param>
        /// <returns></returns>
        T GetStructAt(int index);

        /// <summary>
        /// Obtains the row with the specified ID.
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        T this[long id] { get; }
    }
}
