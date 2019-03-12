using System.Collections.Generic;

namespace Cave.Data
{
    /// <summary>
    /// Provides an interface for tables stored at the memory.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface IMemoryTable<T> : ITable<T>, IMemoryTable
        where T : struct
    {
        /// <summary>
        /// Replaces the whole data at the table with the specified one without writing transactions.
        /// </summary>
        /// <param name="items"></param>
        void SetStructs(IEnumerable<T> items);

        /// <summary>
        /// Inserts a row into the table. If an ID &lt; 0 is specified an automatically generated ID will be used to add the dataset.
        /// </summary>
        /// <param name="row">The row to insert. If an ID &lt;= 0 is specified an automatically generated ID will be used to add the dataset.</param>
        /// <param name="writeTransaction">If true a transaction is generated at the <see cref="TransactionLog"/>.</param>
        /// <returns>Returns the ID of the inserted dataset.</returns>
        long Insert(T row, bool writeTransaction);

        /// <summary>
        /// Replaces a row at the table. The ID has to be given. This inserts (if the row does not exist) or updates (if it exists) the row.
        /// </summary>
        /// <param name="row">The row to replace (valid ID needed).</param>
        /// <param name="writeTransaction"></param>
        void Replace(T row, bool writeTransaction);

        /// <summary>
        /// Updates a row at the table. The row must exist already!.
        /// </summary>
        /// <param name="row">The row to update.</param>
        /// <param name="writeTransaction">If true a transaction is generated at the <see cref="TransactionLog"/>.</param>
        void Update(T row, bool writeTransaction);

        /// <summary>
        /// Inserts rows into the table. If an ID &lt; 0 is specified an automatically generated ID will be used to add the dataset.
        /// </summary>
        /// <param name="rows">The rows.</param>
        /// <param name="writeTransaction">If true a transaction is generated at the <see cref="TransactionLog" />.</param>
        void Insert(IEnumerable<T> rows, bool writeTransaction);

        /// <summary>
        /// Replaces rows at the table. The ID has to be given. This inserts (if the row does not exist) or updates (if it exists) the row.
        /// </summary>
        /// <param name="rows">The rows.</param>
        /// <param name="writeTransaction">if set to <c>true</c> [write transaction].</param>
        void Replace(IEnumerable<T> rows, bool writeTransaction);

        /// <summary>Updates rows at the table. The row must exist already!.</summary>
        /// <param name="rows">The rows.</param>
        /// <param name="writeTransaction">If true a transaction is generated at the <see cref="TransactionLog" />.</param>
        void Update(IEnumerable<T> rows, bool writeTransaction);
    }
}
