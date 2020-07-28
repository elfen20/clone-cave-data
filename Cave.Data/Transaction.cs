using System;

namespace Cave.Data
{
    /// <summary>Proivides transactions for database rows.</summary>
    public sealed class Transaction
    {
        Transaction(TransactionType type, Row row)
        {
            Type = type;
            Row = row;
        }

        /// <summary>Gets the <see cref="TransactionType" />.</summary>
        public TransactionType Type { get; }

        /// <summary>
        ///     Gets the full row data (only set on <see cref="TransactionType.Updated" /> and
        ///     <see cref="TransactionType.Inserted" />).
        /// </summary>
        public Row Row { get; }

        /// <summary>Gets the created date time.</summary>
        /// <value>The created date time.</value>
        public DateTime Created { get; } = DateTime.UtcNow;

        /// <summary>Creates a new "deleted row" transaction.</summary>
        /// <param name="row">Row data.</param>
        /// <returns>A new transaction.</returns>
        public static Transaction Delete(Row row) => new Transaction(TransactionType.Deleted, row);

        /// <summary>Creates a new "inserted row" transaction using the specified ID of the inserted row.</summary>
        /// <param name="row">Row data.</param>
        /// <returns>A new transaction.</returns>
        public static Transaction Insert(Row row) => new Transaction(TransactionType.Inserted, row);

        /// <summary>Creates a new "inserted row" transaction.</summary>
        /// <param name="row">Row data.</param>
        /// <returns>A new transaction.</returns>
        public static Transaction Replace(Row row) => new Transaction(TransactionType.Replaced, row);

        /// <summary>Creates a new "updated row" transaction.</summary>
        /// <param name="row">Row data.</param>
        /// <returns>A new transaction.</returns>
        public static Transaction Updated(Row row) => new Transaction(TransactionType.Updated, row);

        /// <summary>Provides a string for this instance.</summary>
        /// <returns>{Type} {Row}.</returns>
        public override string ToString() => $"{Type} {Row}";

        /// <inheritdoc />
        public override int GetHashCode() => Type.GetHashCode() ^ Row.GetHashCode();
    }
}
