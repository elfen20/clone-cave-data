namespace Cave.Data
{
    /// <summary>Provides available transaction types.</summary>
    public enum TransactionType : byte
    {
        /// <summary>Undefined</summary>
        Undefined = 0,

        /// <summary>Entry was deleted</summary>
        Deleted = 1,

        /// <summary>Entry was inserted</summary>
        Inserted = 2,

        /// <summary>Entry was updated</summary>
        Updated = 3,

        /// <summary>Entry was replaced</summary>
        Replaced = 4
    }
}
