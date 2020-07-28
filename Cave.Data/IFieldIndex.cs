using System.Collections.Generic;

namespace Cave.Data
{
    /// <summary>Provides a table field index implementation.</summary>
    public interface IFieldIndex
    {
        /// <summary>Retrieves all identifiers for the specified object.</summary>
        /// <param name="obj">The object.</param>
        /// <returns>All matching rows found.</returns>
        IEnumerable<object[]> Find(object obj);
    }
}
