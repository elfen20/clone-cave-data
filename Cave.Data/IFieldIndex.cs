using System.Collections.Generic;
using Cave.Collections.Generic;

namespace Cave.Data
{
    /// <summary>
    /// Provides a table field index implementation
    /// </summary>
    public interface IFieldIndex
    {
        /// <summary>Retrieves all identifiers for the specified object.</summary>
        /// <param name="obj">The object.</param>
        /// <returns></returns>
        IItemSet<long> Find(object obj);

        /// <summary>Gets the sorted identifiers.</summary>
        /// <value>The sorted identifiers.</value>
        IEnumerable<long> SortedIDs { get; }
    }
}
