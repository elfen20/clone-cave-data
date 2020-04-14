using System;
using System.Linq;

namespace Cave.Data
{
    /// <summary>
    /// Provides a database identifier (alias primary key).
    /// </summary>
    public class Identifier : IEquatable<Identifier>
    {
        readonly object[] data;

        /// <summary>
        /// Initializes a new instance of the <see cref="Identifier"/> class.
        /// </summary>
        /// <param name="layout">Table layout.</param>
        /// <param name="row">Row to create to create identifier for.</param>
        internal Identifier(RowLayout layout, Row row)
        {
            if (!layout.Identifier.Any())
            {
                data = row.Values;
            }
            else
            {
                data = layout.Identifier.Select(field => row[field.Index]).ToArray();
            }
        }

        /// <inheritdoc/>
        public override string ToString() => data.Select(d => $"{d}").Join('|');

        /// <inheritdoc/>
        public override bool Equals(object obj) => obj is Identifier other ? Equals(other) : false;

        /// <inheritdoc/>
        public bool Equals(Identifier other)
        {
            return other.data.SequenceEqual(data);
        }

        /// <inheritdoc/>
        public override int GetHashCode()
        {
            int hash = 0;
            foreach (var item in data)
            {
                hash ^= item?.GetHashCode() ?? 0;
                hash.Rol(1);
            }
            return hash;
        }
    }
}
