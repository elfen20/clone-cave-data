using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Cave.Data
{
    /// <summary>
    /// Provides a table field index implementation.
    /// </summary>
    public sealed class FieldIndex : IFieldIndex
    {
#if USE_BOXING
        /// <summary>
        /// resolves value to IDs
        /// </summary>
        SortedDictionary<BoxedValue, Set<string>> index;
#else
        /// <summary>
        /// resolves value to IDs.
        /// </summary>
        FakeSortedDictionary<object, List<object[]>> index;

#pragma warning disable SA1214 // Readonly fields should appear before non-readonly fields
        readonly object nullValue = new BoxedValue(null);
#pragma warning restore SA1214 // Readonly fields should appear before non-readonly fields
#endif

        /// <summary>Initializes a new instance of the <see cref="FieldIndex"/> class.</summary>
        public FieldIndex()
        {
#if USE_BOXING
            index = new FakeSortedDictionary<BoxedValue, List<object[]>>();
#else
            index = new FakeSortedDictionary<object, List<object[]>>();
#endif
        }

        /// <summary>Gets the id count.</summary>
        /// <value>The id count.</value>
        public int Count { get; private set; }

        /// <summary>Obtains all IDs with the specified hashcode.</summary>
        /// <param name="value">The value.</param>
        /// <returns>Returns the rows.</returns>
        public IEnumerable<object[]> Find(object value)
        {
#if USE_BOXING
            BoxedValue obj = new BoxedValue(value);
#else
            var obj = value == null ? nullValue : value;
#endif
            return index.ContainsKey(obj) ? index[obj].ToArray() : new object[][] { };
        }

        /// <summary>
        /// Clears the index.
        /// </summary>
        internal void Clear()
        {
            index.Clear();
            Count = 0;
        }

        /// <summary>
        /// Adds an ID, object combination to the index.
        /// </summary>
        internal void Add(object[] row, int fieldNumber)
        {
            var value = row.GetValue(fieldNumber);
#if USE_BOXING
            BoxedValue obj = new BoxedValue(value);
#else
            var obj = value == null ? nullValue : value;
#endif
            if (index.ContainsKey(obj))
            {
                index[obj].Add(row);
            }
            else
            {
                index[obj] = new List<object[]> { row, };
            }
            Count++;
        }

        /// <summary>Replaces a row at the index.</summary>
        /// <param name="oldRow">Row to remove.</param>
        /// <param name="newRow">Row to add.</param>
        /// <param name="fieldNumber">Fieldnumber.</param>
        /// <exception cref="ArgumentException">Value {value} is not present at index (equals check {index})!
        /// or
        /// Row {row} is not present at index! (Present: {value} => {rows})!
        /// or
        /// Could not remove row {row} value {value}!.
        /// </exception>
        internal void Replace(object[] oldRow, object[] newRow, int fieldNumber)
        {
            if (Equals(oldRow, newRow))
            {
                return;
            }

            Delete(oldRow, fieldNumber);
            Add(newRow, fieldNumber);
        }

        /// <summary>Removes a row from the index.</summary>
        /// <param name="row">The row.</param>
        /// <param name="fieldNumber">The fieldnumber.</param>
        /// <exception cref="ArgumentException">Value {value} is not present at index (equals check {index})!
        /// or
        /// Row {row} is not present at index! (Present: {value} => {rows})!
        /// or
        /// Could not remove row {row} value {value}!.
        /// </exception>
        internal void Delete(object[] row, int fieldNumber)
        {
            var value = row.GetValue(fieldNumber);
#if USE_BOXING
            BoxedValue obj = new BoxedValue(value);
#else
            var obj = value == null ? nullValue : value;
#endif

            // remove ID from old hash
            if (!index.TryGetValue(obj, out List<object[]> rows))
            {
                throw new ArgumentException($"Value {value} is not present at index (equals check {index.Join(",")})!");
            }

            int i = GetRowIndex(rows, row);
            if (i < 0)
            {
                throw new KeyNotFoundException($"Row {row} is not present at index! (Present: {value} => {rows.Join(",")})!");
            }

            if (rows.Count > 1)
            {
                rows.RemoveAt(i);
            }
            else
            {
                if (!index.Remove(obj))
                {
                    throw new ArgumentException($"Could not remove row {row} value {value}!");
                }
            }
            Count--;
        }

        int GetRowIndex(IList<object[]> rows, object[] row)
        {
            for (int i = 0; i < rows.Count; i++)
            {
                if (rows[i].SequenceEqual(row))
                {
                    return i;
                }
            }
            return -1;
        }

        class BoxedValue : IComparable<BoxedValue>, IEquatable<BoxedValue>, IComparable
        {
            object val;

            public BoxedValue(object value)
            {
                val = value;
            }

            public override int GetHashCode()
            {
                return val == null ? 0 : val.GetHashCode();
            }

            public override bool Equals(object obj)
            {
                return obj is BoxedValue ? Equals((BoxedValue)obj) : Equals(obj, val);
            }

            public int CompareTo(BoxedValue other)
            {
                return Comparer.Default.Compare(val, other.val);
            }

            public bool Equals(BoxedValue other)
            {
                return Equals(other.val, val);
            }

            public override string ToString()
            {
                return val == null ? "<null>" : val.ToString();
            }

            public int CompareTo(object obj)
            {
                return obj is BoxedValue ? CompareTo((BoxedValue)obj) : Comparer.Default.Compare(val, obj);
            }
        }
    }
}
