using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Cave.Collections.Generic;

namespace Cave.Data
{
    /// <summary>
    /// Provides a table field index implementation.
    /// </summary>
    public sealed class FieldIndex : IFieldIndex
    {
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

#if USE_BOXING
        /// <summary>
        /// resolves value to IDs
        /// </summary>
        SortedDictionary<BoxedValue, Set<long>> index;
#else
        /// <summary>
        /// resolves value to IDs.
        /// </summary>
        FakeSortedDictionary<object, Set<long>> index;

#pragma warning disable SA1214 // Readonly fields should appear before non-readonly fields
        readonly object nullValue = new BoxedValue(null);
#pragma warning restore SA1214 // Readonly fields should appear before non-readonly fields
#endif

        /// <summary>Gets the id count.</summary>
        /// <value>The id count.</value>
        public int Count { get; private set; }

        /// <summary>Initializes a new instance of the <see cref="FieldIndex"/> class.</summary>
        public FieldIndex()
        {
#if USE_BOXING
            index = new FakeSortedDictionary<BoxedValue, Set<long>>();
#else
            index = new FakeSortedDictionary<object, Set<long>>();
#endif
        }

        /// <summary>
        /// Adds an ID, object combination to the index.
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        internal void Add(long id, object value)
        {
#if USE_BOXING
            BoxedValue obj = new BoxedValue(value);
#else
            var obj = value == null ? nullValue : value;
#endif
            if (index.ContainsKey(obj))
            {
                index[obj].Add(id);
            }
            else
            {
                var list = new Set<long>
                {
                    id,
                };
                index[obj] = list;
            }
            Count++;
        }

        /// <summary>Replaces the object for the specified identifier.</summary>
        /// <param name="id">The identifier.</param>
        /// <param name="oldObj">The old object.</param>
        /// <param name="newObj">The new object.</param>
        /// <exception cref="ArgumentException">
        /// </exception>
        internal void Replace(long id, object oldObj, object newObj)
        {
            if (Comparer.Default.Compare(oldObj, newObj) == 0)
            {
                return;
            }

            Delete(id, oldObj);
            Add(id, newObj);
        }

        /// <summary>Removes an ID from the index.</summary>
        /// <param name="id">The identifier.</param>
        /// <param name="value">The value.</param>
        /// <exception cref="ArgumentException">
        /// </exception>
        internal void Delete(long id, object value)
        {
#if USE_BOXING
            BoxedValue obj = new BoxedValue(value);
#else
            var obj = value == null ? nullValue : value;
#endif

            // remove ID from old hash
            if (!index.TryGetValue(obj, out Set<long> ids))
            {
                throw new ArgumentException(string.Format("Object {0} is not present at index (equals check {1})!", obj, index.Join(",")));
            }
            if (!ids.Contains(id))
            {
                throw new KeyNotFoundException(string.Format("ID {0} is not present at index! (Present: {1} => {2})", id, obj, ids.Join(",")));
            }

            if (ids.Count > 1)
            {
                ids.Remove(id);
            }
            else
            {
                if (!index.Remove(obj))
                {
                    throw new ArgumentException(string.Format("Could not remove id {0} object {1} could not be removed!", id, obj));
                }
            }
            Count--;
        }

        /// <summary>
        /// Clears the index.
        /// </summary>
        internal void Clear()
        {
            index.Clear();
            Count = 0;
        }

        /// <summary>Obtains all IDs with the specified hashcode.</summary>
        /// <param name="value">The value.</param>
        /// <returns></returns>
        public IItemSet<long> Find(object value)
        {
#if USE_BOXING
            BoxedValue obj = new BoxedValue(value);
#else
            var obj = value == null ? nullValue : value;
#endif
            return index.ContainsKey(obj) ? new ReadOnlySet<long>(index[obj]) : (IItemSet<long>)new Set<long>();
        }

        /// <summary>Gets the sorted identifiers.</summary>
        /// <value>The sorted identifiers.</value>
        public IEnumerable<long> SortedIDs =>
#if USE_BOXING
                return new FieldIndexEnumeration<BoxedValue>(m_Index);
#else
                new FieldIndexEnumeration<object>(index);
#endif

        class FieldIndexEnumeration<T> : IEnumerable<long>
        {
            FakeSortedDictionary<T, Set<long>> idx;

            public FieldIndexEnumeration(FakeSortedDictionary<T, Set<long>> idx)
            {
                this.idx = idx;
            }

            public IEnumerator<long> GetEnumerator()
            {
                return new FieldIndexEnumerator<T>(idx);
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return new FieldIndexEnumerator<T>(idx);
            }
        }

        class FieldIndexEnumerator<T> : IEnumerator<long>
        {
            FakeSortedDictionary<T, Set<long>> idx;
            IEnumerator outer;
            IEnumerator inner;

            public FieldIndexEnumerator(FakeSortedDictionary<T, Set<long>> idx)
            {
                this.idx = idx;
                Reset();
            }

            public long Current => (long)inner.Current;

            object IEnumerator.Current => inner.Current;

            public void Dispose()
            {
            }

            public bool MoveNext()
            {
                if (inner != null)
                {
                    if (inner.MoveNext())
                    {
                        return true;
                    }
                }
                while (outer.MoveNext())
                {
                    var keyValuePair = (KeyValuePair<T, Set<long>>)outer.Current;
                    inner = keyValuePair.Value.GetEnumerator();
                    if (inner.MoveNext())
                    {
                        return true;
                    }
                }
                return false;
            }

            public void Reset()
            {
                outer = idx.GetEnumerator();
                outer.Reset();
                inner = null;
            }
        }
    }
}
