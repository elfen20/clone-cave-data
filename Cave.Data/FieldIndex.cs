#region CopyRight 2018
/*
    Copyright (c) 2005-2018 Andreas Rohleder (andreas@rohleder.cc)
    All rights reserved
*/
#endregion
#region License LGPL-3
/*
    This program/library/sourcecode is free software; you can redistribute it
    and/or modify it under the terms of the GNU Lesser General Public License
    version 3 as published by the Free Software Foundation subsequent called
    the License.

    You may not use this program/library/sourcecode except in compliance
    with the License. The License is included in the LICENSE file
    found at the installation directory or the distribution package.

    Permission is hereby granted, free of charge, to any person obtaining
    a copy of this software and associated documentation files (the
    "Software"), to deal in the Software without restriction, including
    without limitation the rights to use, copy, modify, merge, publish,
    distribute, sublicense, and/or sell copies of the Software, and to
    permit persons to whom the Software is furnished to do so, subject to
    the following conditions:

    The above copyright notice and this permission notice shall be included
    in all copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
    EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
    MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
    NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
    LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
    OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
    WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/
#endregion License
#region Authors & Contributors
/*
   Author:
     Andreas Rohleder <andreas@rohleder.cc>

   Contributors:
 */
#endregion Authors & Contributors

using Cave.Collections.Generic;
using System;
using System.Collections.Generic;
using System.Collections;
using System.IO;
using Cave.Text;
using System.Linq;
using Cave.IO;
using System.Diagnostics;

namespace Cave.Data
{
    /// <summary>
    /// Provides a table field index implementation
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
                if (obj is BoxedValue)
                {
                    return Equals((BoxedValue)obj);
                }

                return Equals(obj, val);
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
                if (obj is BoxedValue)
                {
                    return CompareTo((BoxedValue)obj);
                }

                return Comparer.Default.Compare(val, obj);
            }
        }

#if USE_BOXING
        /// <summary>
        /// resolves value to IDs
        /// </summary>
        SortedDictionary<BoxedValue, Set<long>> m_Index;
#else
        /// <summary>
        /// resolves value to IDs
        /// </summary>
        SortedDictionary<object, Set<long>> m_Index;

        readonly object Null = new BoxedValue(null);
#endif

        /// <summary>Gets the id count.</summary>
        /// <value>The id count.</value>
        public int Count { get; private set; }

        /// <summary>Initializes a new instance of the <see cref="FieldIndex"/> class.</summary>
        public FieldIndex()
        {
#if USE_BOXING
            m_Index = new SortedDictionary<BoxedValue, Set<long>>();
#else
            m_Index = new SortedDictionary<object, Set<long>>();
#endif
        }

        /// <summary>
        /// Adds an ID, object combination to the index
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        internal void Add(long id, object value)
        {
#if USE_BOXING
            BoxedValue obj = new BoxedValue(value);
#else
            var obj = value == null ? Null : value;
#endif
            if (m_Index.ContainsKey(obj))
            {
                m_Index[obj].Add(id);
            }
            else
            {
                Set<long> list = new Set<long>();
                list.Add(id);
                m_Index[obj] = list;
            }
            Count++;
        }

        /// <summary>Replaces the object for the specified identifier</summary>
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

        /// <summary>Removes an ID from the index</summary>
        /// <param name="id">The identifier.</param>
        /// <param name="value">The value.</param>
        /// <exception cref="ArgumentException">
        /// </exception>
        internal void Delete(long id, object value)
        {
#if USE_BOXING
            BoxedValue obj = new BoxedValue(value);
#else
            var obj = value == null ? Null : value;
#endif
            //remove ID from old hash
            Set<long> ids;
            if (!m_Index.TryGetValue(obj, out ids))
            {
                //TODO REMOVE ME
                var items = m_Index.Keys.Where(i => Equals(i, obj)).ToList();
                foreach (var item in items)
                {
                    Trace.TraceWarning("Key {0} hash {1} != Key {2} hash {3} - Compare Result {4}", obj, obj.GetHashCode(), item, item.GetHashCode(), Comparer.Default.Compare(obj, item));
                }
				File.WriteAllText("temp.txt", m_Index.Keys.JoinNewLine());
                //END REMOVE ME
                throw new ArgumentException(string.Format("Object {0} is not present at index (equals check {1})!", obj, items.Join(",")));
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
                if (!m_Index.Remove(obj))
                {
                    throw new ArgumentException(string.Format("Could not remove id {0} object {1} could not be removed!", id, obj));
                }
            }
            Count--;
        }

        /// <summary>
        /// Clears the index
        /// </summary>
        internal void Clear()
        {
            m_Index.Clear();
            Count = 0;
        }

        /// <summary>Obtains all IDs with the specified hashcode</summary>
        /// <param name="value">The value.</param>
        /// <returns></returns>
        public IItemSet<long> Find(object value)
        {
#if USE_BOXING
            BoxedValue obj = new BoxedValue(value);
#else
            var obj = value == null ? Null : value;
#endif
            if (m_Index.ContainsKey(obj))
            {
                return new ReadOnlySet<long>(m_Index[obj]);
            }
            return new Set<long>();
        }

        /// <summary>Gets the sorted identifiers.</summary>
        /// <value>The sorted identifiers.</value>
        public IEnumerable<long> SortedIDs
        {
            get
            {
#if USE_BOXING
                return new FieldIndexEnumeration<BoxedValue>(m_Index);
#else
                return new FieldIndexEnumeration<object>(m_Index);
#endif
            }
        }

        class FieldIndexEnumeration<T> : IEnumerable<long>
        {
            SortedDictionary<T, Set<long>> idx;

            public FieldIndexEnumeration(SortedDictionary<T, Set<long>> idx)
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
            SortedDictionary<T, Set<long>> idx;
            IEnumerator outer;
            IEnumerator inner;

            public FieldIndexEnumerator(SortedDictionary<T, Set<long>> idx)
            {
                this.idx = idx;
                Reset();
            }

            public long Current { get { return (long)inner.Current; } }

            object IEnumerator.Current { get { return inner.Current; } }

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
                    KeyValuePair<T, Set<long>> keyValuePair = (KeyValuePair<T, Set<long>>)outer.Current;
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
