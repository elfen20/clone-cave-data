using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Cave.Data
{
    class FakeSortedDictionary<TKey, TValue> : IEnumerable<KeyValuePair<TKey, TValue>>
    {
        IDictionary<TKey, TValue> unsorted;
        TKey[] sortedKeys;

        #region constructor

        public FakeSortedDictionary()
        {
            unsorted = new Dictionary<TKey, TValue>();
        }

        public FakeSortedDictionary(int capacity)
        {
            unsorted = new Dictionary<TKey, TValue>(capacity);
        }

        #endregion

        #region properties

        public ICollection<TKey> UnsortedKeys => unsorted.Keys;

        public IList<TKey> SortedKeys
        {
            get
            {
                if (sortedKeys == null)
                {
                    sortedKeys = new TKey[unsorted.Count];
                    unsorted.Keys.CopyTo(sortedKeys, 0);
                    Array.Sort(sortedKeys);
                }
                return sortedKeys;
            }
        }

        public IList<TValue> Values
        {
            get
            {
                return SortedKeys.Select(k => unsorted[k]).ToList();
            }
        }

        public int Count => unsorted.Count;

        public bool IsReadOnly => unsorted.IsReadOnly;

        public TValue this[TKey key]
        {
            get => unsorted[key];
            set
            {
                unsorted[key] = value;
                sortedKeys = null;
            }
        }

        #endregion

        #region functions

        public void Add(TKey key, TValue value)
        {
            unsorted.Add(key, value);
            sortedKeys = null;
        }

        public void Add(KeyValuePair<TKey, TValue> item)
        {
            unsorted.Add(item);
            sortedKeys = null;
        }

        public void Clear()
        {
            unsorted.Clear();
            sortedKeys = null;
        }

        public bool Contains(KeyValuePair<TKey, TValue> item)
        {
            return unsorted.Contains(item);
        }

        public bool ContainsKey(TKey key)
        {
            return unsorted.ContainsKey(key);
        }

        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
        {
            unsorted.CopyTo(array, arrayIndex);
        }

        public bool Remove(TKey key)
        {
            sortedKeys = null;
            return unsorted.Remove(key);
        }

        public bool Remove(KeyValuePair<TKey, TValue> item)
        {
            sortedKeys = null;
            return unsorted.Remove(item);
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            return unsorted.TryGetValue(key, out value);
        }

        public IList<KeyValuePair<TKey, TValue>> ToArray()
        {
            var count = unsorted.Count;
            var result = new KeyValuePair<TKey, TValue>[count];
            var i = 0;
            foreach (var key in SortedKeys)
            {
                result[i++] = new KeyValuePair<TKey, TValue>(key, unsorted[key]);
            }
            return result;
        }

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator() => ToArray().GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => ToArray().GetEnumerator();

        #endregion
    }
}
