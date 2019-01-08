using System;
using System.Collections;
using System.Collections.Generic;

namespace Cave.Data
{
    class FakeSortedDictionary<TKey, TValue> : IDictionary<TKey, TValue>
    {
        IDictionary<TKey, TValue> dict;
        TKey[] keys;

        public FakeSortedDictionary()
        {
            dict = new Dictionary<TKey, TValue>();
        }

        public FakeSortedDictionary(int capacity)
        {
            dict = new Dictionary<TKey, TValue>(capacity);
        }

        public TValue this[TKey key]
        {
            get => dict[key];
            set
            {
                dict[key] = value;
                keys = null;
            }
        }

        public ICollection<TKey> Keys
        {
            get
            {
                if (keys == null)
                {
                    keys = new TKey[dict.Count];
                    dict.Keys.CopyTo(keys, 0);
                    Array.Sort(keys);
                }
                return keys;
            }
        }

        public ICollection<TValue> Values => dict.Values;

        public int Count => dict.Count;

        public bool IsReadOnly => dict.IsReadOnly;

        public void Add(TKey key, TValue value)
        {
            dict.Add(key, value);
            keys = null;
        }

        public void Add(KeyValuePair<TKey, TValue> item)
        {
            dict.Add(item);
            keys = null;
        }

        public void Clear()
        {
            dict.Clear();
            keys = null;
        }

        public bool Contains(KeyValuePair<TKey, TValue> item)
        {
            return dict.Contains(item);
        }

        public bool ContainsKey(TKey key)
        {
            return dict.ContainsKey(key);
        }

        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
        {
            dict.CopyTo(array, arrayIndex);
        }

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            return dict.GetEnumerator();
        }

        public bool Remove(TKey key)
        {
            keys = null;
            return dict.Remove(key);
        }

        public bool Remove(KeyValuePair<TKey, TValue> item)
        {
            keys = null;
            return dict.Remove(item);
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            return dict.TryGetValue(key, out value);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return dict.GetEnumerator();
        }
    }
}
