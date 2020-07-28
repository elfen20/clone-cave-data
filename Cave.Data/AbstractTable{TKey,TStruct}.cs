using System;
using System.Collections.Generic;
using System.Linq;

namespace Cave.Data
{
    /// <summary>Provides a table of structures (rows).</summary>
    /// <typeparam name="TKey">Key identifier type.</typeparam>
    /// <typeparam name="TStruct">Row structure type.</typeparam>
    public abstract class AbstractTable<TKey, TStruct> : AbstractTable<TStruct>, ITable<TKey, TStruct>, ITable<TStruct>, ITable
        where TKey : IComparable<TKey>
        where TStruct : struct
    {
        /// <summary>Gets the identifier field.</summary>
        protected abstract IFieldProperties KeyField { get; }

        #region ITable{TKey, TStruct} properties

        /// <inheritdoc />
        public TStruct this[TKey id] => GetStruct(id);

        #endregion

        #region ITable{TKey, TStruct} interface

        /// <inheritdoc />
        public bool Exist(TKey id) => BaseTable.Exist(KeyField.Name, id);

        /// <inheritdoc />
        public void Delete(TKey id)
        {
            var result = BaseTable.TryDelete(KeyField.Name, id);
            switch (result)
            {
                case 0: throw new ArgumentException($"Could not delete id {id}. No matching dataset found!");
                case 1: break;
                default: throw new InvalidOperationException($"Multiple datasets with id {id} deleted!");
            }
        }

        /// <inheritdoc />
        public void Delete(IEnumerable<TKey> ids) => BaseTable.TryDelete(Search.FieldIn(KeyField.Name, ids));

        /// <inheritdoc />
        public Row GetRow(TKey id) => BaseTable.GetRow(KeyField.Name, id);

        /// <inheritdoc />
        public IList<Row> GetRows(IEnumerable<TKey> ids) => BaseTable.GetRows(Search.FieldIn(KeyField.Name, ids));

        /// <inheritdoc />
        public TStruct GetStruct(TKey id) => GetRow(id).GetStruct<TStruct>(Layout);

        /// <inheritdoc />
        public IList<TStruct> GetStructs(IEnumerable<TKey> ids)
            => GetRows(Search.FieldIn(KeyField.Name, ids)).Select(r => r.GetStruct<TStruct>(Layout)).ToList();

        /// <inheritdoc />
        public IDictionary<TKey, TStruct> GetDictionary(Search search = null, ResultOption resultOption = null)
            => GetRows(search, resultOption).ToDictionary(r => (TKey) r[KeyField.Index], r => r.GetStruct<TStruct>(Layout));

        #endregion
    }
}
