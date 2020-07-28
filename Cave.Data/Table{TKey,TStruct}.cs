using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Cave.Data
{
    /// <summary>Provides a table of structures (rows).</summary>
    /// <typeparam name="TKey">Key identifier type.</typeparam>
    /// <typeparam name="TStruct">Row structure type.</typeparam>
    public class Table<TKey, TStruct> : AbstractTable<TKey, TStruct>
        where TKey : IComparable<TKey>
        where TStruct : struct
    {
        /// <summary>Initializes a new instance of the <see cref="Table{TKey, TStruct}" /> class.</summary>
        /// <param name="table">The table instance to wrap.</param>
        public Table(ITable table)
        {
            BaseTable = table;
            var layout = RowLayout.CreateTyped(typeof(TStruct));
            if (table.Flags.HasFlag(TableFlags.IgnoreMissingFields))
            {
                var result = new List<IFieldProperties>();
                foreach (var field in layout)
                {
                    var match = table.Layout.FirstOrDefault(f => f.Equals(field));
                    if (match == null)
                    {
                        throw new InvalidDataException($"Field {field} cannot be found at table {table}");
                    }

                    var target = match.Clone();
                    target.FieldInfo = field.FieldInfo;
                    result.Add(target);
                }

                layout = new RowLayout(table.Name, result.ToArray(), typeof(TStruct));
            }
            else
            {
                RowLayout.CheckLayout(layout, table.Layout);
            }

            Layout = layout;
            var keyField = layout.Identifier.Single();
            if (keyField.ValueType != typeof(TKey))
            {
                throw new ArgumentException($"{nameof(TKey)} needs to be of type {keyField.ValueType}!");
            }

            KeyField = keyField;
            table.UseLayout(layout);
        }

        /// <inheritdoc />
        public override RowLayout Layout { get; }

        /// <inheritdoc />
        protected override ITable BaseTable { get; }

        /// <inheritdoc />
        protected override IFieldProperties KeyField { get; }

        /// <inheritdoc />
        public override void Connect(IDatabase database, TableFlags flags, RowLayout layout) => BaseTable.Connect(database, flags, layout);

        /// <summary>Not supported.</summary>
        /// <param name="layout">Unused parameter.</param>
        public override void UseLayout(RowLayout layout) => throw new NotSupportedException();
    }
}
