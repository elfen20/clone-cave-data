using System;
using System.Collections;
using System.Collections.Generic;

namespace Cave.Data
{
    internal sealed class TableSorter : IComparer<long>
    {
        readonly bool descending;
        readonly int fieldNumber;
        readonly ITable table;

        public TableSorter(ITable table, int fieldNumber, ResultOptionMode mode)
        {
            if (fieldNumber < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(fieldNumber));
            }

            this.table = table ?? throw new ArgumentNullException("Table");
            switch (mode)
            {
                case ResultOptionMode.SortAsc: descending = false; break;
                case ResultOptionMode.SortDesc: descending = true; break;
                default: throw new ArgumentOutOfRangeException(nameof(mode));
            }
            this.fieldNumber = fieldNumber;
        }

        public int Compare(long x, long y)
        {
            var val1 = table.GetRow(x).GetValue(fieldNumber);
            var val2 = table.GetRow(y).GetValue(fieldNumber);
            return descending ? Comparer.Default.Compare(val2, val1) : Comparer.Default.Compare(val1, val2);
        }
    }
}
