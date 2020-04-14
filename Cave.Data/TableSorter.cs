using System;
using System.Collections;
using System.Collections.Generic;

namespace Cave.Data
{
    internal sealed class TableSorter : IComparer<Row>
    {
        readonly IComparer comparer;
        readonly bool descending;
        readonly IFieldProperties field;

        public TableSorter(IFieldProperties field, ResultOptionMode mode)
        {
            this.field = field ?? throw new ArgumentNullException(nameof(field));
            switch (field.DataType)
            {
                default:
                case DataType.Binary: throw new NotSupportedException();

                case DataType.Bool: comparer = Comparer<bool>.Default; break;
                case DataType.Int8: comparer = Comparer<sbyte>.Default; break;
                case DataType.Int16: comparer = Comparer<short>.Default; break;
                case DataType.Int32: comparer = Comparer<int>.Default; break;
                case DataType.Int64: comparer = Comparer<long>.Default; break;
                case DataType.UInt8: comparer = Comparer<byte>.Default; break;
                case DataType.UInt16: comparer = Comparer<ushort>.Default; break;
                case DataType.UInt32: comparer = Comparer<uint>.Default; break;
                case DataType.UInt64: comparer = Comparer<ulong>.Default; break;
                case DataType.Char: comparer = Comparer<char>.Default; break;
                case DataType.DateTime: comparer = Comparer<DateTime>.Default; break;
                case DataType.Decimal: comparer = Comparer<decimal>.Default; break;
                case DataType.Double: comparer = Comparer<double>.Default; break;
                case DataType.Enum: comparer = Comparer.Default; break;
                case DataType.Single: comparer = Comparer<float>.Default; break;
                case DataType.String: comparer = Comparer<string>.Default; break;
                case DataType.TimeSpan: comparer = Comparer<TimeSpan>.Default; break;
                case DataType.User: comparer = Comparer<string>.Default; break;
            }

            switch (mode)
            {
                case ResultOptionMode.SortAsc: descending = false; break;
                case ResultOptionMode.SortDesc: descending = true; break;
                default: throw new ArgumentOutOfRangeException(nameof(mode));
            }
        }

        public int Compare(Row row1, Row row2)
        {
            var val1 = row1[field.Index];
            var val2 = row2[field.Index];
            return descending ? comparer.Compare(val2, val1) : comparer.Compare(val1, val2);
        }
    }
}
