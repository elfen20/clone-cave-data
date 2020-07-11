using System;
using System.Collections.Generic;
using System.Linq;
using Cave.Collections;

namespace Cave.Data
{
    /// <summary>
    /// Provides a data row implementation providing untyped data to strong typed struct interop.
    /// </summary>
    public sealed class Row : IEquatable<Row>
    {
        /// <summary>
        /// Gets the current values of the row.
        /// </summary>
        public readonly object[] Values;

        /// <summary>
        /// Gets the row layout.
        /// </summary>
        public readonly RowLayout Layout;

        #region constructors

        /// <summary>
        /// Initializes a new instance of the <see cref="Row"/> class.
        /// </summary>
        /// <param name="layout">Layout of this row.</param>
        /// <param name="values">Values for all fields.</param>
        /// <param name="clone">Copy values on row create.</param>
        internal Row(RowLayout layout, object[] values, bool clone)
        {
            Layout = layout ?? throw new ArgumentNullException(nameof(layout));
            if (values == null)
            {
                throw new ArgumentNullException(nameof(values));
            }
            if (values.Length != layout.FieldCount)
            {
                throw new ArgumentOutOfRangeException(nameof(layout), "values.Length != layout.FieldCount");
            }
            Values = clone ? (object[])values.Clone() : values;
        }

        #endregion

        /// <summary>
        /// Gets the fieldcount.
        /// </summary>
        public int FieldCount => Values.Length;

        /// <summary>
        /// Gets the content of the field with the specified <paramref name="fieldIndex"/>.
        /// </summary>
        /// <param name="fieldIndex">Index of the field.</param>
        /// <returns>Returns a value or null.</returns>
        public object this[int fieldIndex] => Values[fieldIndex];

        /// <summary>
        /// Gets the content of the field with the specified <paramref name="fieldName"/>.
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <returns>Returns a value or null.</returns>
        public object this[string fieldName] => Values[Layout.GetFieldIndex(fieldName, true)];

        /// <summary>
        /// Gets all values of the row.
        /// </summary>
        /// <returns>A copy of all values.</returns>
        public object[] CopyValues() => (object[])Values.Clone();

        /// <summary>
        /// Gets a struct containing all values of the row.
        /// </summary>
        /// <param name="layout">Table layout.</param>
        /// <typeparam name="TStruct">Structure type.</typeparam>
        /// <returns>A new structure instance.</returns>
        public TStruct GetStruct<TStruct>(RowLayout layout)
            where TStruct : struct
        {
            if (!layout.IsTyped)
            {
                throw new NotSupportedException(string.Format("This Row was not created from a typed layout!"));
            }

            object result = default(TStruct);
            layout.SetValues(ref result, Values);
            return (TStruct)result;
        }

        /// <summary>Obtains a row value as string using the string format defined at the rowlayout.</summary>
        /// <param name="layout">The layout.</param>
        /// <param name="index">The field index.</param>
        /// <returns>The string to display.</returns>
        public string GetDisplayString(RowLayout layout, int index)
        {
            var value = Values[index];
            return value == null ? string.Empty : layout.GetDisplayString(index, value);
        }

        /// <summary>
        /// Gets all row values as strings using the string format defined at the rowlayout.
        /// </summary>
        /// <param name="layout">Table layout.</param>
        /// <returns>An array containing the display strings for all fields.</returns>
        public string[] GetDisplayStrings(RowLayout layout)
        {
            var strings = new string[Values.Length];
            for (var i = 0; i < Values.Length; i++)
            {
                strings[i] = layout.GetDisplayString(i, Values[i]);
            }
            return strings;
        }

        #region overrides

        /// <inheritdoc/>
        public override string ToString() => CsvWriter.RowToString(CsvProperties.Default, Layout, this);

        #endregion

        /// <summary>
        /// Gets the value with the specified name.
        /// </summary>
        /// <typeparam name="T">The expected type.</typeparam>
        /// <param name="fieldName">The name of the field.</param>
        /// <returns>Returns the field value.</returns>
        public T GetValue<T>(string fieldName) => GetValue<T>(Layout.GetFieldIndex(fieldName, true));

        /// <summary>
        /// Gets the value with the specified name.
        /// </summary>
        /// <typeparam name="T">The expected type.</typeparam>
        /// <param name="fieldIndex">The index of the field.</param>
        /// <returns>Returns the field value.</returns>
        public T GetValue<T>(int fieldIndex)
        {
            var result = TypeExtension.ConvertValue(typeof(T), Values[fieldIndex], null);
            if (result is null)
            {
                return default(T);
            }
            else
            {
                return (T)result;
            }
        }

        /// <summary>
        /// Gets a dictionary containing all field.Name = value combinations of this row.
        /// </summary>
        /// <returns>Returns a new <see cref="IDictionary{TKey, TValue}"/> instance.</returns>
        public IDictionary<string, object> ToDictionary() => Layout.ToDictionary(field => field.Name, field => Values[field.Index]);

        /// <summary>
        /// Determines whether the specified <see cref="object" />, is equal to this instance.
        /// </summary>
        /// <param name="obj">The <see cref="object" /> to compare with this instance.</param>
        /// <returns>
        /// <c>true</c> if the specified <see cref="object" /> is equal to this instance; otherwise, <c>false</c>.
        /// </returns>
        public override bool Equals(object obj)
        {
            return Equals(obj as Row);
        }

        /// <summary>Equalses the specified other.</summary>
        /// <param name="other">The other.</param>
        /// <returns>
        /// <c>true</c> if the specified <see cref="Row" /> is equal to this instance; otherwise, <c>false</c>.
        /// </returns>
        public bool Equals(Row other)
        {
            if (other?.Values?.Length != Values.Length)
            {
                return false;
            }
            for (int i = 0; i < Values.Length; i++)
            {
                var source = Values[i];
                var target = other.Values[i];
                if (!DefaultComparer.Equals(source, target))
                {
                    return false;
                }
            }
            return true;
        }

        /// <summary>Returns a hash code for this instance.</summary>
        /// <returns>
        /// A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table.
        /// </returns>
        public override int GetHashCode()
        {
            int hash = 0x1234;
            foreach (var obj in Values)
            {
                hash = hash.BitwiseRotateLeft(1);
                if (obj is null)
                {
                    continue;
                }
                if (obj.GetType().IsArray)
                {
                    foreach (var item in (Array)obj)
                    {
                        hash = hash.BitwiseRotateLeft(1);
                        hash ^= item?.GetHashCode() ?? 0;
                    }
                }
                else
                {
                    hash ^= obj.GetHashCode();
                }
            }
            return hash;
        }
    }
}
