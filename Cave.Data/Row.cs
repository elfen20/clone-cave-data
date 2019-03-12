using System;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using Cave.Collections;

namespace Cave.Data
{
    /// <summary>
    /// Provides a data row implementation providing untyped data to strong typed struct interop.
    /// </summary>
    [DebuggerTypeProxy(typeof(DebugView))]
    public sealed class Row : IEquatable<Row>
    {
        internal class DebugView
        {
            Row row;

            public DebugView(Row row)
            {
                this.row = row;
            }

            [DebuggerBrowsable(DebuggerBrowsableState.RootHidden)]
            public object[] Fields => row.data;
        }

        /// <summary>Implements the operator ==.</summary>
        /// <param name="x">The x row.</param>
        /// <param name="y">The y row.</param>
        /// <returns>The result of the operator.</returns>
        public static bool operator ==(Row x, Row y) => ReferenceEquals(null, x) ? ReferenceEquals(y, null) : x.Equals(y);

        /// <summary>Implements the operator !=.</summary>
        /// <param name="x">The x row.</param>
        /// <param name="y">The y row.</param>
        /// <returns>The result of the operator.</returns>
        public static bool operator !=(Row x, Row y)
        {
            return ReferenceEquals(null, x) ? !ReferenceEquals(y, null) : !x.Equals(y);
        }

        #region private implementation
        [DebuggerBrowsable(DebuggerBrowsableState.Never)]
        object[] data;
        #endregion

        #region constructors

        /// <summary>
        /// Creates a new <see cref="Row"/> instance.
        /// </summary>
        public static Row Create<T>(ref RowLayout layoutCache, T item)
            where T : struct
        {
            if (layoutCache == null)
            {
                layoutCache = RowLayout.CreateTyped(typeof(T));
            }

            return Create(layoutCache, item);
        }

        /// <summary>
        /// Creates a new <see cref="Row"/> instance.
        /// </summary>
        public static Row Create<T>(RowLayout layout, T item)
            where T : struct
        {
            if (!layout.IsTyped)
            {
                throw new NotSupportedException(string.Format("RowLayout needs to be a typed layout!"));
            }

            return new Row(layout.GetValues(item));
        }

        /// <summary>
        /// Creates a new <see cref="Row"/> instance.
        /// </summary>
        public Row(object[] values)
        {
            data = values;
        }
        #endregion

        #region Row Members

        /// <summary>
        /// Gets the value of the specified field.
        /// </summary>
        /// <param name="fieldNumber">The fieldnumber to read.</param>
        /// <returns>Returns the value.</returns>
        public object GetValue(int fieldNumber)
        {
            return data[fieldNumber];
        }

        /// <summary>
        /// Retrieves a string for the specified value. The string may be parsed back to a value using <see cref="RowLayout.ParseValue(int, string, string, CultureInfo)" />.
        /// </summary>
        /// <param name="layout">The layout.</param>
        /// <param name="fieldNumber">The field number.</param>
        /// <param name="value">The value.</param>
        /// <param name="stringMarker">The string marker.</param>
        /// <param name="jsonMode">if set to <c>true</c> [json mode].</param>
        /// <param name="culture">The culture.</param>
        /// <returns></returns>
        public string GetString(RowLayout layout, int fieldNumber, object value, string stringMarker, bool jsonMode, CultureInfo culture = null)
        {
            FieldProperties field = layout.GetProperties(fieldNumber);
            return field.GetString(value, stringMarker, jsonMode, culture);
        }

        /// <summary>Sets the identifier.</summary>
        /// <param name="idFieldIndex">Index of the identifier field.</param>
        /// <param name="id">The identifier.</param>
        /// <returns></returns>
        public Row SetID(int idFieldIndex, long id)
        {
            var row = GetValues();
            row[idFieldIndex] = id;
            return new Row(row);
        }

        /// <summary>Gets the identifier.</summary>
        /// <param name="idFieldIndex">Index of the identifier field.</param>
        /// <returns></returns>
        public long GetID(int idFieldIndex)
        {
            var value = data[idFieldIndex];
            return value is long ? (long)value : Convert.ToInt64(value);
        }

        /// <summary>
        /// Gets all values of the row.
        /// </summary>
        /// <returns></returns>
        public object[] GetValues()
        {
            return (object[])data.Clone();
        }

        /// <summary>
        /// Gets a struct containing all values of the row.
        /// </summary>
        /// <exception cref="NotSupportedException">Thrown if the row was not created with a typed layout.</exception>
        public T GetStruct<T>(RowLayout layout)
            where T : struct
        {
            if (!layout.IsTyped)
            {
                throw new NotSupportedException(string.Format("This Row was not created from a typed layout!"));
            }

            object result = default(T);
            layout.SetValues(ref result, data);
            return (T)result;
        }

        #endregion

        /// <summary>Obtains a row value as string using the string format defined at the rowlayout.</summary>
        /// <param name="layout">The layout.</param>
        /// <param name="field">The field.</param>
        /// <returns></returns>
        public string GetDisplayString(RowLayout layout, int field)
        {
            var value = GetValue(field);
            return value == null ? string.Empty : layout.GetDisplayString(field, value);
        }

        /// <summary>
        /// Gets all row values as strings using the string format defined at the rowlayout.
        /// </summary>
        /// <returns></returns>
        public string[] GetDisplayStrings(RowLayout layout)
        {
            var values = GetValues();
            var strings = new string[values.Length];
            for (var i = 0; i < values.Length; i++)
            {
                strings[i] = layout.GetDisplayString(i, values[i]);
            }
            return strings;
        }

        #region overrides

        /// <summary>
        /// Returns the row type and fieldcount.
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            var result = new StringBuilder();
            result.Append("Row[");
            result.Append(data.Length);
            result.Append("] ");
            result.Append(" ");
            var l_First = true;
            foreach (var obj in data)
            {
                if (l_First)
                {
                    l_First = false;
                }
                else
                {
                    result.Append(" ");
                }

                if (obj != null)
                {
                    result.Append(obj.ToString());
                }
            }
            return result.ToString();
        }

        #endregion

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
            return ReferenceEquals(null, other) ? false : DefaultComparer.Equals(data, other.data);
        }

        /// <summary>Returns a hash code for this instance.</summary>
        /// <returns>
        /// A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table.
        /// </returns>
        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
    }
}
