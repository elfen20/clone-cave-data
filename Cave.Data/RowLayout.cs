using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using Cave.IO;

namespace Cave.Data
{
    /// <summary>
    ///     Provides a row layout implementation.
    /// </summary>
    public sealed class RowLayout : IEquatable<RowLayout>, IEnumerable<IFieldProperties>
    {
        /// <inheritdoc />
        public IEnumerator<IFieldProperties> GetEnumerator() => properties.GetEnumerator();

        /// <inheritdoc />
        IEnumerator IEnumerable.GetEnumerator() => properties.GetEnumerator();

        /// <inheritdoc />
        public bool Equals(RowLayout layout)
        {
            if (layout == null)
            {
                throw new ArgumentNullException(nameof(layout));
            }

            if (layout.FieldCount != FieldCount)
            {
                return false;
            }

            for (var i = 0; i < FieldCount; i++)
            {
                if (!layout.properties[i].Equals(properties[i]))
                {
                    return false;
                }
            }

            return true;
        }

        /// <summary>
        ///     Clears the layout cache.
        /// </summary>
        public static void ClearCache()
        {
            lock (layoutCache)
            {
                layoutCache.Clear();
            }
        }

        /// <summary>
        ///     Checks two layouts for equality.
        /// </summary>
        /// <param name="expected">The expected layout.</param>
        /// <param name="current">The layout to check.</param>
        /// <param name="fieldPropertiesConversion">field conversion function to use.</param>
        public static void CheckLayout(RowLayout expected, RowLayout current,
            Func<IFieldProperties, IFieldProperties> fieldPropertiesConversion = null)
        {
            if (ReferenceEquals(expected, current))
            {
                return;
            }

            if (expected == null)
            {
                throw new ArgumentNullException(nameof(expected));
            }

            if (current == null)
            {
                throw new ArgumentNullException(nameof(current));
            }

            if (expected.FieldCount != current.FieldCount)
            {
                throw new InvalidDataException($"Fieldcount of table {current.Name} differs (found {current.FieldCount} expected {expected.FieldCount})!");
            }

            for (var i = 0; i < expected.FieldCount; i++)
            {
                var expectedField = expected[i];
                var currentField = current[i];
                if (fieldPropertiesConversion != null)
                {
                    expectedField = fieldPropertiesConversion(expectedField);
                    currentField = fieldPropertiesConversion(currentField);
                }

                if (!expectedField.Equals(currentField))
                {
                    throw new InvalidDataException($"Fieldproperties of table {current.Name} differ! (found {currentField} expected {expectedField})!");
                }
            }
        }

        /// <summary>Loads the row layout from the specified reader.</summary>
        /// <param name="reader">The reader.</param>
        /// <returns>A new <see cref="RowLayout" /> instance.</returns>
        public static RowLayout Load(DataReader reader)
        {
            if (reader == null)
            {
                throw new ArgumentNullException(nameof(reader));
            }

            var count = reader.Read7BitEncodedInt32();
            var tableName = reader.ReadString();
            var fieldProperties = new FieldProperties[count];
            for (var i = 0; i < count; i++)
            {
                var field = fieldProperties[i] = new FieldProperties();
                field.Load(reader, i);
            }

            return new RowLayout(tableName, fieldProperties, null);
        }

        /// <summary>Creates an alien row layout without using any field properies.</summary>
        /// <param name="type">Type to parse fields from.</param>
        /// <param name="onlyPublic">if set to <c>true</c> [use only public].</param>
        /// <returns>A new <see cref="RowLayout" /> instance.</returns>
        public static RowLayout CreateAlien(Type type, bool onlyPublic)
        {
            if (type == null)
            {
                throw new ArgumentNullException("Type");
            }

            var bindingFlags = BindingFlags.Public | BindingFlags.Instance;
            if (onlyPublic)
            {
                bindingFlags |= BindingFlags.NonPublic;
            }

            var rawInfos = type.GetFields(bindingFlags);
            var properties = new FieldProperties[rawInfos.Length];
            for (var i = 0; i < rawInfos.Length; i++)
            {
                var fieldInfo = rawInfos[i];
                try
                {
                    if (fieldInfo.FieldType.IsArray)
                    {
                        continue;
                    }

                    var field = new FieldProperties();
                    field.LoadFieldInfo(i, fieldInfo);
                    properties[i] = field;
                }
                catch (Exception ex)
                {
                    throw new InvalidDataException(
                        $"Error while loading field properties of type {type.FullName} field {fieldInfo}!", ex);
                }
            }

            return new RowLayout(type.Name, properties, type);
        }

        /// <summary>
        ///     Creates a new layout with the given name and field properties.
        /// </summary>
        /// <param name="name">Name of the layout.</param>
        /// <param name="fields">FieldProperties to use.</param>
        /// <returns>A new <see cref="RowLayout" /> instance.</returns>
        public static RowLayout CreateUntyped(string name, params IFieldProperties[] fields) => new RowLayout(name, fields, null);

        /// <summary>Creates a RowLayout instance for the specified struct.</summary>
        /// <param name="type">The type to build the rowlayout for.</param>
        /// <param name="excludedFields">The excluded fields.</param>
        /// <returns>A new <see cref="RowLayout" /> instance.</returns>
        public static RowLayout CreateTyped(Type type, string[] excludedFields) => CreateTyped(type, null, null, excludedFields);

        /// <summary>Creates a RowLayout instance for the specified struct.</summary>
        /// <param name="type">The type to build the rowlayout for.</param>
        /// <param name="nameOverride">The table name override.</param>
        /// <param name="storage">The Storage engine to use.</param>
        /// <param name="excludedFields">The excluded fields.</param>
        /// <returns>A new <see cref="RowLayout" /> instance.</returns>
        public static RowLayout CreateTyped(Type type, string nameOverride = null, IStorage storage = null, params string[] excludedFields)
        {
            if (type == null)
            {
                throw new ArgumentNullException(nameof(type));
            }

            lock (layoutCache)
            {
                var cacheName = $"{type.FullName},{nameOverride}";
                if (!layoutCache.TryGetValue(cacheName, out var result))
                {
                    var isStruct = type.IsValueType && !type.IsEnum && !type.IsPrimitive;
                    if (!isStruct)
                    {
                        throw new ArgumentException(
                            $"Type {type} is not a struct! Only structs may be used as row definition!");
                    }

                    var tableName = TableAttribute.GetName(type);
                    if (string.IsNullOrEmpty(tableName))
                    {
                        tableName = type.Name;
                    }

                    if (!string.IsNullOrEmpty(nameOverride))
                    {
                        tableName = nameOverride;
                    }

                    if (tableName.HasInvalidChars(ASCII.Strings.SafeName))
                    {
                        throw new ArgumentException("Invalid characters at table name!");
                    }

                    var rawInfos =
                        type.GetFields(BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance);
                    var properties = new List<IFieldProperties>(rawInfos.Length);
                    for (var i = 0; i < rawInfos.Length; i++)
                    {
                        var fieldInfo = rawInfos[i];
                        try
                        {
                            if (fieldInfo.GetCustomAttributes(typeof(FieldAttribute), false).Length == 0)
                            {
                                continue;
                            }

                            if (excludedFields != null)
                            {
                                if (Array.IndexOf(excludedFields, fieldInfo.Name) > -1)
                                {
                                    continue;
                                }
                            }

                            var fieldProperties = new FieldProperties();
                            fieldProperties.LoadFieldInfo(i, fieldInfo);
                            properties.Add(storage == null
                                ? fieldProperties
                                : storage.GetDatabaseFieldProperties(fieldProperties));
                        }
                        catch (Exception ex)
                        {
                            throw new InvalidOperationException(
                                $"Error while loading field properties of type {type.FullName} field {fieldInfo}!", ex);
                        }
                    }

                    result = new RowLayout(tableName, properties.ToArray(), type);
                    if (!DisableLayoutCache)
                    {
                        layoutCache[cacheName] = result;
                    }
                }

                return result;
            }
        }

        /// <summary>
        ///     Gets the <see cref="DataType" /> for a given <see cref="Type" />.
        /// </summary>
        /// <param name="type">The <see cref="Type" /> to convert.</param>
        /// <returns>The data type.</returns>
        public static DataType DataTypeFromType(Type type)
        {
            if (type == null)
            {
                throw new ArgumentNullException(nameof(type));
            }

            if (type == typeof(sbyte))
            {
                return DataType.Int8;
            }

            if (type == typeof(byte))
            {
                return DataType.UInt8;
            }

            if (type == typeof(short))
            {
                return DataType.Int16;
            }

            if (type == typeof(ushort))
            {
                return DataType.UInt16;
            }

            if (type == typeof(int))
            {
                return DataType.Int32;
            }

            if (type == typeof(uint))
            {
                return DataType.UInt32;
            }

            if (type == typeof(long))
            {
                return DataType.Int64;
            }

            if (type == typeof(ulong))
            {
                return DataType.UInt64;
            }

            if (type == typeof(char))
            {
                return DataType.Char;
            }

            if (type == typeof(string))
            {
                return DataType.String;
            }

            if (type == typeof(float))
            {
                return DataType.Single;
            }

            if (type == typeof(double))
            {
                return DataType.Double;
            }

            if (type == typeof(bool))
            {
                return DataType.Bool;
            }

            if (type == typeof(decimal))
            {
                return DataType.Decimal;
            }

            if (type == typeof(byte[]))
            {
                return DataType.Binary;
            }

            if (type == typeof(TimeSpan))
            {
                return DataType.TimeSpan;
            }

            if (type == typeof(DateTime))
            {
                return DataType.DateTime;
            }

            return type.IsEnum ? DataType.Enum : DataType.User;
        }

        /// <summary>
        ///     Retrieves a string for the specified value.
        ///     The string may be parsed back to a value using <see cref="ParseValue(int, string, string, IFormatProvider)" />.
        /// </summary>
        /// <param name="fieldIndex">The field number.</param>
        /// <param name="value">The value.</param>
        /// <param name="stringMarker">The string marker.</param>
        /// <param name="culture">The culture.</param>
        /// <returns>A string for the value.</returns>
        public string GetString(int fieldIndex, object value, string stringMarker, CultureInfo culture = null)
        {
            var field = properties[fieldIndex];
            return field.GetString(value, stringMarker, culture);
        }

        /// <summary>Gets the string representing the specified value using the field properties.</summary>
        /// <param name="fieldIndex">The field number.</param>
        /// <param name="value">The value.</param>
        /// <returns>The string to display.</returns>
        public string GetDisplayString(int fieldIndex, object value)
        {
            var field = properties[fieldIndex];
            switch (field.DisplayFormat)
            {
                case "FormatTimeSpan":
                case "TimeSpan":
                    switch (field.DataType)
                    {
                        case DataType.TimeSpan: return ((TimeSpan)value).FormatTime();
                        default: return Convert.ToDouble(value).FormatTime();
                    }
                case "FormatValue":
                case "Size":
                    return Convert.ToDecimal(value).FormatSize();
                case "FormatBinary":
                case "Binary":
                    return Convert.ToDecimal(value).FormatSize();
            }

            switch (field.DataType)
            {
                case DataType.Int8: return ((sbyte)value).ToString(field.DisplayFormat);
                case DataType.Int16: return ((short)value).ToString(field.DisplayFormat);
                case DataType.Int32: return ((int)value).ToString(field.DisplayFormat);
                case DataType.Int64: return ((long)value).ToString(field.DisplayFormat);
                case DataType.UInt8: return ((byte)value).ToString(field.DisplayFormat);
                case DataType.UInt16: return ((ushort)value).ToString(field.DisplayFormat);
                case DataType.UInt32: return ((uint)value).ToString(field.DisplayFormat);
                case DataType.UInt64: return ((ulong)value).ToString(field.DisplayFormat);
                case DataType.Binary: return Base64.NoPadding.Encode((byte[])value);
                case DataType.DateTime: return ((DateTime)value).ToString(field.DisplayFormat);
                case DataType.Single: return ((float)value).ToString(field.DisplayFormat);
                case DataType.Double: return ((double)value).ToString(field.DisplayFormat);
                case DataType.Decimal: return ((decimal)value).ToString(field.DisplayFormat);
                default: return value == null ? string.Empty : value.ToString();
            }
        }

        /// <summary>Parses the value.</summary>
        /// <param name="index">The field index.</param>
        /// <param name="value">The value.</param>
        /// <param name="stringMarker">The string marker.</param>
        /// <param name="provider">The format provider.</param>
        /// <returns>The value parsed.</returns>
        public object ParseValue(int index, string value, string stringMarker, IFormatProvider provider = null)
        {
            var field = properties[index];
            return field.ParseValue(value, stringMarker, provider);
        }

        /// <summary>Enums the value.</summary>
        /// <param name="index">The field index.</param>
        /// <param name="value">The value.</param>
        /// <returns>The enum value.</returns>
        public object EnumValue(int index, long value)
        {
            var field = properties[index];
            return Enum.Parse(field.ValueType, value.ToString(), true);
        }

        /// <summary>
        ///     Gets the name of the field with the given number.
        /// </summary>
        /// <param name="index">The field index.</param>
        /// <returns>The name of the field.</returns>
        public string GetName(int index) => properties[index].Name;

        /// <summary>
        ///     Gets the value of a field from the specified struct.
        /// </summary>
        /// <param name="index">The field index.</param>
        /// <param name="item">The struct to read the value from.</param>
        /// <returns>The value of the specified field.</returns>
        public object GetValue(int index, object item)
        {
            if (!IsTyped)
            {
                throw new InvalidOperationException("This RowLayout was not created from a typed struct!");
            }

            return properties[index].FieldInfo.GetValue(item);
        }

        /// <summary>
        ///     Sets a value at the specified struct.
        /// </summary>
        /// <param name="index">The field index.</param>
        /// <param name="item">The struct to set the value at.</param>
        /// <param name="value">The value to set.</param>
        public void SetValue(int index, ref object item, object value)
        {
            if (!IsTyped)
            {
                throw new InvalidOperationException("This RowLayout was not created from a typed struct!");
            }

            if ((value != null) && (value.GetType() != properties[index].ValueType))
            {
                value = Convert.ChangeType(value, properties[index].ValueType);
            }

            properties[index].FieldInfo.SetValue(item, value);
        }

        /// <summary>
        ///     Gets all values of the struct.
        /// </summary>
        /// <param name="item">The struct to get the values from.</param>
        /// <returns>Returns all values of the struct.</returns>
        /// <typeparam name="TStruct">Structure type.</typeparam>
        public object[] GetValues<TStruct>(TStruct item)
            where TStruct : struct
        {
            if (!IsTyped)
            {
                throw new InvalidOperationException("This RowLayout was not created from a typed struct!");
            }

            if (RowType != typeof(TStruct))
            {
                throw new InvalidOperationException(
                    $"This RowLayout {RowType} does not match structure type {typeof(TStruct)}!");
            }

            var result = new object[FieldCount];
            for (var i = 0; i < FieldCount; i++)
            {
                var value = result[i] = properties[i].FieldInfo.GetValue(item);
                if (value is DateTime dt && (dt.Kind == DateTimeKind.Unspecified))
                {
                    throw new ArgumentOutOfRangeException("DateTime.Kind may not be DateTimeKind.Unspecified!");
                }
            }

            return result;
        }

        /// <summary>
        ///     Loads the structure fields into a new <see cref="Row" /> instance.
        /// </summary>
        /// <typeparam name="TStruct">Structure type.</typeparam>
        /// <param name="item">Structure to read.</param>
        /// <returns>A new row instance.</returns>
        public Row GetRow<TStruct>(TStruct item)
            where TStruct : struct
            => new Row(this, GetValues(item), false);

        /// <summary>
        ///     Sets all values of the struct.
        /// </summary>
        /// <param name="item">The struct to set the values at.</param>
        /// <param name="values">The values to set.</param>
        public void SetValues(ref object item, object[] values)
        {
            if (values == null)
            {
                throw new ArgumentNullException("Values");
            }

            if (!IsTyped)
            {
                throw new InvalidOperationException("This RowLayout was not created from a typed struct!");
            }

            for (var i = 0; i < FieldCount; i++)
            {
                var value = values[i];
                var field = properties[i];
                if ((value != null) && (value.GetType() != field.ValueType))
                {
                    switch (field.DataType)
                    {
                        case DataType.User:
                            value = properties[i].ParseValue(value.ToString());
                            break;
                        case DataType.Enum:
                            value = Enum.Parse(field.ValueType, value.ToString(), true);
                            break;
                        default:
                            value = Convert.ChangeType(values[i], properties[i].ValueType);
                            break;
                    }
                }

                properties[i].FieldInfo.SetValue(item, value);
            }
        }

        /// <summary>
        ///     Checks whether a field with the specified name exists or not.
        /// </summary>
        /// <param name="fieldName">The field name.</param>
        /// <returns>True is the field exists.</returns>
        public bool HasField(string fieldName) => GetFieldIndex(fieldName, false) > -1;

        /// <summary>
        ///     Gets the field index of the specified field name.
        /// </summary>
        /// <param name="fieldName">The field name to search for.</param>
        /// <returns>The field index of the specified field name.</returns>
        [Obsolete("Use int GetFieldIndex(string fieldName, bool throwException) instead!"),]
        public int GetFieldIndex(string fieldName) => GetFieldIndex(fieldName, false);

        /// <summary>
        ///     Gets the field index of the specified field name.
        /// </summary>
        /// <param name="fieldName">The field name to search for.</param>
        /// <param name="throwException">Throw exception if field cannot be found.</param>
        /// <returns>The field index of the specified field name.</returns>
        public int GetFieldIndex(string fieldName, bool throwException)
        {
            for (var i = 0; i < FieldCount; i++)
            {
                if (properties[i].Name.Equals(fieldName))
                {
                    return i;
                }
            }

            for (var i = 0; i < FieldCount; i++)
            {
                var names = properties[i].AlternativeNames?.Split(" ,;".ToCharArray(), StringSplitOptions.RemoveEmptyEntries);
                if ((names != null) && names.Any(n => n == fieldName))
                {
                    return i;
                }
            }

            if (throwException)
            {
                throw new ArgumentOutOfRangeException(nameof(fieldName), $"FieldName {fieldName} is not present at layout {this}!");
            }

            return -1;
        }

        /// <summary>Creates a copy of this layout without the specified field.</summary>
        /// <param name="fieldName">Name of the field to remove.</param>
        /// <returns>Returns a new <see cref="RowLayout" /> instance.</returns>
        public RowLayout Remove(string fieldName)
        {
            var index = GetFieldIndex(fieldName, true);
            var fieldProperties = new List<IFieldProperties>();
            for (var i = 0; i < FieldCount; i++)
            {
                if (i == index)
                {
                    continue;
                }

                fieldProperties.Add(properties[i]);
            }

            return new RowLayout(Name, fieldProperties.ToArray(), RowType);
        }

        /// <inheritdoc />
        public override bool Equals(object obj) => obj is RowLayout layout && Equals(layout);

        /// <inheritdoc />
        public override int GetHashCode()
        {
            var result = IsTyped ? 0x00001234 : 0x12345678;
            for (var i = 0; i < FieldCount; i++)
            {
                result ^= properties[i].GetHashCode() ^ i;
            }

            return result;
        }

        /// <summary>Saves the layout to the specified writer.</summary>
        /// <param name="writer">The writer.</param>
        public void Save(DataWriter writer)
        {
            if (writer == null)
            {
                throw new ArgumentNullException("Writer");
            }

            writer.Write7BitEncoded32(FieldCount);
            writer.WritePrefixed(Name);
            for (var i = 0; i < FieldCount; i++)
            {
                Save(writer, properties[i]);
            }
        }

        /// <summary>Returns a <see cref="string" /> that represents this instance.</summary>
        /// <returns>A <see cref="string" /> that represents this instance.</returns>
        public override string ToString() => $"RowLayout [{FieldCount}] {Name}";

        /// <summary>Saves the fieldproperties to the specified writer.</summary>
        /// <param name="writer">The writer.</param>
        /// <param name="field">Field properties to save.</param>
        void Save(DataWriter writer, IFieldProperties field)
        {
            if (writer == null)
            {
                throw new ArgumentNullException(nameof(writer));
            }

            writer.Write7BitEncoded32((int)field.DataType);
            writer.Write7BitEncoded32((int)field.TypeAtDatabase);
            writer.Write7BitEncoded32((int)field.Flags);
            writer.WritePrefixed(field.Name);
            writer.WritePrefixed(field.NameAtDatabase);
            var typeName =
                field.ValueType.AssemblyQualifiedName.Substring(0, field.ValueType.AssemblyQualifiedName.IndexOf(','));
            writer.WritePrefixed(typeName);
            if (field.DataType == DataType.DateTime)
            {
                writer.Write7BitEncoded32((int)field.DateTimeKind);
                writer.Write7BitEncoded32((int)field.DateTimeType);
            }

            if ((field.DataType == DataType.String) || (field.DataType == DataType.User))
            {
                writer.Write(field.MaximumLength);
            }
        }

        #region fields

        /// <summary>
        ///     Gets the name of the layout.
        /// </summary>
        public readonly string Name;

        /// <summary>The row type.</summary>
        public readonly Type RowType;

        /// <summary>
        ///     Gets the fieldcount.
        /// </summary>
        public readonly int FieldCount;

        static readonly Dictionary<string, RowLayout> layoutCache = new Dictionary<string, RowLayout>();
        readonly IList<IFieldProperties> properties;

        #endregion

        #region constructor

        /// <summary>
        ///     Initializes a new instance of the <see cref="RowLayout" /> class.
        /// </summary>
        public RowLayout()
        {
            properties = new IFieldProperties[0];
            Name = "Undefined";
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="RowLayout" /> class.
        /// </summary>
        /// <param name="name">The Name of the Layout.</param>
        /// <param name="fields">The fieldproperties to use.</param>
        /// <param name="rowtype">Dotnet row type.</param>
        internal RowLayout(string name, IFieldProperties[] fields, Type rowtype)
        {
            if (string.IsNullOrEmpty(name))
            {
                throw new ArgumentNullException("Name");
            }

            if (name.HasInvalidChars(ASCII.Strings.SafeName))
            {
                throw new ArgumentException("Invalid characters at table name!");
            }

            FieldCount = fields.Length;
            properties = fields;
            Name = name;
            RowType = rowtype;
        }

        #endregion

        #region properties

        /// <summary>
        ///     Gets or sets a value indicating whether caching for known typed layouts is disabled or not.
        /// </summary>
        public static bool DisableLayoutCache { get; set; }

        /// <summary>
        ///     Gets a value indicating whether the layout was created from a typed struct or not.
        /// </summary>
        public bool IsTyped => RowType != null;

        /// <summary>Gets the fields marked with the <see cref="FieldFlags.ID" />.</summary>
        public IEnumerable<IFieldProperties> Identifier => properties.Where(p => p.Flags.HasFlag(FieldFlags.ID));

        /// <summary>Gets the field properties.</summary>
        /// <value>A new readonly collection instance containing all field properties.</value>
        public IList<IFieldProperties> Fields => new ReadOnlyCollection<IFieldProperties>(properties);

        #endregion

        #region indexer

        /// <summary>Gets the field properties of the field with the specified index.</summary>
        /// <param name="index">Field index.</param>
        /// <returns>The <see cref="FieldProperties" /> instance.</returns>
        public IFieldProperties this[int index] => properties[index];

        /// <summary>Gets the field properties of the field with the specified name.</summary>
        /// <param name="fieldName">Field name.</param>
        /// <returns>The <see cref="FieldProperties" /> instance.</returns>
        public IFieldProperties this[string fieldName] => properties[GetFieldIndex(fieldName, true)];

        #endregion
    }
}
