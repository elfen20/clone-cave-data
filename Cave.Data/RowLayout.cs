using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using Cave.IO;

namespace Cave.Data
{
    /// <summary>
    /// Provides a row layout implementation
    /// </summary>
    public sealed class RowLayout
    {
        /// <summary>
        /// Checks two layouts for equality
        /// </summary>
        /// <param name="expected">The expected layout</param>
        /// <param name="current">The layout to check</param>
        public static void CheckLayout(RowLayout expected, RowLayout current, Func<FieldProperties, FieldProperties> fieldPropertiesConversion = null)
        {
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
                throw new InvalidDataException(string.Format("Fieldcount of table {0} differs (found {1} expected {2})!", current.Name, current.FieldCount, expected.FieldCount));
            }
            for (int i = 0; i < expected.FieldCount; i++)
            {
                FieldProperties expectedField = expected.GetProperties(i);
                FieldProperties currentField = current.GetProperties(i);
                if (fieldPropertiesConversion != null)
                {
                    expectedField = fieldPropertiesConversion(expectedField);
                    currentField = fieldPropertiesConversion(currentField);
                }
                if (!expectedField.Equals(currentField))
                {
                    throw new InvalidDataException(string.Format("Fieldproperties of table {0} differ! (found {1} expected {2})!", current.Name, currentField, expectedField));
                }
            }
        }

        /// <summary>Loads the row layout from the specified reader.</summary>
        /// <param name="reader">The reader.</param>
        /// <returns></returns>
        public static RowLayout Load(DataReader reader)
        {
            if (reader == null)
            {
                throw new ArgumentNullException("Reader");
            }

            int count = reader.Read7BitEncodedInt32();
            string name = reader.ReadString();
            List<FieldProperties> fieldProperties = new List<FieldProperties>(count);
            for (int i = 0; i < count; i++)
            {
                fieldProperties.Add(FieldProperties.Load(name, reader));
            }
            return new RowLayout(name, fieldProperties.ToArray());
        }

        /// <summary>Gets the name of the log source.</summary>
        /// <value>The name of the log source.</value>
        public string LogSourceName => "RowLayout";

        /// <summary>Creates an alien row layout without using any field properies.</summary>
        /// <param name="type">Type to parse fields from.</param>
        /// <param name="onlyPublic">if set to <c>true</c> [use only public].</param>
        /// <returns>Returns a new RowLayout instance</returns>
        /// <exception cref="Exception"></exception>
        public static RowLayout CreateAlien(Type type, bool onlyPublic)
        {
            if (type == null)
            {
                throw new ArgumentNullException("Type");
            }

            BindingFlags bindingFlags = BindingFlags.Public | BindingFlags.Instance;
            if (onlyPublic)
            {
                bindingFlags |= BindingFlags.NonPublic;
            }

            FieldInfo[] rawInfos = type.GetFields(bindingFlags);
            List<FieldProperties> properties = new List<FieldProperties>(rawInfos.Length);
            List<FieldInfo> infos = new List<FieldInfo>(rawInfos.Length);
            foreach (FieldInfo fieldInfo in rawInfos)
            {
                try
                {
                    if (fieldInfo.FieldType.IsArray)
                    {
                        continue;
                    }

                    FieldProperties field = FieldProperties.Create(type.Name, fieldInfo);
                    properties.Add(field);
                    infos.Add(fieldInfo);
                }
                catch (Exception ex)
                {
                    throw new InvalidDataException(string.Format("Error while loading field properties of type {0} field {1}!", type.FullName, fieldInfo), ex);
                }
            }
            return new RowLayout(type.Name, properties.ToArray(), infos.ToArray(), -1, type);
        }

        /// <summary>
        /// Creates a new layout with the given name and field properties
        /// </summary>
        /// <param name="name">Name of the layout</param>
        /// <param name="fields">FieldProperties to use</param>
        /// <returns>
        /// Returns a new RowLayout instance
        /// </returns>
        public static RowLayout CreateUntyped(string name, params FieldProperties[] fields)
        {
            return new RowLayout(name, fields);
        }

        /// <summary>Creates a RowLayout instance for the specified struct</summary>
        /// <param name="type">The type to build the rowlayout for</param>
        /// <param name="excludedFields">The excluded fields.</param>
        /// <returns>Returns a new RowLayout instance</returns>
        /// <exception cref="ArgumentException"></exception>
        /// <exception cref="Exception">Cannot add multiple ID fields!
        /// or
        /// Error while loading field properties of type {0} field {1}!</exception>
        public static RowLayout CreateTyped(Type type, string[] excludedFields)
        {
            return CreateTyped(type, null, null, excludedFields);
        }

        /// <summary>Creates a RowLayout instance for the specified struct</summary>
        /// <param name="type">The type to build the rowlayout for</param>
        /// <param name="nameOverride">The table name override.</param>
        /// <param name="storage">The Storage engine to use</param>
        /// <param name="excludedFields">The excluded fields.</param>
        /// <returns>Returns a new RowLayout instance</returns>
        /// <exception cref="ArgumentException"></exception>
        /// <exception cref="Exception">Cannot add multiple ID fields!
        /// or
        /// Error while loading field properties of type {0} field {1}!</exception>
        public static RowLayout CreateTyped(Type type, string nameOverride = null, IStorage storage = null, params string[] excludedFields)
        {
            if (type == null)
            {
                throw new ArgumentNullException("Type");
            }

            int idFieldIndex = -1;
            bool l_IsStruct = type.IsValueType && !type.IsEnum && !type.IsPrimitive;
            if (!l_IsStruct)
            {
                throw new ArgumentException(string.Format("Type {0} is not a struct! Only structs may be used as row definition!", type));
            }

            string tableName = TableAttribute.GetName(type);
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

            FieldInfo[] rawInfos = type.GetFields(BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance);
            List<FieldProperties> properties = new List<FieldProperties>(rawInfos.Length);
            List<FieldInfo> fieldInfos = new List<FieldInfo>(rawInfos.Length);
            foreach (FieldInfo fieldInfo in rawInfos)
            {
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
                    FieldProperties field = FieldProperties.Create(tableName, fieldInfo);

                    if (storage != null)
                    {
                        field = storage.GetDatabaseFieldProperties(field);
                    }
                    if ((field.Flags & FieldFlags.ID) != 0)
                    {
                        if (idFieldIndex != -1)
                        {
                            throw new InvalidOperationException(string.Format("Cannot add multiple ID fields!"));
                        }

                        idFieldIndex = properties.Count;
                    }
                    properties.Add(field);
                    fieldInfos.Add(fieldInfo);
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException(string.Format("Error while loading field properties of type {0} field {1}!", type.FullName, fieldInfo), ex);
                }
            }
            return new RowLayout(tableName, properties.ToArray(), fieldInfos.ToArray(), idFieldIndex, type);
        }

        /// <summary>
        /// Obtains the <see cref="DataType"/> for a given <see cref="Type"/>
        /// </summary>
        /// <param name="type">The <see cref="Type"/> to convert</param>
        /// <returns></returns>
        public static DataType DataTypeFromType(Type type)
        {
            if (type == null)
            {
                throw new ArgumentNullException("Type");
            }

            if (type == typeof(sbyte)) { return DataType.Int8; }
            if (type == typeof(byte)) { return DataType.UInt8; }
            if (type == typeof(short)) { return DataType.Int16; }
            if (type == typeof(ushort)) { return DataType.UInt16; }
            if (type == typeof(int)) { return DataType.Int32; }
            if (type == typeof(uint)) { return DataType.UInt32; }
            if (type == typeof(long)) { return DataType.Int64; }
            if (type == typeof(ulong)) { return DataType.UInt64; }
            if (type == typeof(char)) { return DataType.Char; }
            if (type == typeof(string)) { return DataType.String; }
            if (type == typeof(float)) { return DataType.Single; }
            if (type == typeof(double)) { return DataType.Double; }
            if (type == typeof(bool)) { return DataType.Bool; }
            if (type == typeof(decimal)) { return DataType.Decimal; }
            if (type == typeof(byte[])) { return DataType.Binary; }
            if (type == typeof(TimeSpan)) { return DataType.TimeSpan; }
            if (type == typeof(DateTime)) { return DataType.DateTime; }
            if (type.IsEnum) { return DataType.Enum; }
            return DataType.User;
        }

        /// <summary>Gets the string representing the specified value using the field properties.</summary>
        /// <param name="field">The field number.</param>
        /// <param name="value">The value.</param>
        /// <returns></returns>
        public string GetDisplayString(int field, object value)
        {
            FieldProperties fp = m_Properties[field];
            if (fp.DisplayFormat == "FormatTimeSpan")
            {
                switch (fp.DataType)
                {
                    case DataType.TimeSpan: return StringExtensions.FormatTime((TimeSpan)value);
                    default: return StringExtensions.FormatTime(Convert.ToDouble(value));
                }
            }
            if (fp.DisplayFormat == "FormatValue")
            {
                return StringExtensions.FormatSize(Convert.ToDecimal(value));
            }
            if (fp.DisplayFormat == "FormatBinaryValue")
            {
                return StringExtensions.FormatBinarySize(Convert.ToDecimal(value));
            }
            switch (fp.DataType)
            {
                case DataType.Int8: return ((sbyte)value).ToString(fp.DisplayFormat);
                case DataType.Int16: return ((short)value).ToString(fp.DisplayFormat);
                case DataType.Int32: return ((int)value).ToString(fp.DisplayFormat);
                case DataType.Int64: return ((long)value).ToString(fp.DisplayFormat);
                case DataType.UInt8: return ((byte)value).ToString(fp.DisplayFormat);
                case DataType.UInt16: return ((ushort)value).ToString(fp.DisplayFormat);
                case DataType.UInt32: return ((uint)value).ToString(fp.DisplayFormat);
                case DataType.UInt64: return ((ulong)value).ToString(fp.DisplayFormat);
                case DataType.Binary: return Base64.NoPadding.Encode((byte[])value);
                case DataType.DateTime: return ((DateTime)value).ToString(fp.DisplayFormat);
                case DataType.Single: return ((float)value).ToString(fp.DisplayFormat);
                case DataType.Double: return ((double)value).ToString(fp.DisplayFormat);
                case DataType.Decimal: return ((decimal)value).ToString(fp.DisplayFormat);
                default: return value == null ? "" : value.ToString();
            }
        }

        FieldProperties[] m_Properties;
        FieldInfo[] m_Infos;

        /// <summary>
        /// Provides direct access to the field properties
        /// </summary>
        public FieldProperties GetProperties(int field) { return m_Properties[field]; }

        /// <summary>
        /// Provides direct access to the field properties
        /// </summary>
        public FieldProperties GetProperties(string fieldName) { return m_Properties[GetFieldIndex(fieldName)]; }

        /// <summary>
        /// Provides direct access to the field infos
        /// </summary>
        public FieldInfo GetInfo(int field) { return m_Infos[field]; }

        /// <summary>
        /// Obtains the name of the layout
        /// </summary>
        public readonly string Name;

        /// <summary>
        /// Obtains whether the layout was created from a typed struct or not
        /// </summary>
        public bool IsTyped => RowType != null;

        /// <summary>The row type</summary>
        public readonly Type RowType;

        /// <summary>
        /// Obtains the fieldcount
        /// </summary>
        public readonly int FieldCount;

        /// <summary>
        /// Obtains the index of the ID field
        /// </summary>
        public readonly int IDFieldIndex = -1;

        /// <summary>
        /// Obtains the name of the ID field
        /// </summary>
        public readonly FieldProperties IDField;

        /// <summary>Initializes a new undefined instance of the <see cref="RowLayout"/> class.</summary>
        public RowLayout()
        {
            m_Properties = new FieldProperties[0];
            m_Infos = new FieldInfo[0];
            Name = "Undefined";
        }

        /// <summary>
        /// Creates a RowLayout instance for the specified fields.
        /// </summary>
        /// <param name="name">The Name of the Layout</param>
        /// <param name="fields">The fieldproperties to use</param>
        RowLayout(string name, FieldProperties[] fields)
        {
            if (name.HasInvalidChars(ASCII.Strings.SafeName))
            {
                throw new ArgumentException("Invalid characters at table name!");
            }

            FieldCount = fields.Length;
            m_Properties = new FieldProperties[FieldCount];
            m_Infos = new FieldInfo[FieldCount];
            for (int i = 0; i < FieldCount; i++)
            {
                m_Properties[i] = fields[i];
                if ((fields[i].Flags & FieldFlags.ID) != 0)
                {
                    if (IDFieldIndex < 0)
                    {
                        IDFieldIndex = i;
                        IDField = m_Properties[i];
                    }
                }
            }
            Name = name;
        }

        /// <summary>Parses the value.</summary>
        /// <param name="fieldNumber">The field number.</param>
        /// <param name="value">The value.</param>
        /// <param name="stringMarker">The string marker.</param>
        /// <param name="culture">The culture.</param>
        /// <returns></returns>
        public object ParseValue(int fieldNumber, string value, string stringMarker, CultureInfo culture = null)
        {
            FieldProperties field = GetProperties(fieldNumber);
            return field.ParseValue(value, stringMarker, culture);
        }

        /// <summary>Enums the value.</summary>
        /// <param name="fieldNumber">The field number.</param>
        /// <param name="value">The value.</param>
        /// <returns></returns>
        public object EnumValue(int fieldNumber, long value)
        {
            FieldProperties field = GetProperties(fieldNumber);
            return Enum.Parse(field.ValueType, value.ToString(), true);
        }

        /// <summary>Initializes a new typed instance of the <see cref="RowLayout" /> class.</summary>
        /// <param name="name">Name of the layout.</param>
        /// <param name="properties">The field properties.</param>
        /// <param name="infos">The field infos.</param>
        /// <param name="idFieldIndex">Index of the identifier field.</param>
        /// <param name="rowType">Type of the row.</param>
        /// <exception cref="System.ArgumentNullException">Name</exception>
        /// <exception cref="System.ArgumentException">Invalid characters at table name!</exception>
        /// <exception cref="InvalidDataException">FieldCount does not match!</exception>
        RowLayout(string name, FieldProperties[] properties, FieldInfo[] infos, int idFieldIndex, Type rowType)
        {
            if (string.IsNullOrEmpty(name))
            {
                throw new ArgumentNullException("Name");
            }

            if (name.HasInvalidChars(ASCII.Strings.SafeName))
            {
                throw new ArgumentException("Invalid characters at table name!");
            }

            Name = name;
            RowType = rowType;
            m_Properties = properties;
            m_Infos = infos;
            IDFieldIndex = idFieldIndex;
            IDField = (IDFieldIndex < 0) ? null : m_Properties[IDFieldIndex];
            FieldCount = properties.Length;
            if (infos.Length != FieldCount)
            {
                throw new InvalidDataException("FieldCount does not match!");
            }
        }

        /// <summary>
        /// Obtains the ID of the dataset from the specified struct
        /// </summary>
        /// <param name="item">The struct to read the ID from</param>
        /// <returns>Returns the ID of the dataset</returns>
        public long GetID<T>(T item) where T : struct
        {
            if (IDFieldIndex < 0)
            {
                throw new DataException($"{this}: ID Field cannot not be found");
            }

            return (long)GetValue(IDFieldIndex, item);
        }

        /// <summary>Gets the identifier.</summary>
        /// <param name="row">The row.</param>
        /// <returns></returns>
        /// <exception cref="DataException"></exception>
        public long GetID(Row row)
        {
            if (IDFieldIndex < 0)
            {
                throw new DataException($"{this}: ID Field cannot not be found");
            }

            object value = row.GetValue(IDFieldIndex);
            if (value is long)
            {
                return (long)value;
            }

            return Convert.ToInt64(value);
        }

        /// <summary>
        /// Sets the ID of the dataset at the specified boxed struct
        /// </summary>
        /// <param name="item">The struct to set the ID at</param>
        /// <param name="id">The ID of the dataset</param>
        public void SetID(ref object item, long id)
        {
            if (IDFieldIndex < 0)
            {
                throw new DataException($"{this}: ID Field cannot not be found");
            }

            SetValue(IDFieldIndex, ref item, id);
        }

        /// <summary>
        /// Sets the ID of the dataset at the specified boxed struct
        /// </summary>
        /// <param name="item">The struct to set the ID at</param>
        /// <param name="id">The ID of the dataset</param>
        public void SetID<T>(ref T item, long id)
        {
            if (IDFieldIndex < 0)
            {
                throw new DataException($"{this}: ID Field cannot not be found");
            }

            object box = item;
            SetValue(IDFieldIndex, ref box, id);
            item = (T)box;
        }

        /// <summary>
        /// Gets the name of the field with the given number
        /// </summary>
        /// <param name="field"></param>
        /// <returns></returns>
        public string GetName(int field)
        {
            return m_Properties[field].Name;
        }

        /// <summary>
        /// Obtains the value of a field from the specified struct
        /// </summary>
        /// <param name="field">The field number/index</param>
        /// <param name="item">The struct to read the value from</param>
        /// <returns>Returns the value of the specified field</returns>
        public object GetValue(int field, object item)
        {
            if (!IsTyped)
            {
                throw new InvalidOperationException(string.Format("This RowLayout was not created from a typed struct!"));
            }

            return m_Infos[field].GetValue(item);
        }

        /// <summary>
        /// Sets a value at the specified struct
        /// </summary>
        /// <param name="field">The field number/index</param>
        /// <param name="item">The struct to set the value at</param>
        /// <param name="value">The value to set</param>
        public void SetValue(int field, ref object item, object value)
        {
            if (!IsTyped)
            {
                throw new InvalidOperationException(string.Format("This RowLayout was not created from a typed struct!"));
            }

            if (value != null && value.GetType() != m_Properties[field].ValueType)
            {
                value = Convert.ChangeType(value, m_Properties[field].ValueType);
            }
            m_Infos[field].SetValue(item, value);
        }

        /// <summary>
        /// Obtains all values of the struct
        /// </summary>
        /// <param name="item">The struct to get the values from</param>
        /// <returns>Returns all values of the struct</returns>
        public object[] GetValues<T>(T item) where T : struct
        {
            if (!IsTyped)
            {
                throw new InvalidOperationException(string.Format("This RowLayout was not created from a typed struct!"));
            }

            object[] result = new object[FieldCount];
            for (int i = 0; i < FieldCount; i++)
            {
                result[i] = m_Infos[i].GetValue(item);
            }
            return result;
        }

        /// <summary>
        /// Sets all values of the struct
        /// </summary>
        /// <param name="item">The struct to set the values at</param>
        /// <param name="values">The values to set</param>
        public void SetValues(ref object item, object[] values)
        {
            if (values == null)
            {
                throw new ArgumentNullException("Values");
            }

            if (!IsTyped)
            {
                throw new InvalidOperationException(string.Format("This RowLayout was not created from a typed struct!"));
            }

            for (int i = 0; i < FieldCount; i++)
            {
                object value = values[i];
                FieldProperties field = m_Properties[i];
                if (value != null && value.GetType() != field.ValueType)
                {
                    switch (field.DataType)
                    {
                        case DataType.User: value = m_Properties[i].ParseValue(value.ToString(), null); break;
                        case DataType.Enum: value = Enum.Parse(field.ValueType, value.ToString(), true); break;
                        default: value = Convert.ChangeType(values[i], m_Properties[i].ValueType); break;
                    }
                }
                m_Infos[i].SetValue(item, value);
            }
        }

        /// <summary>
        /// Obtains the field index of the specified field name
        /// </summary>
        /// <param name="field">The fieldname to search for</param>
        /// <returns>Returns the field index of the specified field name</returns>
        public int GetFieldIndex(string field)
        {
            for (int i = 0; i < FieldCount; i++)
            {
                if (m_Properties[i].Name.Equals(field))
                {
                    return i;
                }
            }
            for (int i = 0; i < FieldCount; i++)
            {
                string[] names = m_Properties[i].AlternativeNames?.Split(" ,;".ToCharArray(), StringSplitOptions.RemoveEmptyEntries);
                if (names != null && names.Any(n => n == field))
                {
                    return i;
                }
            }
            return -1;
        }

        /// <summary>Creates a copy of this layout without the specified field.</summary>
        /// <param name="fieldName">Name of the field to remove.</param>
        /// <returns>Returns a new <see cref="RowLayout"/> instance</returns>
        public RowLayout Remove(string fieldName)
        {
            int index = GetFieldIndex(fieldName);
            List<FieldProperties> fieldProperties = new List<FieldProperties>();
            List<FieldInfo> fieldInfos = new List<FieldInfo>();
            for (int i = 0; i < FieldCount; i++)
            {
                if (i == index)
                {
                    continue;
                }

                fieldProperties.Add(m_Properties[i]);
                fieldInfos.Add(m_Infos[i]);
            }
            return new RowLayout(Name, fieldProperties.ToArray(), fieldInfos.ToArray(), IDFieldIndex, RowType);
        }

        /// <summary>
        /// Checks another RowLayout for equality with this one
        /// </summary>
        /// <param name="layout"></param>
        /// <returns></returns>
        public bool Equals(RowLayout layout)
        {
            if (layout == null)
            {
                throw new ArgumentNullException("Layout");
            }

            if (layout.FieldCount != FieldCount)
            {
                return false;
            }

            for (int i = 0; i < FieldCount; i++)
            {
                if (!layout.m_Properties[i].Equals(m_Properties[i]))
                {
                    return false;
                }
            }
            return true;
        }

        /// <summary>
        /// Checks another RowLayout for equality with this one
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object obj)
        {
            if (obj is RowLayout layout)
            {
                return Equals(layout);
            }

            return false;
        }

        /// <summary>
        /// Obtains the hash code for this instance
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            int result = IsTyped ? 0x00001234 : 0x12345678;
            for (int i = 0; i < FieldCount; i++)
            {
                result ^= m_Properties[i].GetHashCode() ^ i;
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
            for (int i = 0; i < FieldCount; i++)
            {
                m_Properties[i].Save(writer);
            }
        }

        /// <summary>Gets the fields.</summary>
        /// <value>The fields.</value>
        public ICollection<FieldProperties> Fields => new ReadOnlyCollection<FieldProperties>(m_Properties);

        /// <summary>Returns a <see cref="string" /> that represents this instance.</summary>
        /// <returns>A <see cref="string" /> that represents this instance.</returns>
        public override string ToString()
        {
            return string.Format("RowLayout [{0}] {1}", FieldCount, Name);
        }
    }
}
