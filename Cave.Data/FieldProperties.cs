using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using Cave.Data;
using Cave.IO;

namespace Cave
{
    /// <summary>
    /// Provides field properties.
    /// </summary>
    public sealed class FieldProperties
    {
        /// <summary>
        /// Creates new <see cref="FieldProperties" /> using the specified FieldInfo.
        /// </summary>
        /// <param name="sourceName">Name of the source (class, struct, table).</param>
        /// <param name="fieldInfo">The field information.</param>
        /// <exception cref="NotSupportedException">Array types (except byte[]) are not supported!.</exception>
        public static FieldProperties Create(string sourceName, FieldInfo fieldInfo)
        {
            if (fieldInfo == null)
            {
                throw new ArgumentNullException("fieldInfo");
            }

            if (string.IsNullOrEmpty(sourceName))
            {
                throw new ArgumentNullException("sourceName");
            }

            string name = fieldInfo.Name;
            Type valueType = fieldInfo.FieldType;
            DataType dataType = RowLayout.DataTypeFromType(valueType);
            DataType databaseDataType = dataType;
            FieldFlags flags = FieldFlags.None;
            string databaseName = fieldInfo.Name;
            float maximumLength = 0;
            string displayFormat = null;
            string description = null;
            DateTimeKind dateTimeKind = DateTimeKind.Unspecified;
            DateTimeType dateTimeType = DateTimeType.Undefined;
            StringEncoding stringEncoding = StringEncoding.Undefined;

            if ((dataType == DataType.User) && fieldInfo.FieldType.IsArray)
            {
                throw new NotSupportedException(string.Format("Array types (except byte[]) are not supported!\nPlease define a class with a valid ToString() member and static Parse(string) constructor instead!"));
            }

            switch (dataType)
            {
                case DataType.Enum: databaseDataType = DataType.Int64; break;
                case DataType.User: databaseDataType = DataType.String; break;
            }

            bool ignoreField = true;
            string alternativeNames = null;
            foreach (Attribute attribute in fieldInfo.GetCustomAttributes(false))
            {
                if (attribute is FieldAttribute fieldAttribute)
                {
                    if (!ignoreField)
                    {
                        throw new InvalidDataException(string.Format("Duplicate field attribute found at table {0} field {1}!", name, fieldInfo.Name));
                    }

                    maximumLength = fieldAttribute.Length;
                    if (fieldAttribute.Name != null)
                    {
                        databaseName = fieldAttribute.Name;
                    }

                    flags = fieldAttribute.Flags;
                    displayFormat = fieldAttribute.DisplayFormat;
                    ignoreField = false;
                    alternativeNames = fieldAttribute.AlternativeNames;
                    continue;
                }
                if (attribute is DescriptionAttribute descriptionAttribute)
                {
                    description = descriptionAttribute.Description;
                    continue;
                }
                if (attribute is DateTimeFormatAttribute dateTimeFormatAttribute)
                {
                    dateTimeKind = dateTimeFormatAttribute.Kind;
                    dateTimeType = dateTimeFormatAttribute.Type;
                    switch (dateTimeType)
                    {
                        case DateTimeType.BigIntTicks:
                        case DateTimeType.BigIntHumanReadable: databaseDataType = DataType.Int64; break;
                        case DateTimeType.DecimalSeconds: databaseDataType = DataType.Decimal; break;
                        case DateTimeType.DoubleSeconds: databaseDataType = DataType.Double; break;

                        case DateTimeType.Undefined:
                        case DateTimeType.Native: databaseDataType = DataType.DateTime; break;

                        default: throw new NotImplementedException(string.Format("DateTimeType {0} is not implemented!", dateTimeType));
                    }
                    continue;
                }
                if (attribute is TimeSpanFormatAttribute timeSpanFormatAttribute)
                {
                    dateTimeType = timeSpanFormatAttribute.Type;
                    switch (dateTimeType)
                    {
                        case DateTimeType.BigIntTicks:
                        case DateTimeType.BigIntHumanReadable: databaseDataType = DataType.Int64; break;
                        case DateTimeType.DecimalSeconds: databaseDataType = DataType.Decimal; break;
                        case DateTimeType.DoubleSeconds: databaseDataType = DataType.Double; break;

                        case DateTimeType.Undefined:
                        case DateTimeType.Native: databaseDataType = DataType.TimeSpan; break;

                        default: throw new NotImplementedException(string.Format("DateTimeType {0} is not implemented!", dateTimeType));
                    }
                    continue;
                }
                if (attribute is StringFormatAttribute stringFormatAttribute)
                {
                    stringEncoding = stringFormatAttribute.Encoding;
                    continue;
                }
            }
            if (databaseName == null)
            {
                databaseName = fieldInfo.Name;
            }

            return new FieldProperties(sourceName, flags, dataType, valueType, maximumLength, name, databaseDataType, dateTimeType, dateTimeKind, stringEncoding, databaseName, description, displayFormat, alternativeNames);
        }

        /// <summary>Loads fieldproperties from the specified reader.</summary>
        /// <param name="sourceName">Name of the source.</param>
        /// <param name="reader">The reader.</param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException">reader.</exception>
        /// <exception cref="TypeLoadException"></exception>
        /// <exception cref="NotSupportedException"></exception>
        public static FieldProperties Load(string sourceName, DataReader reader)
        {
            if (reader == null)
            {
                throw new ArgumentNullException("reader");
            }

            DataType dataType = (DataType)reader.Read7BitEncodedInt32();
            DataType databaseDataType = (DataType)reader.Read7BitEncodedInt32();
            FieldFlags flags = (FieldFlags)reader.Read7BitEncodedInt32();
            string name = reader.ReadString();
            string databaseName = reader.ReadString();

            string typeName = reader.ReadString();
            Type valueType = AppDom.FindType(typeName);

            if (dataType == DataType.DateTime)
            {
                DateTimeKind dateTimeKind = (DateTimeKind)reader.Read7BitEncodedInt32();
                DateTimeType dateTimeType = (DateTimeType)reader.Read7BitEncodedInt32();
                return new FieldProperties(sourceName, flags, dataType, valueType, 0, name, databaseDataType, dateTimeType, dateTimeKind, 0, databaseName, null, null, null);
            }
            if (dataType == DataType.String || dataType == DataType.User)
            {
                float m_MaximumLength = reader.ReadSingle();
                return new FieldProperties(sourceName, flags, dataType, valueType, m_MaximumLength, name, databaseDataType, 0, 0, 0, databaseName, null, null, null);
            }
            return new FieldProperties(sourceName, flags, dataType, valueType, 0, name, databaseDataType, 0, 0, 0, databaseName, null, null, null);
        }

        /// <summary>Gets the name of the log source.</summary>
        /// <value>The name of the log source.</value>
        public string LogSourceName => "FieldProperties <" + SourceName + ">";

        bool m_ParserUsed;
        MethodInfo m_StaticParse;
        ConstructorInfo m_Constructor;

        /// <summary>The name of the source of the field definition (struct, class, table).</summary>
        public readonly string SourceName;

        /// <summary>
        /// Obtains the dotnet type of the value.
        /// </summary>
        public readonly Type ValueType;

        /// <summary>
        /// Obtains the <see cref="DataType"/> of the field.
        /// </summary>
        public readonly DataType DataType;

        /// <summary>
        /// Obtains the <see cref="FieldFlags"/> of the field.
        /// </summary>
        public readonly FieldFlags Flags;

        /// <summary>
        /// Obtains the name of the field.
        /// </summary>
        public readonly string Name;

        /// <summary>
        /// Obtains the name of the field at the database.
        /// </summary>
        public readonly string NameAtDatabase;

        /// <summary>
        /// Obtains the DataType of the field at the database.
        /// </summary>
        public readonly DataType TypeAtDatabase;

        /// <summary>
        /// Provides the date time kind used at the database.
        /// </summary>
        public readonly DateTimeKind DateTimeKind;

        /// <summary>
        /// Provides the date time type used at the database (has to match the <see cref="TypeAtDatabase"/>).
        /// </summary>
        public readonly DateTimeType DateTimeType;

        /// <summary>The string encoding used at the database.</summary>
        public readonly StringEncoding StringEncoding;

        /// <summary>
        /// Obtains the maximum length of the field.
        /// </summary>
        public readonly float MaximumLength;

        /// <summary>
        /// Description of the field.
        /// </summary>
        public readonly string Description;

        /// <summary>
        /// Format arguments for displaying values.
        /// </summary>
        public readonly string DisplayFormat;

        /// <summary>The alternative names for this field.</summary>
        public readonly string AlternativeNames;

        /// <summary>
        /// Obtains the fieldinfo used to create this instance (if any).
        /// </summary>
        public readonly FieldInfo FieldInfo;

        /// <summary>Creates new <see cref="FieldProperties" /> and sets the database name and datatype to the default name and datatype.</summary>
        /// <param name="sourceName">Name of the source of the field definition (tablename, typename, ...).</param>
        /// <param name="fieldFlags">The <see cref="FieldFlags" /> of the field.</param>
        /// <param name="dataType">The <see cref="DataType" /> of the field.</param>
        /// <param name="name">The <see cref="Name" /> of the field.</param>
        public FieldProperties(string sourceName, FieldFlags fieldFlags, DataType dataType, string name)
            : this(sourceName, fieldFlags, dataType, null, 0, name, dataType, 0, 0, 0, name, null, null, null)
        {
        }

        /// <summary>
        /// Creates new <see cref="FieldProperties"/> and sets the database name and datatype to the default name and datatype.
        /// </summary>
        /// <param name="sourceName">Name of the source of the field definition (tablename, typename, ...).</param>
        /// <param name="dataType">The <see cref="DataType"/> of the field.</param>
        /// <param name="fieldFlags">The <see cref="FieldFlags"/> of the field.</param>
        /// <param name="valueType">The dotnet type of the value.</param>
        /// <param name="name">The <see cref="Name"/> of the field.</param>
        public FieldProperties(string sourceName, FieldFlags fieldFlags, DataType dataType, Type valueType, string name)
            : this(sourceName, fieldFlags, dataType, valueType, 0, name, dataType, 0, 0, 0, name, null, null, null)
        {
        }

        /// <summary>
        /// Creates new <see cref="FieldProperties"/> and sets the database name and datatype to the default name and datatype.
        /// </summary>
        /// <param name="sourceName">Name of the source of the field definition (tablename, typename, ...).</param>
        /// <param name="dataType">The <see cref="DataType"/> of the field.</param>
        /// <param name="fieldFlags">The <see cref="FieldFlags"/> of the field.</param>
        /// <param name="valueType">The dotnet type of the value.</param>
        /// <param name="maximumLength">The maximum length of the field.</param>
        /// <param name="name">The <see cref="Name"/> of the field.</param>
        /// <param name="description">The description of the field.</param>
        public FieldProperties(string sourceName, FieldFlags fieldFlags, DataType dataType, Type valueType, float maximumLength, string name, string description)
            : this(sourceName, fieldFlags, dataType, valueType, maximumLength, name, dataType, DateTimeType.Undefined, DateTimeKind.Unspecified, StringEncoding.Undefined, name, description, null, null)
        {
        }

        /// <summary>Creates new <see cref="FieldProperties" />.</summary>
        /// <param name="sourceName">Name of the source of the field definition (tablename, typename, ...).</param>
        /// <param name="fieldFlags">The <see cref="FieldFlags" /> of the field.</param>
        /// <param name="dataType">The <see cref="DataType" /> of the field.</param>
        /// <param name="valueType">The dotnet type of the value.</param>
        /// <param name="maximumLength">The maximum length of the field.</param>
        /// <param name="fieldName">The <see cref="Name" /> of the field.</param>
        /// <param name="databaseDataType">The <see cref="DataType" /> of the field at the database.</param>
        /// <param name="dateTimeType">DateTimeType used to store the value at the database.</param>
        /// <param name="dateTimeKind">DateTimeKind used to convert the local value to the database value.</param>
        /// <param name="stringEncoding">The string encoding.</param>
        /// <param name="databaseFieldName">The name of the field at the database.</param>
        /// <param name="description">The description of the field.</param>
        /// <param name="displayFormat">Provides agruments used when formatting field values with <see cref="Row.GetDisplayStrings" />.</param>
        /// <param name="alternativeNames">The alternative names.</param>
        /// <exception cref="ArgumentNullException">ValueType
        /// or
        /// ValueType
        /// or
        /// DatabaseDataType.</exception>
        /// <exception cref="NotSupportedException"></exception>
        /// <exception cref="NotImplementedException">Unknown DataType!.</exception>
        public FieldProperties(string sourceName, FieldFlags fieldFlags, DataType dataType, Type valueType, float maximumLength, string fieldName, DataType databaseDataType, DateTimeType dateTimeType, DateTimeKind dateTimeKind, StringEncoding stringEncoding, string databaseFieldName, string description, string displayFormat, string alternativeNames)
        {
            if (string.IsNullOrEmpty(sourceName))
            {
                throw new ArgumentNullException(nameof(sourceName));
            }

            if (string.IsNullOrEmpty(fieldName))
            {
                throw new ArgumentNullException(nameof(fieldName));
            }

            if (string.IsNullOrEmpty(databaseFieldName))
            {
                throw new ArgumentNullException(nameof(databaseFieldName));
            }

            SourceName = sourceName;
            DataType = dataType;
            Flags = fieldFlags;
            ValueType = valueType;
            MaximumLength = maximumLength;
            Name = fieldName;
            NameAtDatabase = databaseFieldName;
            TypeAtDatabase = databaseDataType;
            Description = description;
            DisplayFormat = displayFormat;
            DateTimeType = dateTimeType;
            DateTimeKind = dateTimeKind;
            StringEncoding = stringEncoding;
            AlternativeNames = alternativeNames;
            switch (DataType)
            {
                case DataType.Binary:
                case DataType.Bool:
                case DataType.Decimal:
                case DataType.Double:
                case DataType.Int8:
                case DataType.Int16:
                case DataType.Int32:
                case DataType.Int64:
                case DataType.UInt8:
                case DataType.UInt16:
                case DataType.UInt32:
                case DataType.UInt64:
                case DataType.Char:
                case DataType.Single:
                    break;
                case DataType.TimeSpan:
                    if (DateTimeType == DateTimeType.Undefined)
                    {
                        DateTimeType = DateTimeType.Native;
#if DEBUG
                        Trace.TraceWarning("Field {0} DateTimeType undefined! Falling back to native date time type. (Precisision may be only seconds!)", this);
#endif
                    }
                    break;
                case DataType.DateTime:
                    if (DateTimeType == DateTimeType.Undefined)
                    {
                        DateTimeType = DateTimeType.Native;
#if DEBUG
                        Trace.TraceWarning("Field {0} DateTimeType undefined! Falling back to native date time type. (Precisision may be only seconds!)", this);
#endif
                    }
                    break;
                case DataType.Enum:
                    if (null == ValueType)
                    {
                        throw new ArgumentNullException("ValueType");
                    }

                    if (TypeAtDatabase == DataType.Enum)
                    {
                        TypeAtDatabase = DataType.Int64;
#if DEBUG
                        Trace.TraceWarning("Field {0} DatabaseDataType undefined! Using DatabaseDataType {1}!", this, TypeAtDatabase);
#endif
                    }
                    break;
                case DataType.String:
                    if (StringEncoding == StringEncoding.Undefined)
                    {
                        StringEncoding = StringEncoding.UTF8;
#if DEBUG
                        Trace.TraceWarning("Field {0} StringEncoding undefined! Using StringEncoding {1}!", this, StringEncoding);
#endif
                    }
                    break;
                case DataType.User:
                    if (null == ValueType)
                    {
                        throw new ArgumentNullException("ValueType");
                    }

                    switch (TypeAtDatabase)
                    {
                        case DataType.User: TypeAtDatabase = DataType.String; Trace.TraceWarning("Field {0} DatabaseDataType undefined! Using DatabaseDataType {1}!", this, TypeAtDatabase); break;
                        case DataType.String: break;
                        default: throw new NotSupportedException(string.Format("Datatype {0} is not supported for field {1}!", TypeAtDatabase, ToString()));
                    }
                    goto case DataType.String;
                default:
                    throw new NotImplementedException("Unknown DataType!");
            }
        }

        /// <summary>Creates new <see cref="FieldProperties" /> by changing the database datatype and field name.</summary>
        /// <param name="properties">The properties.</param>
        /// <param name="dataTypeAtDatabase">The data type at database.</param>
        /// <param name="fieldNameAtDatabase">The field name at database.</param>
        /// <exception cref="System.ArgumentNullException">properties.</exception>
        public FieldProperties(FieldProperties properties, DataType dataTypeAtDatabase, string fieldNameAtDatabase = null)
        {
            if (properties == null)
            {
                throw new ArgumentNullException("properties");
            }

            SourceName = properties.SourceName;
            DataType = properties.DataType;
            Flags = properties.Flags;
            ValueType = properties.ValueType;
            MaximumLength = properties.MaximumLength;
            Name = properties.Name;
            NameAtDatabase = fieldNameAtDatabase ?? properties.NameAtDatabase;
            TypeAtDatabase = dataTypeAtDatabase;
            Description = properties.Description;
            DateTimeType = properties.DateTimeType;
            DateTimeKind = properties.DateTimeKind;
            StringEncoding = properties.StringEncoding;
            FieldInfo = properties.FieldInfo;
        }

        /// <summary>
        /// Creates new <see cref="FieldProperties"/> by changing the database datatype.
        /// </summary>
        /// <param name="properties"></param>
        /// <param name="databaseDataType"></param>
        /// <param name="maximumLength"></param>
        public FieldProperties(FieldProperties properties, DataType databaseDataType, float maximumLength)
            : this(properties, databaseDataType)
        {
            MaximumLength = maximumLength;
        }

        /// <summary>
        /// Parses a string and obtains the object by using the <see cref="ValueType" />s static Parse(string) method.
        /// </summary>
        /// <param name="text">The string to parse.</param>
        /// <param name="stringMarker">The string marker.</param>
        /// <param name="culture">The culture.</param>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException"></exception>
        /// <exception cref="NotSupportedException"></exception>
        /// <exception cref="InvalidDataException"></exception>
        /// <exception cref="NotImplementedException"></exception>
        /// <exception cref="MissingMethodException"></exception>
        public object ParseValue(string text, string stringMarker = null, CultureInfo culture = null)
        {
            if (culture == null)
            {
                culture = CultureInfo.InvariantCulture;
            }

            if (ValueType == null)
            {
                throw new InvalidOperationException(string.Format("This function requires a valid ValueType!"));
            }

            if (text == null)
            {
                return null;
            }

            if (stringMarker != null)
            {
                if (text == "null")
                {
                    return null;
                }
            }

            switch (DataType)
            {
                case DataType.TimeSpan:
                {
                    if (string.IsNullOrEmpty(text) || text == "null")
                    {
                        return default(TimeSpan);
                    }

                    switch (DateTimeType)
                    {
                        default: throw new NotSupportedException(string.Format("DateTimeType {0} is not supported", DateTimeType));

                        case DateTimeType.BigIntHumanReadable:
                            return new TimeSpan(DateTime.ParseExact(text, CaveSystemData.BigIntDateTimeFormat, culture).Ticks);

                        case DateTimeType.Undefined:
                        case DateTimeType.Native:
                            if (stringMarker != null)
                            {
                                text = text.Unbox(stringMarker, false);
                            }

                            return TimeSpan.Parse(text);

                        case DateTimeType.BigIntTicks:
                            return new TimeSpan(long.Parse(text, culture));

                        case DateTimeType.DecimalSeconds:
                            return new TimeSpan((long)decimal.Round(decimal.Parse(text, culture) * TimeSpan.TicksPerSecond));

                        case DateTimeType.DoubleSeconds:
                            return new TimeSpan((long)Math.Round(double.Parse(text, culture) * TimeSpan.TicksPerSecond));
                    }
                }

                case DataType.DateTime:
                {
                    if (string.IsNullOrEmpty(text) || text == "null")
                    {
                        return default(DateTime);
                    }

                    switch (DateTimeType)
                    {
                        default: throw new NotSupportedException(string.Format("DateTimeType {0} is not supported", DateTimeType));

                        case DateTimeType.BigIntHumanReadable:
                            return DateTime.ParseExact(text, CaveSystemData.BigIntDateTimeFormat, culture);

                        case DateTimeType.Undefined:
                        case DateTimeType.Native:
                            if (stringMarker != null)
                            {
                                text = text.Unbox(stringMarker, false);
                            }

                            return DateTime.ParseExact(text, StringExtensions.InterOpDateTimeFormat, culture);

                        case DateTimeType.BigIntTicks:
                            return new DateTime(long.Parse(text, culture), DateTimeKind);

                        case DateTimeType.DecimalSeconds:
                            return new DateTime((long)decimal.Round(decimal.Parse(text, culture) * TimeSpan.TicksPerSecond), DateTimeKind);

                        case DateTimeType.DoubleSeconds:
                            return new DateTime((long)Math.Round(double.Parse(text, culture) * TimeSpan.TicksPerSecond), DateTimeKind);
                    }
                }
                case DataType.Binary:
                {
                    if (string.IsNullOrEmpty(text) || text == "null")
                    {
                        return null;
                    }

                    if (stringMarker != null)
                    {
                        text = text.Unbox(stringMarker, false);
                    }

                    return Base64.NoPadding.Decode(text);
                }
                case DataType.Bool:
                    if (text.Length == 0)
                    {
                        return false;
                    }

                    return (text.ToLower() == "true" || text.ToLower() == "yes" || text == "1");
                case DataType.Single:
                    if (text.Length == 0)
                    {
                        return 0f;
                    }

                    return float.Parse(text, culture);
                case DataType.Double:
                    if (text.Length == 0)
                    {
                        return 0d;
                    }

                    return double.Parse(text, culture);
                case DataType.Decimal:
                    if (text.Length == 0)
                    {
                        return 0m;
                    }

                    return decimal.Parse(text, culture);
                case DataType.Int8:
                    if (text.Length == 0)
                    {
                        return (sbyte)0;
                    }

                    return (sbyte.Parse(text, culture));
                case DataType.Int16:
                    if (text.Length == 0)
                    {
                        return (short)0;
                    }

                    return (short.Parse(text, culture));
                case DataType.Int32:
                    if (text.Length == 0)
                    {
                        return 0;
                    }

                    return (int.Parse(text, culture));
                case DataType.Int64:
                    if (text.Length == 0)
                    {
                        return 0L;
                    }

                    return (long.Parse(text, culture));
                case DataType.UInt8:
                    if (text.Length == 0)
                    {
                        return (byte)0;
                    }

                    return (byte.Parse(text, culture));
                case DataType.UInt16:
                    if (text.Length == 0)
                    {
                        return (ushort)0;
                    }

                    return (ushort.Parse(text, culture));
                case DataType.UInt32:
                    if (text.Length == 0)
                    {
                        return 0U;
                    }

                    return (uint.Parse(text, culture));
                case DataType.UInt64:
                    if (text.Length == 0)
                    {
                        return 0UL;
                    }

                    return (ulong.Parse(text, culture));
                case DataType.Enum:
                    if (stringMarker != null)
                    {
                        text = text.Unbox(stringMarker, false);
                    }

                    if (text.Length == 0)
                    {
                        text = "0";
                    }

                    return Enum.Parse(ValueType, text, true);

                case DataType.Char:
                    if (stringMarker != null)
                    {
                        text = text.Unbox(stringMarker, false).Unescape();
                    }

                    if (text.Length != 1)
                    {
                        throw new InvalidDataException();
                    }

                    return (text[0]);

                case DataType.String:
                    if (stringMarker != null)
                    {
                        text = text.Unbox(stringMarker, false).Unescape();
                    }

                    return text;

                case DataType.User: break;

                default: throw new NotImplementedException();
            }

            if (stringMarker != null)
            {
                text = text.Unbox(stringMarker, false).Unescape();
            }

            if (!m_ParserUsed)
            {
                // lookup static Parse(string) method first
                m_StaticParse = ValueType.GetMethod("Parse", BindingFlags.Public | BindingFlags.Static, null, new Type[] { typeof(string) }, null);

                // if there is none, search constructor(string)
                if (m_StaticParse == null)
                {
                    m_Constructor = ValueType.GetConstructor(new Type[] { typeof(string) });
                }
                m_ParserUsed = true;
            }

            // has static Parse(string) ?
            if (m_StaticParse != null)
            {
                // use method to parse value
                return m_StaticParse.Invoke(null, new object[] { text });
            }

            // has constructor(string) ?
            if (m_Constructor != null)
            {
                return m_Constructor.Invoke(new object[] { text });
            }
            throw new MissingMethodException(string.Format("Could not find a way to parse or create {0} from string!", ValueType));
        }

        /// <summary>
        /// Retrieves a string for the specified value. The string may be parsed back to a value using <see cref="ParseValue(string, string, CultureInfo)" />.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <param name="stringMarker">The string marker.</param>
        /// <param name="jsonMode">if set to <c>true</c> [json mode].</param>
        /// <param name="culture">The culture.</param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        /// <exception cref="InvalidDataException">
        /// </exception>
        public string GetString(object value, string stringMarker, bool jsonMode, CultureInfo culture = null)
        {
            if (culture == null)
            {
                culture = CultureInfo.InvariantCulture;
            }

            if (value == null)
            {
                return stringMarker == null ? null : "null";
            }
            switch (DataType)
            {
                case DataType.DateTime:
                {
                    DateTime dt = (DateTime)value;
                    switch (DateTimeKind)
                    {
                        case DateTimeKind.Utc: dt = dt.ToUniversalTime(); break;
                        case DateTimeKind.Local: dt = dt.ToLocalTime(); break;
                    }
                    if (jsonMode)
                    {
                        // javascript uses time/date in milliseconds. var timeInMsec = new Date() - pastDate;
                        return (dt - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind)).TotalMilliseconds.ToString(culture);
                    }
                    switch (DateTimeType)
                    {
                        default: throw new NotSupportedException(string.Format("DateTimeType {0} is not supported", DateTimeType));
                        case DateTimeType.BigIntHumanReadable:
                            if (dt <= DateTime.MinValue)
                            {
                                return "0";
                            }

                            return dt.ToString(CaveSystemData.BigIntDateTimeFormat, culture).TrimStart('0');
                        case DateTimeType.Native: return dt.ToString(StringExtensions.InterOpDateTimeFormat).Box(stringMarker);
                        case DateTimeType.BigIntTicks: return dt.Ticks.ToString();
                        case DateTimeType.DecimalSeconds: return (dt.Ticks / (decimal)TimeSpan.TicksPerSecond).ToString(culture);
                        case DateTimeType.DoubleSeconds: return (dt.Ticks / (double)TimeSpan.TicksPerSecond).ToString(culture);
                    }
                }
                case DataType.Binary:
                {
                    byte[] data = (byte[])value;
                    return data.Length == 0 ? "\"\"" : Base64.NoPadding.Encode(data).Box(stringMarker);
                }
                case DataType.Bool:
                {
                    return ((bool)value) ? "true" : "false";
                }
                case DataType.TimeSpan:
                {
                    // json does not support 64bit integers, so we transmit it as string
                    return jsonMode ? ((TimeSpan)value).TotalMilliseconds.ToString(culture) : ((TimeSpan)value).Ticks.ToString();
                }
                case DataType.Single:
                {
                    float f = ((float)value);
                    if (float.IsNaN(f) || float.IsInfinity(f))
                    {
                        throw new InvalidDataException(string.Format("Cannot serialize float with value {0}!", f));
                    }

                    return f.ToString(culture);
                }
                case DataType.Double:
                {
                    double d = ((double)value);
                    if (double.IsNaN(d) || double.IsInfinity(d))
                    {
                        throw new InvalidDataException(string.Format("Cannot serialize double with value {0}!", d));
                    }

                    return d.ToString(culture);
                }
                case DataType.Decimal: return ((decimal)value).ToString(culture);
                case DataType.Int8: return ((sbyte)value).ToString(culture);
                case DataType.Int16: return ((short)value).ToString(culture);
                case DataType.Int32: return ((int)value).ToString(culture);
                case DataType.Int64:
                {
                    // json does not support 64bit integers, so we transmit it as string
                    return jsonMode ? ((long)value).ToString(culture).Box(stringMarker) : ((long)value).ToString(culture);
                }
                case DataType.UInt8: return ((byte)value).ToString(culture);
                case DataType.UInt16: return ((ushort)value).ToString(culture);
                case DataType.UInt32: return ((uint)value).ToString(culture);
                case DataType.UInt64:
                {
                    // json does not support 64bit integers, so we transmit it as string
                    return jsonMode ? ((long)value).ToString(culture).Box(stringMarker) : ((ulong)value).ToString(culture);
                }
                case DataType.Enum: return value.ToString().Box(stringMarker);
                case DataType.Char: return value.ToString().Box(stringMarker);

                case DataType.String:
                case DataType.User:
                default: break;
            }

            string s = (value is IConvertible) ? ((IConvertible)value).ToString(culture) : value.ToString();
            return s.Escape().Box(stringMarker);
        }

        /// <summary>
        /// Obtains an enum value from the specified long value.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public object EnumValue(long value)
        {
            if (ValueType == null)
            {
                throw new InvalidOperationException(string.Format("This function requires a valid ValueType!"));
            }

            // handle enum
            if (DataType != DataType.Enum)
            {
                throw new ArgumentException(string.Format("DataType is not an enum!"));
            }

            return Enum.ToObject(ValueType, value);
        }

        /// <summary>
        /// Obtains the dot net type name of the field.
        /// </summary>
        public string DotNetTypeName
        {
            get
            {
                switch (DataType)
                {
                    case DataType.Binary: return "byte[]";
                    case DataType.Bool: return "bool";
                    case DataType.DateTime: return "DateTime";
                    case DataType.Decimal: return "decimal";
                    case DataType.Double: return "double";
                    case DataType.Int16: return "short";
                    case DataType.Int32: return "int";
                    case DataType.Int64: return "long";
                    case DataType.Int8: return "sbyte";
                    case DataType.Single: return "float";
                    case DataType.String: return "string";
                    case DataType.TimeSpan: return "TimeSpan";
                    case DataType.UInt16: return "ushort";
                    case DataType.UInt32: return "uint";
                    case DataType.UInt64: return "ulong";
                    case DataType.UInt8: return "byte";
                    case DataType.Char: return "char";

                    default:
                        // case DataType.User:
                        // case DataType.Enum:
                        if (ValueType != null)
                        {
                            return ValueType.Name;
                        }

                        return "unknown " + DataType.ToString();
                }
            }
        }

        /// <summary>Saves the fieldproperties to the specified writer.</summary>
        /// <param name="writer">The writer.</param>
        /// <exception cref="NotSupportedException"></exception>
        public void Save(DataWriter writer)
        {
            if (writer == null)
            {
                throw new ArgumentNullException("Writer");
            }

            writer.Write7BitEncoded32((int)DataType);
            writer.Write7BitEncoded32((int)TypeAtDatabase);
            writer.Write7BitEncoded32((int)Flags);
            writer.WritePrefixed(Name);
            writer.WritePrefixed(NameAtDatabase);
            string typeName = ValueType.AssemblyQualifiedName.Substring(0, ValueType.AssemblyQualifiedName.IndexOf(','));
            writer.WritePrefixed(typeName);
            if (DataType == DataType.DateTime)
            {
                writer.Write7BitEncoded32((int)DateTimeKind);
                writer.Write7BitEncoded32((int)DateTimeType);
            }
            if (DataType == DataType.String || DataType == DataType.User)
            {
                writer.Write(MaximumLength);
            }
        }



        /// <summary>
        /// Obtains the hashcode for the instance.
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            return ToString().GetHashCode();
        }

        /// <summary>
        /// Checks another FieldProperties instance for equality.
        /// </summary>
        /// <param name="other">The FieldProperties to check for equality.</param>
        /// <returns>Returns true if the other instance equals this one, false otherwise.</returns>
        public bool Equals(FieldProperties other)
        {
            if (other == null)
            {
                return false;
            }

            // check name
            if ((other.Name != Name) && (other.Name != NameAtDatabase) && (other.NameAtDatabase != Name))
            {
                char[] splitters = " ,;".ToCharArray();
                if (true == AlternativeNames?.Split(splitters, StringSplitOptions.RemoveEmptyEntries).Any(n => n == other.Name || n == other.NameAtDatabase))
                {
                    return true;
                }

                return true == other.AlternativeNames?.Split(splitters, StringSplitOptions.RemoveEmptyEntries).Any(n => n == Name || n == NameAtDatabase);
            }

            // check flags
            if ((other.Flags & FieldFlags.MatchMask) != (Flags & FieldFlags.MatchMask))
            {
                return false;
            }

            // additional checks
            if (FieldInfo != null)
            {
                if (other.FieldInfo != null)
                {
                    // both typed, full match needed
                    return (other.ValueType == ValueType);
                }

                // only this typed, other is db -> check conversions
                switch (DataType)
                {
                    case DataType.TimeSpan:
                    case DataType.DateTime:
                        switch (DateTimeType)
                        {
                            case DateTimeType.BigIntTicks:
                            case DateTimeType.BigIntHumanReadable: return (other.DataType == DataType.Int64);
                            case DateTimeType.DecimalSeconds: return (other.DataType == DataType.Decimal);
                            case DateTimeType.DoubleSeconds: return (other.DataType == DataType.Double);
                            case DateTimeType.Undefined:
                            case DateTimeType.Native: return (other.DataType == DataType.DateTime);
                            default: return false;
                        }
                }
                return true;
            }

            if (other.FieldInfo == null)
            {
                // TODO check if this is enough
                if (DataType == other.DataType)
                {
                    return true;
                }

                return TypeAtDatabase == other.TypeAtDatabase;
            }

            // only other typed, other is db -> check conversions
            switch (other.DataType)
            {
                case DataType.TimeSpan:
                case DataType.DateTime:
                    switch (other.DateTimeType)
                    {
                        case DateTimeType.BigIntTicks:
                        case DateTimeType.BigIntHumanReadable: return (DataType == DataType.Int64);
                        case DateTimeType.DecimalSeconds: return (DataType == DataType.Decimal);
                        case DateTimeType.DoubleSeconds: return (DataType == DataType.Double);
                        case DateTimeType.Undefined:
                        case DateTimeType.Native: return (DataType == DataType.DateTime);
                        default: throw new NotImplementedException(string.Format("Missing implementation for DateTimeType {0}", other.DateTimeType));
                    }
            }
            return true;
        }

        /// <summary>
        /// Checks another FieldProperties instance for equality.
        /// </summary>
        /// <param name="obj">The FieldProperties to check for equality.</param>
        /// <returns>Returns true if the other instance equals this one, false otherwise.</returns>
        public override bool Equals(object obj)
        {
            return obj is FieldProperties other ? Equals(other) : false;
        }

        /// <summary>Returns a <see cref="System.String" /> that represents this instance.</summary>
        /// <returns>A <see cref="System.String" /> that represents this instance.</returns>
        public override string ToString()
        {
            StringBuilder result = new StringBuilder();
            if (0 != Flags)
            {
                result.AppendFormat("[{0}] ", Flags);
            }
            switch (DataType)
            {
                case DataType.DateTime:
                    result.AppendFormat("DateTime {0}.{1}", DateTimeType, DateTimeKind);
                    break;
                default:
                    result.Append(DotNetTypeName);
                    break;
            }
            result.AppendFormat(" {0}.{1}", SourceName, Name);
            if ((MaximumLength > 0) && (MaximumLength < int.MaxValue))
            {
                result.AppendFormat(" [{0}]", MaximumLength);
            }

            return result.ToString();
        }
    }
}
