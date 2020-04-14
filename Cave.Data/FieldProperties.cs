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
    public class FieldProperties : IFieldProperties
    {
        bool parserInitialized;
        MethodInfo staticParse;
        ConstructorInfo constructor;

        #region IFieldProperties properties

        /// <inheritdoc/>
        public bool IsNullable { get; set; }

        /// <inheritdoc/>
        public int Index { get; set; }

        /// <inheritdoc/>
        public Type ValueType { get; set; }

        /// <inheritdoc/>
        public DataType DataType { get; set; }

        /// <inheritdoc/>
        public FieldFlags Flags { get; set; }

        /// <inheritdoc/>
        public string Name { get; set; }

        /// <inheritdoc/>
        public string NameAtDatabase { get; set; }

        /// <inheritdoc/>
        public DataType TypeAtDatabase { get; set; }

        /// <inheritdoc/>
        public DateTimeKind DateTimeKind { get; set; }

        /// <inheritdoc/>
        public DateTimeType DateTimeType { get; set; }

        /// <inheritdoc/>
        public StringEncoding StringEncoding { get; set; }

        /// <inheritdoc/>
        public float MaximumLength { get; set; }

        /// <inheritdoc/>
        public string Description { get; set; }

        /// <inheritdoc/>
        public string DisplayFormat { get; set; }

        /// <inheritdoc/>
        public string AlternativeNames { get; set; }

        /// <inheritdoc/>
        public FieldInfo FieldInfo { get; set; }

        /// <inheritdoc/>
        public object DefaultValue { get; set; }

        /// <inheritdoc/>
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

                        return $"unknown datatype {DataType}";
                }
            }
        }

        #endregion

        #region public functions

        #region IFieldProperties

        /// <inheritdoc/>
        public FieldProperties Clone()
        {
            return new FieldProperties()
            {
                Index = Index,
                ValueType = ValueType,
                DataType = DataType,
                Flags = Flags,
                Name = Name,
                NameAtDatabase = NameAtDatabase,
                TypeAtDatabase = TypeAtDatabase,
                DateTimeKind = DateTimeKind,
                DateTimeType = DateTimeType,
                StringEncoding = StringEncoding,
                MaximumLength = MaximumLength,
                Description = Description,
                DisplayFormat = DisplayFormat,
                AlternativeNames = AlternativeNames,
                FieldInfo = FieldInfo,
                DefaultValue = DefaultValue,
            };
        }

        #endregion

        /// <inheritdoc/>
        public object ParseValue(string text, string stringMarker = null, IFormatProvider provider = null)
        {
            if (provider == null)
            {
                provider = CultureInfo.InvariantCulture;
            }

            if (ValueType == null)
            {
                throw new InvalidOperationException("This function requires a valid ValueType!");
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
                        return IsNullable ? null : (object)default(TimeSpan);
                    }

                    switch (DateTimeType)
                    {
                        default: throw new NotSupportedException($"DateTimeType {DateTimeType} is not supported.");

                        case DateTimeType.BigIntHumanReadable:
                            return new TimeSpan(DateTime.ParseExact(text, Storage.BigIntDateTimeFormat, provider).Ticks);

                        case DateTimeType.Undefined:
                        case DateTimeType.Native:
                            if (stringMarker != null)
                            {
                                text = text.Unbox(stringMarker, false);
                            }
#if NET20 || NET35
                            return TimeSpan.Parse(text);
#else
                            return TimeSpan.Parse(text, provider);
#endif
                        case DateTimeType.BigIntTicks:
                            return new TimeSpan(long.Parse(text, provider));

                        case DateTimeType.DecimalSeconds:
                            return new TimeSpan((long)decimal.Round(decimal.Parse(text, provider) * TimeSpan.TicksPerSecond));

                        case DateTimeType.DoubleSeconds:
                        {
                            var value = double.Parse(text, provider) * TimeSpan.TicksPerSecond;
                            var longValue = (long)value;
                            if (value > 0 && longValue < 0)
                            {
                                Trace.WriteLine("DoubleSeconds exceeded (long) range. Overflow detected!");
                                longValue = long.MaxValue;
                            }
                            else if (value < 0 && longValue > 0)
                            {
                                Trace.WriteLine("DoubleSeconds exceeded (long) range. Overflow detected!");
                                longValue = long.MinValue;
                            }
                            return new TimeSpan(longValue);
                        }
                    }
                }

                case DataType.DateTime:
                {
                    if (string.IsNullOrEmpty(text) || text == "null")
                    {
                        return IsNullable ? null : (object)default(DateTime);
                    }

                    switch (DateTimeType)
                    {
                        default: throw new NotSupportedException($"DateTimeType {DateTimeType} is not supported.");

                        case DateTimeType.BigIntHumanReadable:
                            return DateTime.ParseExact(text, Storage.BigIntDateTimeFormat, provider);

                        case DateTimeType.Undefined:
                        case DateTimeType.Native:
                            if (stringMarker != null)
                            {
                                text = text.Unbox(stringMarker, false);
                            }

                            return DateTime.ParseExact(text, StringExtensions.InterOpDateTimeFormat, provider);

                        case DateTimeType.BigIntTicks:
                            return new DateTime(long.Parse(text, provider), DateTimeKind);

                        case DateTimeType.DecimalSeconds:
                            return new DateTime((long)decimal.Round(decimal.Parse(text, provider) * TimeSpan.TicksPerSecond), DateTimeKind);

                        case DateTimeType.DoubleSeconds:
                            return new DateTime((long)Math.Round(double.Parse(text, provider) * TimeSpan.TicksPerSecond), DateTimeKind);

                        case DateTimeType.DoubleEpoch:
                            return new DateTime((long)Math.Round(double.Parse(text, provider) * TimeSpan.TicksPerSecond) + Storage.EpochTicks, DateTimeKind);
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
                        return IsNullable ? null : (object)false;
                    }
                    return text.ToUpperInvariant() == "TRUE" || text.ToUpperInvariant() == "YES" || text == "1";
                case DataType.Single:
                    if (text.Length == 0)
                    {
                        return IsNullable ? null : (object)0f;
                    }
                    return float.Parse(text, provider);
                case DataType.Double:
                    if (text.Length == 0)
                    {
                        return IsNullable ? null : (object)0d;
                    }
                    return double.Parse(text, provider);
                case DataType.Decimal:
                    if (text.Length == 0)
                    {
                        return IsNullable ? null : (object)0m;
                    }
                    return decimal.Parse(text, provider);
                case DataType.Int8:
                    if (text.Length == 0)
                    {
                        return IsNullable ? null : (object)(sbyte)0;
                    }
                    return sbyte.Parse(text, provider);
                case DataType.Int16:
                    if (text.Length == 0)
                    {
                        return IsNullable ? null : (object)(short)0;
                    }
                    return short.Parse(text, provider);
                case DataType.Int32:
                    if (text.Length == 0)
                    {
                        return IsNullable ? null : (object)(int)0;
                    }
                    return int.Parse(text, provider);
                case DataType.Int64:
                    if (text.Length == 0)
                    {
                        return IsNullable ? null : (object)0L;
                    }
                    return long.Parse(text, provider);
                case DataType.UInt8:
                    if (text.Length == 0)
                    {
                        return IsNullable ? null : (object)(byte)0;
                    }
                    return byte.Parse(text, provider);
                case DataType.UInt16:
                    if (text.Length == 0)
                    {
                        return IsNullable ? null : (object)(ushort)0;
                    }
                    return ushort.Parse(text, provider);
                case DataType.UInt32:
                    if (text.Length == 0)
                    {
                        return IsNullable ? null : (object)0U;
                    }
                    return uint.Parse(text, provider);
                case DataType.UInt64:
                    if (text.Length == 0)
                    {
                        return IsNullable ? null : (object)0UL;
                    }
                    return ulong.Parse(text, provider);
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
                    return text[0];

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

            if (!parserInitialized)
            {
                // lookup static Parse(string) method first
                staticParse = ValueType.GetMethod("Parse", BindingFlags.Public | BindingFlags.Static, null, new Type[] { typeof(string) }, null);

                // if there is none, search constructor(string)
                if (staticParse == null)
                {
                    constructor = ValueType.GetConstructor(new Type[] { typeof(string) });
                }
                parserInitialized = true;
            }

            // has static Parse(string) ?
            if (staticParse != null)
            {
                // use method to parse value
                return staticParse.Invoke(null, new object[] { text });
            }

            // has constructor(string) ?
            if (constructor != null)
            {
                return constructor.Invoke(new object[] { text });
            }
            throw new MissingMethodException($"Could not find a way to parse or create {ValueType} from string!");
        }

        /// <inheritdoc/>
        public string GetString(object value, string stringMarker = null, IFormatProvider provider = null)
            => Fields.GetString(value, DataType, DateTimeKind, DateTimeType, stringMarker, provider);

        /// <inheritdoc/>
        public object EnumValue(long value)
        {
            if (ValueType == null)
            {
                throw new InvalidOperationException($"This function requires a valid ValueType!");
            }

            // handle enum only
            if (DataType != DataType.Enum)
            {
                throw new ArgumentException($"DataType is not an enum!");
            }

            return Enum.ToObject(ValueType, value);
        }

        /// <summary>
        /// Checks properties and sets needed but unset settings.
        /// </summary>
        public void Check()
        {
            if (TypeAtDatabase == 0)
            {
                TypeAtDatabase = DataType;
            }

            if (NameAtDatabase == null)
            {
                NameAtDatabase = Name;
            }

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
                    if (ValueType == null)
                    {
                        throw new InvalidOperationException($"Property {nameof(ValueType)} required!");
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
                    break;
                case DataType.User:
                    if (ValueType == null)
                    {
                        throw new InvalidOperationException($"Property {nameof(ValueType)} required!");
                    }

                    switch (TypeAtDatabase)
                    {
                        case DataType.User: TypeAtDatabase = DataType.String; Trace.TraceWarning("Field {0} DatabaseDataType undefined! Using DatabaseDataType {1}!", this, TypeAtDatabase); break;
                        case DataType.String: break;
                        default: throw new NotSupportedException($"Datatype {TypeAtDatabase} is not supported for field {this}!");
                    }
                    goto case DataType.String;
                default:
                    throw new NotImplementedException("Unknown DataType!");
            }
        }

        /// <summary>
        /// Loads field properties using the specified FieldInfo.
        /// </summary>
        /// <param name="index">Field index.</param>
        /// <param name="fieldInfo">The field information.</param>
        /// <exception cref="NotSupportedException">Array types (except byte[]) are not supported!.</exception>
        public void LoadFieldInfo(int index, FieldInfo fieldInfo)
        {
            FieldInfo = fieldInfo ?? throw new ArgumentNullException(nameof(fieldInfo));
            Index = index;

            Name = fieldInfo.Name;
            ValueType = fieldInfo.FieldType;

            var realType = Nullable.GetUnderlyingType(ValueType);
            if (realType != null)
            {
                IsNullable = true;
                ValueType = realType;
            }

            DataType = RowLayout.DataTypeFromType(ValueType);
            NameAtDatabase = fieldInfo.Name;
            TypeAtDatabase = DataType;
            Flags = FieldFlags.None;
            MaximumLength = 0;
            DisplayFormat = null;
            Description = null;
            DateTimeKind = DateTimeKind.Unspecified;
            DateTimeType = DateTimeType.Undefined;
            StringEncoding = StringEncoding.Undefined;
            AlternativeNames = null;
            DefaultValue = null;

            if ((DataType == DataType.User) && fieldInfo.FieldType.IsArray)
            {
                throw new NotSupportedException($"Array types (except byte[]) are not supported!\nPlease define a class with a valid ToString() member and static Parse(string) constructor instead!");
            }

            switch (DataType)
            {
                case DataType.Enum: TypeAtDatabase = DataType.Int64; break;
                case DataType.User: TypeAtDatabase = DataType.String; break;
            }

            foreach (Attribute attribute in fieldInfo.GetCustomAttributes(false))
            {
                if (attribute is FieldAttribute fieldAttribute)
                {
                    MaximumLength = fieldAttribute.Length;
                    if (fieldAttribute.Name != null)
                    {
                        NameAtDatabase = fieldAttribute.Name;
                    }

                    Flags = fieldAttribute.Flags;
                    DisplayFormat = fieldAttribute.DisplayFormat;
                    AlternativeNames = fieldAttribute.AlternativeNames;
                    continue;
                }
                if (attribute is DescriptionAttribute descriptionAttribute)
                {
                    Description = descriptionAttribute.Description;
                    continue;
                }
                if (attribute is DateTimeFormatAttribute dateTimeFormatAttribute)
                {
                    DateTimeKind = dateTimeFormatAttribute.Kind;
                    DateTimeType = dateTimeFormatAttribute.Type;
                    switch (DateTimeType)
                    {
                        case DateTimeType.BigIntTicks:
                        case DateTimeType.BigIntHumanReadable: TypeAtDatabase = DataType.Int64; break;
                        case DateTimeType.DecimalSeconds: TypeAtDatabase = DataType.Decimal; break;
                        case DateTimeType.DoubleSeconds: TypeAtDatabase = DataType.Double; break;
                        case DateTimeType.DoubleEpoch: TypeAtDatabase = DataType.Double; break;

                        case DateTimeType.Undefined:
                        case DateTimeType.Native: TypeAtDatabase = DataType.DateTime; break;

                        default: throw new NotImplementedException($"DateTimeType {DateTimeType} is not implemented!");
                    }
                    continue;
                }
                if (attribute is TimeSpanFormatAttribute timeSpanFormatAttribute)
                {
                    DateTimeType = timeSpanFormatAttribute.Type;
                    switch (DateTimeType)
                    {
                        case DateTimeType.BigIntTicks:
                        case DateTimeType.BigIntHumanReadable: TypeAtDatabase = DataType.Int64; break;
                        case DateTimeType.DecimalSeconds: TypeAtDatabase = DataType.Decimal; break;
                        case DateTimeType.DoubleSeconds: TypeAtDatabase = DataType.Double; break;

                        case DateTimeType.Undefined:
                        case DateTimeType.Native: TypeAtDatabase = DataType.TimeSpan; break;

                        default: throw new NotImplementedException($"DateTimeType {DateTimeType} is not implemented!");
                    }
                    continue;
                }
                if (attribute is StringFormatAttribute stringFormatAttribute)
                {
                    StringEncoding = stringFormatAttribute.Encoding;
                    continue;
                }
                if (attribute is DefaultValueAttribute defaultValueAttribute)
                {
                    DefaultValue = defaultValueAttribute.Value;
                }
            }
            if (NameAtDatabase == null)
            {
                NameAtDatabase = Name;
            }
            Check();
        }

        /// <summary>Loads fieldproperties from the specified reader.</summary>
        /// <param name="reader">The reader.</param>
        /// <param name="index">Field index.</param>
        public void Load(DataReader reader, int index)
        {
            if (reader == null)
            {
                throw new ArgumentNullException(nameof(reader));
            }

            DataType = (DataType)reader.Read7BitEncodedInt32();
            TypeAtDatabase = (DataType)reader.Read7BitEncodedInt32();
            Flags = (FieldFlags)reader.Read7BitEncodedInt32();
            Name = reader.ReadString();
            NameAtDatabase = reader.ReadString();
            Index = index;

            var typeName = reader.ReadString();
            ValueType = Type.GetType(typeName, true);

            if (DataType == DataType.DateTime)
            {
                DateTimeKind = (DateTimeKind)reader.Read7BitEncodedInt32();
                DateTimeType = (DateTimeType)reader.Read7BitEncodedInt32();
            }
            if (DataType == DataType.String || DataType == DataType.User)
            {
                MaximumLength = reader.ReadSingle();
            }
            Check();
        }

        #endregion

        #region public overrides

        /// <summary>
        /// Checks another FieldProperties instance for equality.
        /// </summary>
        /// <param name="obj">The FieldProperties to check for equality.</param>
        /// <returns>Returns true if the other instance equals this one, false otherwise.</returns>
        public override bool Equals(object obj)
        {
            return obj is FieldProperties other ? Equals(other) : false;
        }

        /// <summary>
        /// Gets the hashcode for the instance.
        /// </summary>
        /// <returns>Hashcode for the field.</returns>
        public override int GetHashCode()
        {
            return ToString().GetHashCode();
        }

        /// <summary>
        /// Checks another FieldProperties instance for equality.
        /// </summary>
        /// <param name="other">The FieldProperties to check for equality.</param>
        /// <returns>Returns true if the other instance equals this one, false otherwise.</returns>
        public bool Equals(IFieldProperties other)
        {
            if (other == null)
            {
                return false;
            }

            // check name
            if ((other.Name != Name) && (other.NameAtDatabase != NameAtDatabase))
            {
                var splitters = " ,;".ToCharArray();
                if (AlternativeNames?.Split(splitters, StringSplitOptions.RemoveEmptyEntries).Any(n => n == other.Name || n == other.NameAtDatabase) == true)
                {
                    return true;
                }

                return other.AlternativeNames?.Split(splitters, StringSplitOptions.RemoveEmptyEntries).Any(n => n == Name || n == NameAtDatabase) == true;
            }

            // additional checks
            if (FieldInfo != null)
            {
                if (other.FieldInfo != null)
                {
                    // both typed, full match needed
                    return other.ValueType == ValueType;
                }

                // only this typed, other is db -> check conversions
                switch (DataType)
                {
                    case DataType.TimeSpan:
                    case DataType.DateTime:
                        switch (DateTimeType)
                        {
                            case DateTimeType.BigIntTicks:
                            case DateTimeType.BigIntHumanReadable: return other.DataType == DataType.Int64;
                            case DateTimeType.DecimalSeconds: return other.DataType == DataType.Decimal;
                            case DateTimeType.DoubleSeconds: return other.DataType == DataType.Double;
                            case DateTimeType.DoubleEpoch: return other.DataType == DataType.Double;

                            case DateTimeType.Undefined:
                            case DateTimeType.Native: return other.DataType == DataType;
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
                        case DateTimeType.BigIntHumanReadable: return DataType == DataType.Int64;
                        case DateTimeType.DecimalSeconds: return DataType == DataType.Decimal;
                        case DateTimeType.DoubleSeconds: return DataType == DataType.Double;
                        case DateTimeType.Undefined:
                        case DateTimeType.Native: return other.DataType == DataType;
                        default:
                            Trace.TraceError($"Missing implementation for DateTimeType {other.DateTimeType}");
                            return false;
                    }
            }
            return true;
        }

        /// <summary>Returns a <see cref="string" /> that represents this instance.</summary>
        /// <returns>A <see cref="string" /> that represents this instance.</returns>
        public override string ToString()
        {
            var result = new StringBuilder();
            if (Flags != 0)
            {
                result.Append($"[{Flags}] ");
            }
            result.Append(DotNetTypeName);

            if (IsNullable)
            {
                result.Append(" nullable");
            }

            if (StringEncoding != default)
            {
                result.Append($" {StringEncoding}");
            }

            if (DateTimeType != default)
            {
                result.Append($" {DateTimeType}");
            }

            if (DateTimeKind != default)
            {
                result.Append($" {DateTimeKind}");
            }

            result.Append($" {Name}");
            if ((MaximumLength > 0) && (MaximumLength < int.MaxValue))
            {
                result.Append($" ({MaximumLength})");
            }
            return result.ToString();
        }
        #endregion
    }
}
