using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.IO;
using System.Reflection;

namespace Cave.Data
{
    /// <summary>
    /// Provides static functions for struct field reflections.
    /// </summary>
    public static class Fields
    {
        /// <summary>
        /// Checks whether a field has the <see cref="FieldAttribute"/> and returns the name of the field.
        /// </summary>
        /// <param name="member">The field / property info.</param>
        /// <returns>The name specified with the field attribute or null.</returns>
        public static string GetName(MemberInfo member)
        {
            if (member == null)
            {
                throw new ArgumentNullException("fieldInfo");
            }

            foreach (var attribute in member.GetCustomAttributes(true))
            {
                if (attribute is FieldAttribute fieldAttribute)
                {
                    return !string.IsNullOrEmpty(fieldAttribute.Name) ? fieldAttribute.Name : member.Name;
                }
            }
            return null;
        }

        /// <summary>
        /// Checks whether a field has the <see cref="FieldAttribute"/> and returns the length of the field.
        /// </summary>
        /// <param name="member">The field / property info.</param>
        /// <returns>The length specified with the field attribute or 0.</returns>
        public static uint GetLength(MemberInfo member)
        {
            if (member == null)
            {
                throw new ArgumentNullException("fieldInfo");
            }

            foreach (var attribute in member.GetCustomAttributes(true))
            {
                if (attribute is FieldAttribute fieldAttribute)
                {
                    return fieldAttribute.Length;
                }
            }
            return 0;
        }

        /// <summary>
        /// Checks whether a field has the <see cref="FieldAttribute"/> and returns the flags of the field.
        /// </summary>
        /// <param name="member">The field / property info.</param>
        /// <returns>The flags specified with the field attribute or <see cref="FieldFlags.None"/>.</returns>
        public static FieldFlags GetFlags(MemberInfo member)
        {
            if (member == null)
            {
                throw new ArgumentNullException("fieldInfo");
            }

            foreach (var attribute in member.GetCustomAttributes(true))
            {
                if (attribute is FieldAttribute fieldAttribute)
                {
                    return fieldAttribute.Flags;
                }
            }
            return FieldFlags.None;
        }

        /// <summary>
        /// Gets the description of a field.
        /// If the attribute is not present null is returned.
        /// </summary>
        /// <param name="member">The field / property info.</param>
        /// <returns>The description specified with the field attribute or null.</returns>
        public static string GetDescription(MemberInfo member)
        {
            if (member == null)
            {
                throw new ArgumentNullException("fieldInfo");
            }

            foreach (var attribute in member.GetCustomAttributes(false))
            {
                if (attribute is DescriptionAttribute descriptionAttribute)
                {
                    return descriptionAttribute.Description;
                }
            }
            return null;
        }

        /// <summary>Gets the description of a specified enum or field.</summary>
        /// <param name="value">The enum or field value.</param>
        /// <returns>The description if present or null otherwise.</returns>
        /// <exception cref="ArgumentNullException">Value.</exception>
        /// <exception cref="ArgumentException">Enum value is not defined!.</exception>
        public static string GetDescription(object value)
        {
            if (value == null)
            {
                throw new ArgumentNullException("value");
            }

            Type type = value.GetType();
            if (type.IsEnum)
            {
                if (!Enum.IsDefined(type, value))
                {
                    return null;
                }

                var name = Enum.GetName(value.GetType(), value);
                return GetDescription(type.GetField(name));
            }
            foreach (var attribute in type.GetCustomAttributes(false))
            {
                if (attribute is DescriptionAttribute descriptionAttribute)
                {
                    return descriptionAttribute.Description;
                }
            }
            return null;
        }

        /// <summary>
        /// Converts a (primitive) value to the desired type.
        /// </summary>
        /// <param name="toType">Type to convert to.</param>
        /// <param name="value">Value to convert.</param>
        /// <param name="cultureInfo">The culture to use during formatting.</param>
        /// <returns>An object converted to the specified type.</returns>
        public static object ConvertPrimitive(Type toType, object value, IFormatProvider cultureInfo)
        {
            try
            {
                return Convert.ChangeType(value, toType, cultureInfo);
            }
            catch (Exception ex)
            {
                throw new NotSupportedException(string.Format("The value '{0}' cannot be converted to target type '{1}'!", value, toType), ex);
            }
        }

        /// <summary>
        /// Converts a value to the desired field value.
        /// </summary>
        /// <typeparam name="T">The field type.</typeparam>
        /// <param name="value">The value.</param>
        /// <param name="provider">The format provider to use during formatting.</param>
        /// <returns>An object converted to the specified type.</returns>
        public static T ConvertValue<T>(object value, IFormatProvider provider = null) => (T)ConvertValue(typeof(T), value, provider);

        /// <summary>
        /// Converts a value to the desired field value.
        /// </summary>
        /// <param name="fieldType">The field type.</param>
        /// <param name="value">The value.</param>
        /// <param name="provider">The format provider to use during formatting.</param>
        /// <returns>An object converted to the specified type.</returns>
        public static object ConvertValue(Type fieldType, object value, IFormatProvider provider = null)
        {
            if (fieldType == null)
            {
                throw new ArgumentNullException(nameof(fieldType));
            }

            if (provider == null)
            {
                throw new ArgumentNullException(nameof(provider));
            }

            if (fieldType.Name.StartsWith("Nullable"))
            {
                if (value == null)
                {
                    return null;
                }
#if NET45 || NET46 || NET47 || NETSTANDARD20
                fieldType = fieldType.GenericTypeArguments[0];
#elif NET20 || NET35 || NET40
                fieldType = fieldType.GetGenericArguments()[0];
#else
#error No code defined for the current framework or NETXX version define missing!
#endif
            }
            else if (value == null)
            {
                if (fieldType.IsValueType)
                {
                    return Activator.CreateInstance(fieldType);
                }
                else
                {
                    return null;
                }
            }

            if (fieldType == typeof(bool))
            {
                switch (value.ToString().ToLower())
                {
                    case "true":
                    case "on":
                    case "yes":
                    case "1":
                        return true;
                    case "":
                    case "false":
                    case "off":
                    case "no":
                    case "0":
                        return false;
                }
            }
            if (fieldType.IsPrimitive)
            {
                return ConvertPrimitive(fieldType, value, provider);
            }

            if (fieldType.IsAssignableFrom(value.GetType()))
            {
                return Convert.ChangeType(value, fieldType);
            }

            if (fieldType.IsEnum)
            {
                return Enum.Parse(fieldType, value.ToString(), true);
            }

            // convert to string
            string str;
            {
                if (value is string)
                {
                    str = (string)value;
                }
                else
                {
                    // try to find public ToString(IFormatProvider) method in class
                    MethodInfo method = value.GetType().GetMethod("ToString", BindingFlags.Public | BindingFlags.Instance, null, new Type[] { typeof(IFormatProvider) }, null);
                    if (method != null)
                    {
                        try
                        {
                            str = (string)method.Invoke(value, new object[] { provider });
                        }
                        catch (TargetInvocationException ex)
                        {
                            throw ex.InnerException;
                        }
                    }
                    else
                    {
                        str = value.ToString();
                    }
                }
            }
            if (fieldType == typeof(string))
            {
                return str;
            }

            if (fieldType == typeof(DateTime))
            {
                if (long.TryParse(str, out var ticks))
                {
                    return new DateTime(ticks, DateTimeKind.Unspecified);
                }

                if (DateTimeParser.TryParseDateTime(str, out DateTime dt))
                {
                    return dt;
                }
            }
            if (fieldType == typeof(TimeSpan))
            {
                try
                {
                    if (str.Contains(":"))
                    {
                        return TimeSpan.Parse(str);
                    }
                    if (str.EndsWith("ms"))
                    {
                        return new TimeSpan((long)Math.Round(double.Parse(str.SubstringEnd(1)) * TimeSpan.TicksPerMillisecond));
                    }
                    return str.EndsWith("s")
                        ? new TimeSpan((long)Math.Round(double.Parse(str.SubstringEnd(1)) * TimeSpan.TicksPerSecond))
                        : (object)new TimeSpan(long.Parse(str));
                }
                catch (Exception ex)
                {
                    throw new InvalidDataException(string.Format("Value '{0}' is not a valid TimeSpan!", str), ex);
                }
            }

            // parse from string
            {
                // try to find public static Parse(string, IFormatProvider) method in class
                var errors = new List<Exception>();
                MethodInfo method = fieldType.GetMethod("Parse", BindingFlags.Public | BindingFlags.Static, null, new Type[] { typeof(string), typeof(IFormatProvider) }, null);
                if (method != null)
                {
                    try
                    {
                        return method.Invoke(null, new object[] { str, provider });
                    }
                    catch (TargetInvocationException ex)
                    {
                        errors.Add(ex.InnerException);
                    }
                }
                method = fieldType.GetMethod("Parse", BindingFlags.Public | BindingFlags.Static, null, new Type[] { typeof(string) }, null);
                if (method != null)
                {
                    try
                    {
                        return method.Invoke(null, new object[] { str });
                    }
                    catch (TargetInvocationException ex)
                    {
                        errors.Add(ex.InnerException);
                    }
                }
                if (errors.Count > 0)
                {
                    throw new AggregateException(errors.ToArray());
                }

                throw new MissingMethodException(string.Format("Type {0} has no public static Parse(string, IFormatProvider) or Parse(string) method!", fieldType));
            }
        }

        /// <summary>
        /// Sets all fieldvalues of a struct/class object.
        /// </summary>
        /// <param name="obj">structure object.</param>
        /// <param name="fields">fields to be set.</param>
        /// <param name="values">values to set.</param>
        /// <param name="cultureInfo">The culture to use during formatting.</param>
        public static void SetValues(ref object obj, IList<FieldInfo> fields, IList<object> values, CultureInfo cultureInfo)
        {
            if (obj == null)
            {
                throw new ArgumentNullException("obj");
            }

            if (fields == null)
            {
                throw new ArgumentNullException("fields");
            }

            if (values == null)
            {
                throw new ArgumentNullException("values");
            }

            if (cultureInfo == null)
            {
                throw new ArgumentNullException("cultureInfo");
            }

            for (var i = 0; i < values.Count; i++)
            {
                FieldInfo fieldInfo = fields[i];
                var value = ConvertValue(fieldInfo.FieldType, values[i], cultureInfo);
                fields[i].SetValue(obj, value);
            }
        }

        /// <summary>
        /// Gets an array containing all values of the specified fields.
        /// </summary>
        /// <param name="fields">The fields to read.</param>
        /// <param name="structure">The structure to read fields from.</param>
        /// <returns>An array containing all values.</returns>
        public static object[] GetValues(IList<FieldInfo> fields, object structure)
        {
            if (fields == null)
            {
                throw new ArgumentNullException(nameof(fields));
            }

            var result = new object[fields.Count];
            for (var i = 0; i < fields.Count; i++)
            {
                result[i] = fields[i].GetValue(structure);
            }
            return result;
        }

        /// <summary>
        /// Gets an array containing all values of the specified fields.
        /// </summary>
        /// <param name="properties">The properties to read.</param>
        /// <param name="obj">The object to read properties from.</param>
        /// <returns>An array containing all values.</returns>
        public static object[] GetValues(IList<PropertyInfo> properties, object obj)
        {
            if (properties == null)
            {
                throw new ArgumentNullException(nameof(properties));
            }

            var result = new object[properties.Count];
            for (var i = 0; i < properties.Count; i++)
            {
#if NET20 || NET35 || NET40
                result[i] = properties[i].GetValue(obj, null);
#else
                result[i] = properties[i].GetValue(obj);
#endif
            }
            return result;
        }

        /// <summary>
        /// Gets a string for the specified field value.
        /// </summary>
        /// <remarks>
        /// The result of this function can be used by <see cref="ConvertValue(Type, object, IFormatProvider)"/>.
        /// </remarks>
        /// <param name="value">The value.</param>
        /// <param name="datatype">The datatype.</param>
        /// <param name="dateTimeKind">The date time kind (optional).</param>
        /// <param name="dateTimeType">The date time type (optional).</param>
        /// <param name="stringMarker">The string marker.</param>
        /// <param name="provider">The format provider.</param>
        /// <returns>Returns a string describing the value.</returns>
        public static string GetString(object value, DataType datatype, DateTimeKind dateTimeKind = default, DateTimeType dateTimeType = default, string stringMarker = null, IFormatProvider provider = null)
        {
            if (provider == null)
            {
                provider = CultureInfo.InvariantCulture;
            }
            if (stringMarker == null)
            {
                stringMarker = string.Empty;
            }
            if (value == null)
            {
                return "null";
            }
            switch (datatype)
            {
                case DataType.DateTime:
                {
                    var dt = (DateTime)value;
                    switch (dateTimeKind)
                    {
                        case DateTimeKind.Utc: dt = dt.ToUniversalTime(); break;
                        case DateTimeKind.Local: dt = dt.ToLocalTime(); break;
                    }
                    switch (dateTimeType)
                    {
                        default: throw new NotSupportedException($"DateTimeType {dateTimeType} is not supported");
                        case DateTimeType.BigIntHumanReadable: return dt.ToString(Storage.BigIntDateTimeFormat, provider).TrimStart('0');
                        case DateTimeType.Native: return dt.ToString(StringExtensions.InterOpDateTimeFormat, provider).Box(stringMarker);
                        case DateTimeType.BigIntTicks: return dt.Ticks.ToString(provider);
                        case DateTimeType.DecimalSeconds: return (dt.Ticks / (decimal)TimeSpan.TicksPerSecond).ToString(provider);
                        case DateTimeType.DoubleSeconds: return (dt.Ticks / (double)TimeSpan.TicksPerSecond).ToString(provider);
                        case DateTimeType.DoubleEpoch: return ((dt.Ticks - Storage.EpochTicks) / (double)TimeSpan.TicksPerSecond).ToString(provider);
                    }
                }
                case DataType.Binary:
                {
                    var data = (byte[])value;
                    return (data.Length == 0 ? string.Empty : Base64.NoPadding.Encode(data)).Box(stringMarker);
                }
                case DataType.Bool:
                {
                    return (bool)value ? "true" : "false";
                }
                case DataType.TimeSpan:
                {
                    var ts = (TimeSpan)value;
                    switch (dateTimeType)
                    {
                        default: throw new NotSupportedException($"DateTimeType {dateTimeType} is not supported.");

                        case DateTimeType.BigIntHumanReadable:
                            return new DateTime(ts.Ticks).ToString(Storage.BigIntDateTimeFormat, provider);

                        case DateTimeType.Undefined:
                        case DateTimeType.Native:
                        {
                            var text = ts.ToString();
                            if (stringMarker != null)
                            {
                                text = text.Box(stringMarker);
                            }
                            return text;
                        }
                        case DateTimeType.BigIntTicks:
                            return ts.Ticks.ToString(provider);

                        case DateTimeType.DecimalSeconds:
                            return $"{(ts.Ticks / (decimal)TimeSpan.TicksPerSecond).ToString(provider)}";

                        case DateTimeType.DoubleSeconds:
                            return ts.TotalSeconds.ToString("R", provider);
                    }
                }
                case DataType.Single:
                {
                    var f = (float)value;
                    if (float.IsNaN(f) || float.IsInfinity(f))
                    {
                        throw new InvalidDataException($"Cannot serialize float with value {f}!");
                    }

                    return f.ToString(provider);
                }
                case DataType.Double:
                {
                    var d = (double)value;
                    if (double.IsNaN(d) || double.IsInfinity(d))
                    {
                        throw new InvalidDataException($"Cannot serialize double with value {d}!");
                    }

                    return d.ToString(provider);
                }
                case DataType.Decimal: return ((decimal)value).ToString(provider);
                case DataType.Int8: return ((sbyte)value).ToString(provider);
                case DataType.Int16: return ((short)value).ToString(provider);
                case DataType.Int32: return ((int)value).ToString(provider);
                case DataType.Int64: return ((long)value).ToString(provider);

                case DataType.UInt8: return ((byte)value).ToString(provider);
                case DataType.UInt16: return ((ushort)value).ToString(provider);
                case DataType.UInt32: return ((uint)value).ToString(provider);
                case DataType.UInt64: return ((ulong)value).ToString(provider);

                case DataType.Enum: return value.ToString().Box(stringMarker);
                case DataType.Char: return value.ToString().Box(stringMarker);

                case DataType.String:
                case DataType.User:
                default: break;
            }

            var s = value is IConvertible ? ((IConvertible)value).ToString(provider) : value.ToString();
            return s.EscapeUtf8().Box(stringMarker);
        }
    }
}
