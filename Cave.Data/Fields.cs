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
        /// <param name="fieldInfo"></param>
        /// <returns></returns>
        public static string GetName(MemberInfo fieldInfo)
        {
            if (fieldInfo == null)
            {
                throw new ArgumentNullException("fieldInfo");
            }

            foreach (object attribute in fieldInfo.GetCustomAttributes(true))
            {
                FieldAttribute fieldAttribute = attribute as FieldAttribute;
                if (fieldAttribute != null)
                {
                    return !string.IsNullOrEmpty(fieldAttribute.Name) ? fieldAttribute.Name : fieldInfo.Name;
                }
            }
            return null;
        }

        /// <summary>
        /// Checks whether a field has the <see cref="FieldAttribute"/> and returns the length of the field.
        /// </summary>
        /// <param name="fieldInfo"></param>
        /// <returns></returns>
        public static uint GetLength(MemberInfo fieldInfo)
        {
            if (fieldInfo == null)
            {
                throw new ArgumentNullException("fieldInfo");
            }

            foreach (object attribute in fieldInfo.GetCustomAttributes(true))
            {
                FieldAttribute fieldAttribute = attribute as FieldAttribute;
                if (fieldAttribute != null)
                {
                    return fieldAttribute.Length;
                }
            }
            return 0;
        }

        /// <summary>
        /// Checks whether a field has the <see cref="FieldAttribute"/> and returns the flags of the field.
        /// </summary>
        /// <param name="fieldInfo"></param>
        /// <returns></returns>
        public static FieldFlags GetFlags(MemberInfo fieldInfo)
        {
            if (fieldInfo == null)
            {
                throw new ArgumentNullException("fieldInfo");
            }

            foreach (object attribute in fieldInfo.GetCustomAttributes(true))
            {
                FieldAttribute fieldAttribute = attribute as FieldAttribute;
                if (fieldAttribute != null)
                {
                    return fieldAttribute.Flags;
                }
            }
            return FieldFlags.None;
        }

        /// <summary>
        /// Obtains the description of a field.
        /// If the attribute is not present null is returned.
        /// </summary>
        /// <param name="fieldInfo"></param>
        /// <returns>Returns the description if present or null otherwise.</returns>
        public static string GetDescription(MemberInfo fieldInfo)
        {
            if (fieldInfo == null)
            {
                throw new ArgumentNullException("fieldInfo");
            }

            foreach (object attribute in fieldInfo.GetCustomAttributes(false))
            {
                DescriptionAttribute descriptionAttribute = attribute as DescriptionAttribute;
                if (descriptionAttribute != null)
                {
                    return descriptionAttribute.Description;
                }
            }
            return null;
        }

        /// <summary>Gets the description of a specified enum or field.</summary>
        /// <param name="value">The enum or field value.</param>
        /// <returns>Returns the description if present or null otherwise.</returns>
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

                string name = Enum.GetName(value.GetType(), value);
                return GetDescription(type.GetField(name));
            }
            foreach (object attribute in type.GetCustomAttributes(false))
            {
                DescriptionAttribute descriptionAttribute = attribute as DescriptionAttribute;
                if (descriptionAttribute != null)
                {
                    return descriptionAttribute.Description;
                }
            }
            return null;
        }

        /// <summary>
        /// Converts a (primitive) value to the desired type.
        /// </summary>
        /// <param name="value"></param>
        /// <param name="toType"></param>
        /// <param name="cultureInfo">The culture to use during formatting.</param>
        /// <returns></returns>
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
        /// <param name="fieldType"></param>
        /// <param name="value"></param>
        /// <param name="cultureInfo">The culture to use during formatting.</param>
        /// <returns></returns>
        public static object ConvertValue(Type fieldType, object value, CultureInfo cultureInfo)
        {
            if (fieldType == null)
            {
                throw new ArgumentNullException("fieldType");
            }

            if (cultureInfo == null)
            {
                throw new ArgumentNullException("cultureInfo");
            }

            if (value == null)
            {
                return null;
            }

            if (fieldType.Name.StartsWith("Nullable"))
            {
#if NET45 || NET46 || NET47 || NETSTANDARD20
				fieldType = fieldType.GenericTypeArguments[0];
#elif NET20 || NET35 || NET40
                fieldType = fieldType.GetGenericArguments()[0];
#else
#error No code defined for the current framework or NETXX version define missing!
#endif
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
                    case "false":
                    case "off":
                    case "no":
                    case "0":
                        return false;
                }
            }
            if (fieldType.IsPrimitive)
            {
                return ConvertPrimitive(fieldType, value, cultureInfo);
            }

            if (fieldType.IsAssignableFrom(value.GetType()))
            {
                return Convert.ChangeType(value, fieldType);
            }

            if (fieldType.IsEnum)
            {
                return Enum.Parse(fieldType, value.ToString(), true);
            }
            //convert to string
            string str;
            {
                if (value is string)
                {
                    str = (string)value;
                }
                else
                {
                    //try to find public ToString(IFormatProvider) method in class
                    MethodInfo l_Method = value.GetType().GetMethod("ToString", BindingFlags.Public | BindingFlags.Instance, null, new Type[] { typeof(IFormatProvider) }, null);
                    if (l_Method != null)
                    {
                        try { str = (string)l_Method.Invoke(value, new object[] { cultureInfo }); }
                        catch (TargetInvocationException ex) { throw ex.InnerException; }
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
                if (long.TryParse(str, out long ticks))
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
            //parse from string
            {
                //try to find public static Parse(string, IFormatProvider) method in class
                List<Exception> errors = new List<Exception>();
                MethodInfo method = fieldType.GetMethod("Parse", BindingFlags.Public | BindingFlags.Static, null, new Type[] { typeof(string), typeof(IFormatProvider) }, null);
                if (method != null)
                {
                    try { return method.Invoke(null, new object[] { str, cultureInfo }); }
                    catch (TargetInvocationException ex) { errors.Add(ex.InnerException); }
                }
                method = fieldType.GetMethod("Parse", BindingFlags.Public | BindingFlags.Static, null, new Type[] { typeof(string) }, null);
                if (method != null)
                {
                    try { return method.Invoke(null, new object[] { str }); }
                    catch (TargetInvocationException ex) { errors.Add(ex.InnerException); }
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

            for (int i = 0; i < values.Count; i++)
            {
                FieldInfo fieldInfo = fields[i];
                object value = ConvertValue(fieldInfo.FieldType, values[i], cultureInfo);
                fields[i].SetValue(obj, value);
            }
        }

        /// <summary>
        /// Obtains an array containing all values of the specified fields.
        /// </summary>
        /// <param name="fields"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public static object[] GetValues(IList<FieldInfo> fields, object value)
        {
            if (fields == null)
            {
                throw new ArgumentNullException("fields");
            }

            object[] result = new object[fields.Count];
            for (int i = 0; i < fields.Count; i++)
            {
                result[i] = fields[i].GetValue(value);
            }
            return result;
        }
    }
}
