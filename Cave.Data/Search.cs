using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Cave.Collections.Generic;
using Cave.Data.Sql;

namespace Cave.Data
{
    /// <summary>
    /// Provides database independent search functions.
    /// </summary>
    public sealed class Search
    {
        #region static class

        /// <summary>Inverts the selection of the specified 'items' using the sorting present at specified 'all'.</summary>
        /// <param name="all">All ids (sorting will be kept).</param>
        /// <param name="items">The items to be inverted.</param>
        /// <returns></returns>
        static IItemSet<long> Invert(IEnumerable<long> all, IEnumerable<long> items)
        {
            var result = new Set<long>();
            IItemSet<long> test = items.AsSet();
            foreach (var id in all)
            {
                if (items.Contains(id))
                {
                    continue;
                }

                result.Add(id);
            }
            return result;
        }

        /// <summary>Prepares a search using like to find a full match of all of the specified parts.</summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="parts">The parts.</param>
        /// <returns></returns>
        public static Search FieldContainsAllOf(string fieldName, string[] parts)
        {
            Search result = None;
            foreach (var part in parts)
            {
                result &= FieldLike(fieldName, TextLike("%" + part + "%"));
            }
            return result;
        }

        /// <summary>Prepares a search using like to find a single match of one of the specified parts.</summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="parts">The parts.</param>
        /// <returns></returns>
        public static Search FieldContainsOneOf(string fieldName, string[] parts)
        {
            Search result = None;
            foreach (var part in parts)
            {
                result |= FieldLike(fieldName, TextLike("%" + part + "%"));
            }
            return result;
        }

        /// <summary>Builds a search text from the specified text.</summary>
        /// <param name="text">The text.</param>
        /// <returns></returns>
        /// <remarks>Space, Point, Star, Percent, Underscore and Questionmark are used as wildcard.</remarks>
        public static string TextLike(string text)
        {
            var result = "%" + text.Trim().ReplaceChars(" .*%_?", "%") + "%";
            while (result.Contains("%%"))
            {
                result = result.Replace("%%", "%");
            }

            return result;
        }

        /// <summary>Creates a search for matching specified fields of a row.</summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="table">The table.</param>
        /// <param name="row">The row.</param>
        /// <param name="fields">The fields.</param>
        /// <returns></returns>
        public static Search FullMatch<T>(ITable<T> table, T row, params string[] fields)
            where T : struct
        {
            return FullMatch(table, Row.Create(table.Layout, row), fields);
        }

        /// <summary>Creates a search for matching specified fields of a row.</summary>
        /// <param name="table">The table.</param>
        /// <param name="row">The row.</param>
        /// <param name="fields">The fields.</param>
        /// <returns></returns>
        public static Search FullMatch(ITable table, Row row, params string[] fields)
        {
            Search search = Search.None;
            foreach (var field in fields)
            {
                var index = table.Layout.GetFieldIndex(field);
                var value = row.GetValue(index);
                search &= FieldEquals(field, value);
            }
            return search;
        }

        /// <summary>Creates a search for matching a given row excluding the ID field.</summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="table">The table.</param>
        /// <param name="row">The row data to search for.</param>
        /// <param name="checkDefaultValues">if set to <c>true</c> [check default values].</param>
        /// <returns>Returns a new search instance.</returns>
        public static Search FullMatch<T>(ITable table, T row, bool checkDefaultValues = false)
            where T : struct
        {
            return FullMatch(table, Row.Create(table.Layout, row), checkDefaultValues);
        }

        /// <summary>Creates a search for matching a given row excluding the ID field.</summary>
        /// <param name="table">The table.</param>
        /// <param name="row">The row data to search for.</param>
        /// <param name="checkDefaultValues">if set to <c>true</c> [check default values].</param>
        /// <returns>Returns a new search instance.</returns>
        public static Search FullMatch(ITable table, Row row, bool checkDefaultValues = false)
        {
            return FullMatch(table, row.GetValues(), checkDefaultValues);
        }

        /// <summary>Creates a search for matching a given row excluding the ID field.</summary>
        /// <param name="table">The table.</param>
        /// <param name="fields">The row data to search for.</param>
        /// <param name="checkDefaultValues">if set to <c>true</c> [check default values].</param>
        /// <returns>Returns a new search instance.</returns>
        public static Search FullMatch(ITable table, object[] fields, bool checkDefaultValues = false)
        {
            Search search = None;
            for (var i = 0; i < table.Layout.FieldCount; i++)
            {
                if (table.Layout.IDFieldIndex == i)
                {
                    continue;
                }

                var value = fields.GetValue(i);
                FieldProperties field = table.Layout.GetProperties(i);
                if (checkDefaultValues)
                {
                    if (value == null)
                    {
                        continue;
                    }

                    switch (field.DataType)
                    {
                        case DataType.String:
                            if (Equals(string.Empty, value))
                            {
                                continue;
                            }
                            break;
                        default:
                        {
                            if (field.ValueType != null)
                            {
                                var defaultValue = Activator.CreateInstance(field.ValueType);
                                if (Equals(value, defaultValue))
                                {
                                    continue;
                                }
                            }
                            break;
                        }
                    }
                }
                search &= FieldEquals(table.Layout.GetName(i), fields.GetValue(i));
            }
            return search;
        }

        /// <summary>
        /// Inverts a search.
        /// </summary>
        /// <param name="search"></param>
        /// <returns></returns>
        public static Search operator !(Search search)
        {
            if (search == null)
            {
                throw new ArgumentNullException(nameof(search));
            }

            switch (search.Mode)
            {
                case SearchMode.And:
                case SearchMode.Or:
                    return new Search(search.Mode, !search.invert, search.searchA, search.searchB);
                case SearchMode.Equals:
                case SearchMode.Like:
                case SearchMode.Greater:
                case SearchMode.Smaller:
                case SearchMode.GreaterOrEqual:
                case SearchMode.SmallerOrEqual:
                case SearchMode.In:
                    return new Search(search.Mode, !search.invert, search.fieldName, search.fieldValue);
                default:
                    throw new ArgumentException(string.Format("Invalid mode {0}!", search.Mode));
            }
        }

        /// <summary>
        /// Combines to searches with AND.
        /// </summary>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <returns></returns>
        public static Search operator &(Search left, Search right)
        {
            if (left == null)
            {
                throw new ArgumentNullException("A");
            }

            if (right == null)
            {
                throw new ArgumentNullException("B");
            }

            if (right.Mode == SearchMode.None)
            {
                return left;
            }

            return left.Mode == SearchMode.None ? right : new Search(SearchMode.And, false, left, right);
        }

        /// <summary>
        /// Combines to searches with OR.
        /// </summary>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <returns></returns>
        public static Search operator |(Search left, Search right)
        {
            if (left == null)
            {
                throw new ArgumentNullException(nameof(left));
            }

            if (right == null)
            {
                throw new ArgumentNullException(nameof(right));
            }

            if (right.Mode == SearchMode.None)
            {
                return left;
            }

            return left.Mode == SearchMode.None ? right : new Search(SearchMode.Or, false, left, right);
        }

        /// <summary>
        /// Creates a field in value search.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <param name="values">The values.</param>
        /// <returns></returns>
        public static Search FieldIn(string name, params object[] values)
        {
            var s = new Set<object>();
            foreach (var val in values)
            {
                s.Include(val);
            }

            return s.Count == 0 ? None : new Search(SearchMode.In, false, name, s);
        }

        /// <summary>
        /// Creates a field in value search.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <param name="values">The values.</param>
        /// <returns></returns>
        public static Search FieldIn(string name, IEnumerable values)
        {
            var s = new Set<object>();
            foreach (var val in values)
            {
                s.Include(val);
            }

            return s.Count == 0 ? None : new Search(SearchMode.In, false, name, s);
        }

        /// <summary>
        /// Creates a field equals value search.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public static Search FieldEquals(string name, object value)
        {
            return new Search(SearchMode.Equals, false, name, value);
        }

        /// <summary>
        /// Creates a field not equals value search.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public static Search FieldNotEquals(string name, object value)
        {
            return new Search(SearchMode.Equals, true, name, value);
        }

        /// <summary>
        /// Creates a field like value search.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public static Search FieldLike(string name, object value)
        {
            return new Search(SearchMode.Like, false, name, value);
        }

        /// <summary>
        /// Creates a field not like value search.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public static Search FieldNotLike(string name, object value)
        {
            return new Search(SearchMode.Like, true, name, value);
        }

        /// <summary>
        /// Creates a field greater value search.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public static Search FieldGreater(string name, object value)
        {
            return new Search(SearchMode.Greater, false, name, value);
        }

        /// <summary>
        /// Creates a field greater or equal value search.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public static Search FieldGreaterOrEqual(string name, object value)
        {
            return new Search(SearchMode.GreaterOrEqual, false, name, value);
        }

        /// <summary>
        /// Creates a field smaller value search.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public static Search FieldSmaller(string name, object value)
        {
            return new Search(SearchMode.Smaller, false, name, value);
        }

        /// <summary>
        /// Creates a field smaller or equal value search.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public static Search FieldSmallerOrEqual(string name, object value)
        {
            return new Search(SearchMode.SmallerOrEqual, false, name, value);
        }

        /// <summary>
        /// No search.
        /// </summary>
        public static Search None { get; } = new Search();

        #endregion

        /// <summary>Gets the mode.</summary>
        /// <value>The mode.</value>
        public SearchMode Mode { get; private set; }

        /// <summary>
        /// the field number (unknown == -1).
        /// </summary>
        int fieldNumber = -1;

        /// <summary>
        /// Sub search A (only used with Mode = AND / OR).
        /// </summary>
        Search searchA;

        /// <summary>
        /// Sub search B (only used with Mode = AND / OR).
        /// </summary>
        Search searchB;

        /// <summary>
        /// The fieldname to search for.
        /// </summary>
        string fieldName;

        /// <summary>
        /// The value to search for.
        /// </summary>
        object fieldValue;

        /// <summary>
        /// Invert the search.
        /// </summary>
        bool invert;

        /// <summary>
        /// The RegEx for LIKE comparison.
        /// </summary>
        Regex expression;

        /// <summary>
        /// FieldProperties for database value conversion.
        /// </summary>
        FieldProperties fieldProperties;

        /// <summary>The layout.</summary>
        RowLayout layout;

        /// <summary>
        /// Initializes a new instance of the <see cref="Search"/> class.
        /// </summary>
        Search()
        {
            Mode = SearchMode.None;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Search"/> class.
        /// </summary>
        /// <param name="mode">AND / OR.</param>
        /// <param name="not">Invert the search.</param>
        /// <param name="left">First search to combine.</param>
        /// <param name="right">Second search to combine.</param>
        Search(SearchMode mode, bool not, Search left, Search right)
        {
            switch (mode)
            {
                case SearchMode.And:
                case SearchMode.Or:
                    break;
                default:
                    throw new ArgumentException(string.Format("Invalid mode {0}!", this.Mode));
            }
            invert = not;
            this.Mode = mode;
            searchA = left;
            searchB = right;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Search"/> class.
        /// </summary>
        /// <param name="mode">The mode of operation.</param>
        /// <param name="not">Invert the search.</param>
        /// <param name="name">Name of the field.</param>
        /// <param name="value">Value of the field.</param>
        Search(SearchMode mode, bool not, string name, object value)
        {
            switch (mode)
            {
                case SearchMode.In:
                    if (!(value is Set<object>))
                    {
                        throw new ArgumentException("Value needs to be a set!");
                    }

                    break;
                case SearchMode.Equals:
                case SearchMode.Like:
                case SearchMode.Smaller:
                case SearchMode.Greater:
                case SearchMode.GreaterOrEqual:
                case SearchMode.SmallerOrEqual:
                    break;
                default:
                    throw new ArgumentException(string.Format("Invalid mode {0}!", this.Mode));
            }
            invert = not;
            fieldName = name;
            fieldValue = value;
            this.Mode = mode;
        }

        internal void LoadLayout(RowLayout layout)
        {
            if (!ReferenceEquals(this.layout, null))
            {
                if (ReferenceEquals(layout, this.layout))
                {
                    return;
                }

                throw new Exception("Different Layout already set!");
            }
            switch (Mode)
            {
                case SearchMode.And:
                case SearchMode.Or:
                case SearchMode.None:
                    return;
            }
            this.layout = layout;
            if (fieldName == null)
            {
                throw new ArgumentNullException(nameof(fieldName));
            }

            if (fieldNumber < 0)
            {
                fieldNumber = layout.GetFieldIndex(fieldName);
                if (fieldNumber < 0)
                {
                    throw new ArgumentException(string.Format("Field {0} is not present at table {1}!", fieldName, layout.Name));
                }
            }
            if (fieldProperties == null)
            {
                fieldProperties = layout.GetProperties(fieldNumber);
                if (Mode == SearchMode.In)
                {
                    var result = new Set<object>();
                    foreach (var value in (Set<object>)fieldValue)
                    {
                        result.Add(ConvertValue(value));
                    }
                    fieldValue = result;
                }
                else if (Mode == SearchMode.Like)
                {
                    // Do nothing
                }
                else
                {
                    if (fieldValue != null && fieldProperties.ValueType != fieldValue.GetType())
                    {
                        fieldValue = ConvertValue(fieldValue);
                    }
                }
            }
        }

        object ConvertValue(object value)
        {
            try
            {
                if (fieldProperties.ValueType.IsPrimitive && fieldProperties.ValueType.IsValueType)
                {
                    var conv = value as IConvertible;
                    try
                    {
                        if (conv != null)
                        {
                            return conv.ToType(fieldProperties.ValueType, CultureInfo.InvariantCulture);
                        }
                    }
                    catch
                    {
                    }
                }
                return fieldProperties.ParseValue(value.ToString(), null, CultureInfo.InvariantCulture);
            }
            catch (Exception ex)
            {
                throw new InvalidDataException(string.Format("Search {0} cannot convert value {1} to type {2}!", this, value, fieldProperties), ex);
            }
        }

        /// <summary>
        /// Gets a string describing this instance.
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            string operation;
            switch (Mode)
            {
                case SearchMode.None: return "TRUE";
                case SearchMode.And: return (invert ? "NOT " : string.Empty) + "(" + searchA.ToString() + " AND " + searchB.ToString() + ")";
                case SearchMode.Or: return (invert ? "NOT " : string.Empty) + "(" + searchA.ToString() + " OR " + searchB.ToString() + ")";
                case SearchMode.Equals: operation = invert ? "!=" : "=="; break;
                case SearchMode.Like: operation = invert ? "NOT LIKE" : "LIKE"; break;
                case SearchMode.Greater: operation = invert ? "<=" : ">"; break;
                case SearchMode.Smaller: operation = invert ? ">=" : "<"; break;
                case SearchMode.GreaterOrEqual: operation = invert ? "<" : ">="; break;
                case SearchMode.SmallerOrEqual: operation = invert ? ">" : "<="; break;
                case SearchMode.In: operation = (invert ? "NOT " : string.Empty) + "IN (" + ((IEnumerable)fieldValue).Join(",") + ")"; break;
                default: return string.Format("Unknown mode {0}!", Mode);
            }
            return fieldValue == null
                ? StringExtensions.Format("[{0}] {1} <null>", fieldName, operation)
                : StringExtensions.Format("[{0}] {1} '{2}'", fieldName, operation, StringExtensions.ToString(fieldValue));
        }

        /// <summary>
        /// Gets the hashcode for this instance.
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            return ToString().GetHashCode();
        }

        string ToSqlStringAndParameters(RowLayout layout, SqlSearch search)
        {
            if (!search.Storage.SupportsNamedParameters)
            {
                throw new NotSupportedException(string.Format("This function requires named parameters!"));
            }

            LoadLayout(layout);

            switch (Mode)
            {
                case SearchMode.None: return "1=1";

                case SearchMode.In:
                {
                    // escape the fieldname
                    var name = search.Storage.EscapeFieldName(fieldProperties);
                    var result = new StringBuilder();
                    result.Append(name);
                    result.Append(" ");
                    if (invert)
                    {
                        result.Append("NOT ");
                    }

                    result.Append("IN (");
                    var i = 0;
                    foreach (var value in (Set<object>)fieldValue)
                    {
                        if (i++ > 0)
                        {
                            result.Append(",");
                        }

                        var parameterName = (search.Parameters.Count + 1).ToString();
                        search.AddParameter(fieldProperties, parameterName, value);
                        result.Append(search.Storage.ParameterPrefix);
                        result.Append(parameterName);
                    }
                    result.Append(")");
                    return result.ToString();
                }

                case SearchMode.Equals:
                {
                    // escape the fieldname
                    var name = search.Storage.EscapeFieldName(fieldProperties);

                    // is value null -> yes return "name IS [NOT] NULL"
                    if (fieldValue == null)
                    {
                        return name + " IS " + (invert ? "NOT " : string.Empty) + "NULL";
                    }

                    // no add parameter and return querytext
                    var parameterName = (search.Parameters.Count + 1).ToString();
                    search.AddParameter(fieldProperties, parameterName, fieldValue);
                    return name + (invert ? "<>" : "=") + search.Storage.ParameterPrefix + parameterName;
                }

                case SearchMode.Like:
                {
                    // escape the fieldname
                    var name = search.Storage.EscapeFieldName(fieldProperties);

                    // is value null -> yes return "name IS [NOT] NULL"
                    if (fieldValue == null)
                    {
                        return name + " IS " + (invert ? "NOT " : string.Empty) + "NULL";
                    }

                    // no add parameter and return querytext
                    var parameterName = (search.Parameters.Count + 1).ToString();
                    search.AddParameter(fieldProperties, parameterName, fieldValue);
                    return name + (invert ? " NOT" : string.Empty) + " LIKE " + search.Storage.ParameterPrefix + parameterName;
                }

                case SearchMode.Greater:
                {
                    // escape the fieldname
                    var name = search.Storage.EscapeFieldName(fieldProperties);

                    // no add parameter and return querytext
                    var parameterName = (search.Parameters.Count + 1).ToString();
                    search.AddParameter(fieldProperties, parameterName, fieldValue);
                    return invert
                        ? name + " <= " + search.Storage.ParameterPrefix + parameterName
                        : name + " > " + search.Storage.ParameterPrefix + parameterName;
                }

                case SearchMode.GreaterOrEqual:
                {
                    // escape the fieldname
                    var name = search.Storage.EscapeFieldName(fieldProperties);

                    // no add parameter and return querytext
                    var parameterName = (search.Parameters.Count + 1).ToString();
                    search.AddParameter(fieldProperties, parameterName, fieldValue);
                    return invert
                        ? name + " < " + search.Storage.ParameterPrefix + parameterName
                        : name + " >= " + search.Storage.ParameterPrefix + parameterName;
                }

                case SearchMode.Smaller:
                {
                    // escape the fieldname
                    var name = search.Storage.EscapeFieldName(fieldProperties);

                    // no add parameter and return querytext
                    var parameterName = (search.Parameters.Count + 1).ToString();
                    search.AddParameter(fieldProperties, parameterName, fieldValue);
                    return invert
                        ? name + " >= " + search.Storage.ParameterPrefix + parameterName
                        : name + " < " + search.Storage.ParameterPrefix + parameterName;
                }

                case SearchMode.SmallerOrEqual:
                {
                    // escape the fieldname
                    var name = search.Storage.EscapeFieldName(fieldProperties);

                    // no add parameter and return querytext
                    var parameterName = (search.Parameters.Count + 1).ToString();
                    search.AddParameter(fieldProperties, parameterName, fieldValue);
                    return invert
                        ? name + " > " + search.Storage.ParameterPrefix + parameterName
                        : name + " <= " + search.Storage.ParameterPrefix + parameterName;
                }

                case SearchMode.And: return (invert ? "NOT " : string.Empty) + "(" + searchA.ToSqlStringAndParameters(layout, search) + " AND " + searchB.ToSqlStringAndParameters(layout, search) + ")";
                case SearchMode.Or: return (invert ? "NOT " : string.Empty) + "(" + searchA.ToSqlStringAndParameters(layout, search) + " OR " + searchB.ToSqlStringAndParameters(layout, search) + ")";
                default: throw new NotImplementedException(string.Format("Mode {0} not implemented!", Mode));
            }
        }

        // may be called on different sql database implementations
        string ToSqlString(RowLayout layout, SqlSearch search)
        {
            LoadLayout(layout);
            switch (Mode)
            {
                case SearchMode.None: return "1=1";

                case SearchMode.In:
                {
                    // escape the fieldname
                    var name = search.Storage.EscapeFieldName(fieldProperties);
                    var result = new StringBuilder();
                    if (invert)
                    {
                        result.Append("NOT ");
                    }

                    result.Append("IN (");
                    var i = 0;
                    foreach (var value in (Set<object>)fieldValue)
                    {
                        if (i++ > 0)
                        {
                            result.Append(",");
                        }

                        // prepare value string
                        var str1 = value.ToString();
                        var str2 = StringExtensions.GetValidChars(str1, ASCII.Strings.SafeName);
#if DEBUG
                        if (str1 != str2)
                        {
                            throw new ArgumentException("Value at search does not match safe name requirements!");
                        }
#endif
                        result.Append("'");
                        result.Append(str2);
                        result.Append("'");
                    }
                    return result.ToString();
                }

                case SearchMode.Equals:
                {
                    // escape the fieldname
                    var name = search.Storage.EscapeFieldName(fieldProperties);

                    // is value null -> yes return "name IS [NOT] NULL"
                    if (fieldValue == null)
                    {
                        return name + " IS " + (invert ? "NOT " : string.Empty) + "NULL";
                    }

                    // prepare value string
                    var str1 = fieldValue.ToString();
                    var str2 = StringExtensions.GetValidChars(str1, ASCII.Strings.SafeName);
#if DEBUG
                    if (str1 != str2)
                    {
                        throw new ArgumentException("Value at search does not match safe name requirements!");
                    }
#endif
                    return name + (invert ? "<>" : "=") + "'" + str2 + "'";
                }

                case SearchMode.Like:
                {
                    // escape the fieldname
                    var name = search.Storage.EscapeFieldName(fieldProperties);

                    // is value null -> yes return "name IS [NOT] NULL"
                    if (fieldValue == null)
                    {
                        return name + " IS " + (invert ? "NOT " : string.Empty) + "NULL";
                    }

                    // prepare value string
                    var value = fieldValue.ToString();
                    value = StringExtensions.GetValidChars(value, ASCII.Strings.SafeName);
#if DEBUG
                    if (value != fieldValue.ToString())
                    {
                        throw new ArgumentException("Value at search does not match safe name requirements!");
                    }
#endif
                    return name + (invert ? " NOT" : string.Empty) + " LIKE '" + value + "'";
                }

                case SearchMode.Greater:
                {
                    // escape the fieldname
                    var name = search.Storage.EscapeFieldName(fieldProperties);

                    // prepare value string
                    var value = fieldValue.ToString();
                    value = StringExtensions.GetValidChars(value, ASCII.Strings.SafeName);
#if DEBUG
                    if (value != fieldValue.ToString())
                    {
                        throw new ArgumentException("Value at search does not match safe name requirements!");
                    }
#endif
                    return invert ? name + " <= " + value : name + " > " + value;
                }

                case SearchMode.GreaterOrEqual:
                {
                    // escape the fieldname
                    var name = search.Storage.EscapeFieldName(fieldProperties);

                    // prepare value string
                    var value = fieldValue.ToString();
                    value = StringExtensions.GetValidChars(value, ASCII.Strings.SafeName);
#if DEBUG
                    if (value != fieldValue.ToString())
                    {
                        throw new ArgumentException("Value at search does not match safe name requirements!");
                    }
#endif
                    return invert ? name + " < " + value : name + " >= " + value;
                }

                case SearchMode.Smaller:
                {
                    // escape the fieldname
                    var name = search.Storage.EscapeFieldName(fieldProperties);

                    // prepare value string
                    var value = fieldValue.ToString();
                    value = StringExtensions.GetValidChars(value, ASCII.Strings.SafeName);
#if DEBUG
                    if (value != fieldValue.ToString())
                    {
                        throw new ArgumentException("Value at search does not match safe name requirements!");
                    }
#endif
                    return invert ? name + " >= " + value : name + " < " + value;
                }

                case SearchMode.SmallerOrEqual:
                {
                    // escape the fieldname
                    var name = search.Storage.EscapeFieldName(fieldProperties);

                    // prepare value string
                    var value = fieldValue.ToString();
                    value = StringExtensions.GetValidChars(value, ASCII.Strings.SafeName);
#if DEBUG
                    if (value != fieldValue.ToString())
                    {
                        throw new ArgumentException("Value at search does not match safe name requirements!");
                    }
#endif
                    return invert ? name + " > " + value : name + " <= " + value;
                }

                case SearchMode.And: return (invert ? "NOT " : string.Empty) + "(" + searchA.ToSqlStringAndParameters(layout, search) + " AND " + searchB.ToSqlStringAndParameters(layout, search) + ")";
                case SearchMode.Or: return (invert ? "NOT " : string.Empty) + "(" + searchA.ToSqlStringAndParameters(layout, search) + " OR " + searchB.ToSqlStringAndParameters(layout, search) + ")";
                default: throw new NotImplementedException(string.Format("Mode {0} not implemented!", Mode));
            }
        }

        internal SqlSearch ToSql(RowLayout layout, SqlStorage storage)
        {
            var result = new SqlSearch(storage);
            result.SetText(ToSqlStringAndParameters(layout, result));
            return result;
        }

        // may be called on different sql database implementations
        internal SqlSearch ToSqlNoParameters(RowLayout layout, SqlStorage storage)
        {
            var result = new SqlSearch(storage);
            result.SetText(ToSqlString(layout, result));
            return result;
        }

        bool Like(object value)
        {
            if (fieldValue == null)
            {
                return value == null;
            }

            if (value == null)
            {
                return false;
            }

            var text = value.ToString();
            if (expression == null)
            {
                var valueString = fieldValue.ToString();
                var lastWasWildcard = false;

                var sb = new StringBuilder();
                sb.Append('^');
                foreach (var c in valueString)
                {
                    switch (c)
                    {
                        case '%':
                            if (lastWasWildcard)
                            {
                                continue;
                            }

                            lastWasWildcard = true;
                            sb.Append(".*");
                            continue;
                        case '_':
                            sb.Append(".");
                            continue;
                        case ' ':
                        case '\\':
                        case '*':
                        case '+':
                        case '?':
                        case '|':
                        case '{':
                        case '[':
                        case '(':
                        case ')':
                        case '^':
                        case '$':
                        case '.':
                        case '#':
                            sb.Append('\\');
                            break;
                    }
                    sb.Append(c);
                    lastWasWildcard = false;
                }
                sb.Append('$');
                var s = sb.ToString();
                Trace.TraceInformation("Create regex {0} for search {1}", s, this);
                expression = new Regex(s, RegexOptions.IgnoreCase);
            }
            return expression.IsMatch(text);
        }

        internal bool Check(Row row)
        {
            var result = true;

            switch (Mode)
            {
                case SearchMode.None:
                    if (invert)
                    {
                        result = !result;
                    }

                    return result;

                case SearchMode.And:
                    result = searchA.Check(row) && searchB.Check(row);
                    if (invert)
                    {
                        result = !result;
                    }

                    return result;

                case SearchMode.Or:
                    result = searchA.Check(row) || searchB.Check(row);
                    if (invert)
                    {
                        result = !result;
                    }

                    return result;
            }

            switch (Mode)
            {
                case SearchMode.Greater:
                {
                    var tableValue = (IComparable)row.GetValue(fieldNumber);
                    if (fieldValue is DateTime)
                    {
                        result = Compare((DateTime)tableValue, (DateTime)fieldValue) > 0;
                    }
                    else
                    {
                        result = tableValue.CompareTo(fieldValue) > 0;
                    }
                    break;
                }

                case SearchMode.GreaterOrEqual:
                {
                    var tableValue = (IComparable)row.GetValue(fieldNumber);
                    if (fieldValue is DateTime)
                    {
                        result = Compare((DateTime)tableValue, (DateTime)fieldValue) >= 0;
                    }
                    else
                    {
                        result = tableValue.CompareTo(fieldValue) >= 0;
                    }
                    break;
                }

                case SearchMode.Smaller:
                {
                    var tableValue = (IComparable)row.GetValue(fieldNumber);
                    if (fieldValue is DateTime)
                    {
                        result = Compare((DateTime)tableValue, (DateTime)fieldValue) < 0;
                    }
                    else
                    {
                        result = tableValue.CompareTo(fieldValue) < 0;
                    }
                    break;
                }

                case SearchMode.SmallerOrEqual:
                {
                    var tableValue = (IComparable)row.GetValue(fieldNumber);
                    if (fieldValue is DateTime)
                    {
                        result = Compare((DateTime)tableValue, (DateTime)fieldValue) <= 0;
                    }
                    else
                    {
                        result = tableValue.CompareTo(fieldValue) <= 0;
                    }
                    break;
                }

                case SearchMode.In:
                {
                    var rowValue = row.GetValue(fieldNumber);
                    result = ((Set<object>)fieldValue).Contains(rowValue);
                }
                break;

                case SearchMode.Equals:
                {
                    if (fieldValue is DateTime)
                    {
                        result = Compare((DateTime)row.GetValue(fieldNumber), (DateTime)fieldValue) == 0;
                    }
                    else
                    {
                        result = Equals(row.GetValue(fieldNumber), fieldValue);
                    }
                    break;
                }

                case SearchMode.Like:
                {
                    if (fieldProperties == null)
                    {
                        fieldProperties = layout.GetProperties(fieldNumber);
                        if (fieldValue != null && fieldProperties.ValueType != fieldValue.GetType())
                        {
                            throw new Exception(string.Format("Search for field {0}: Value has to be of type {1}", fieldProperties, fieldProperties.ValueType));
                        }
                    }
                    result = Like(row.GetValue(fieldNumber));
                    break;
                }

                default: throw new NotImplementedException(string.Format("Mode {0} not implemented!", Mode));
            }

            if (invert)
            {
                result = !result;
            }

            return result;
        }

        private int Compare(DateTime tableValue, DateTime checkValue)
        {
            if (checkValue.Kind == DateTimeKind.Local)
            {
                checkValue = checkValue.ToUniversalTime();
            }

            if (tableValue.Kind == DateTimeKind.Local)
            {
                tableValue = tableValue.ToUniversalTime();
            }

            return tableValue.Ticks.CompareTo(checkValue.Ticks);
        }

        /// <summary>Scans a Table for matches with the current search.</summary>
        /// <param name="preselected">The preselected ids.</param>
        /// <param name="layout">Layout of the table.</param>
        /// <param name="indices">FieldIndices or null.</param>
        /// <param name="table">The table to scan.</param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException">Layout
        /// or
        /// Table.</exception>
        /// <exception cref="NotImplementedException"></exception>
        public IItemSet<long> Scan(IItemSet<long> preselected, RowLayout layout, IFieldIndex[] indices, ITable table)
        {
            if (layout == null)
            {
                throw new ArgumentNullException("Layout");
            }

            if (table == null)
            {
                throw new ArgumentNullException("Table");
            }

            LoadLayout(layout);

            switch (Mode)
            {
                case SearchMode.None:
                {
                    return invert ? new Set<long>() : preselected == null ? table.IDs.AsSet() : preselected;
                }

                case SearchMode.And:
                {
                    IItemSet<long> resultA = searchA.Scan(preselected, layout, indices, table);
                    IItemSet<long> result = searchB.Scan(resultA, layout, indices, table);
                    if (invert)
                    {
                        result = Invert(preselected == null ? table.IDs.AsSet() : preselected, result);
                    }

                    return result;
                }

                case SearchMode.Or:
                {
                    var result = new Set<long>(searchA.Scan(preselected, layout, indices, table));
                    var resultB = new Set<long>(searchB.Scan(preselected, layout, indices, table));
                    result.IncludeRange(resultB);
                    return invert ? Invert(preselected == null ? table.IDs.AsSet() : preselected, result) : result;
                }

                case SearchMode.In:
                {
                    IItemSet<long> result = new Set<long>();
                    if (fieldNumber == layout.IDFieldIndex)
                    {
                        // get all ids with given ids
                        IEnumerable<long> idsToLookFor = ((Set<object>)fieldValue).Select(o => (long)o);
                        result.AddRange(idsToLookFor.Where(id => table.Exist(id)));
                        if (preselected != null)
                        {
                            result = Set<long>.BitwiseAnd(result, preselected);
                        }
                    }

                    // check if we can do an index search
                    else if ((indices != null) && (indices[fieldNumber] != null))
                    {
                        // field has an index, get all ids with field value
                        foreach (var value in (Set<object>)fieldValue)
                        {
                            result.AddRange(indices[fieldNumber].Find(value));
                        }
                        if (preselected != null)
                        {
                            result = Set<long>.BitwiseAnd(result, preselected);
                        }
                    }

                    // full scan required...
                    else
                    {
                        Search search = Search.None;
                        foreach (var value in (Set<object>)fieldValue)
                        {
                            search |= Search.FieldEquals(fieldName, value);
                        }
                        result = search.Scan(preselected, layout, indices, table);
                    }
                    return invert ? Invert(preselected == null ? table.IDs.AsSet() : preselected, result) : result;
                }

                case SearchMode.Smaller:
                case SearchMode.Greater:
                case SearchMode.SmallerOrEqual:
                case SearchMode.GreaterOrEqual:
                case SearchMode.Like:
                case SearchMode.Equals:
                {
                    if (Mode == SearchMode.Equals)
                    {
                        // id field select?
                        if (fieldNumber == layout.IDFieldIndex)
                        {
                            var result = new Set<long>();
                            var id = Convert.ToInt64(fieldValue);
                            if (table.Exist(id) && ((preselected == null) || preselected.Contains(id)))
                            {
                                result.Add(id);
                            }
                            return invert ? Invert(preselected == null ? table.IDs.AsSet() : preselected, result) : result;
                        }

                        // check if we can do an index search
                        if ((indices != null) && (indices[fieldNumber] != null))
                        {
                            // field has an index
                            var result = new Set<long>(indices[fieldNumber].Find(fieldValue));
                            if (preselected != null)
                            {
                                result = Set<long>.BitwiseAnd(result, preselected);
                            }

                            return invert ? Invert(preselected == null ? table.IDs.AsSet() : preselected, result) : result;
                        }
#if DEBUG
                        // field has no index, need table scan
                        if (preselected == null && table.RowCount > 1000)
                        {
                            Debug.WriteLine(string.Format("<yellow>Warning:<default> Doing slow memory search on Table <red>{0}<default> Field <red>{1}<default>! You should consider adding an index!", layout.Name, fieldName));
                        }
#endif
                    }

                    // scan without index
                    {
                        var result = new Set<long>();
                        if (preselected != null)
                        {
                            foreach (var id in preselected)
                            {
                                if (Check(table.GetRow(id)))
                                {
                                    result.Add(id);
                                }
                            }
                        }
                        else
                        {
                            foreach (Row row in table.GetRows())
                            {
                                if (Check(row))
                                {
                                    result.Add(row.GetID(layout.IDFieldIndex));
                                }
                            }
                        }
                        return result;
                    }
                }

                default: throw new NotImplementedException(string.Format("Mode {0} not implemented!", Mode));
            }
        }
    }
}
