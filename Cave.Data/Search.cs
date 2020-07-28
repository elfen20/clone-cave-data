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

namespace Cave.Data
{
    /// <summary>Provides database independent search functions.</summary>
    public sealed class Search
    {
        /// <summary>Initializes a new instance of the <see cref="Search" /> class.</summary>
        Search() => Mode = SearchMode.None;

        /// <summary>Initializes a new instance of the <see cref="Search" /> class.</summary>
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
                    throw new ArgumentException($"Invalid mode {Mode}!");
            }

            Inverted = not;
            Mode = mode;
            SearchA = left;
            SearchB = right;
        }

        /// <summary>Initializes a new instance of the <see cref="Search" /> class.</summary>
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
                    throw new ArgumentException($"Invalid mode {Mode}!");
            }

            Inverted = not;
            FieldName = name;
            FieldValue = value;
            Mode = mode;
        }

        /// <summary>Gets no search.</summary>
        public static Search None { get; } = new Search();

        /// <summary>Gets the mode.</summary>
        /// <value>The mode.</value>
        public SearchMode Mode { get; }

        /// <summary>Gets the field number (unknown == -1).</summary>
        public int FieldNumber { get; private set; } = -1;

        /// <summary>Gets sub search A (only used with Mode = AND / OR).</summary>
        public Search SearchA { get; }

        /// <summary>Gets sub search B (only used with Mode = AND / OR).</summary>
        public Search SearchB { get; }

        /// <summary>Gets the fieldname to search for.</summary>
        public string FieldName { get; private set; }

        /// <summary>Gets the value to search for.</summary>
        public object FieldValue { get; private set; }

        /// <summary>Gets a value indicating whether invert the search.</summary>
        public bool Inverted { get; }

        /// <summary>Gets the RegEx for LIKE comparison.</summary>
        public Regex Expression { get; private set; }

        /// <summary>Gets fieldProperties for database value conversion.</summary>
        public IFieldProperties FieldProperties { get; private set; }

        /// <summary>Gets the layout.</summary>
        public RowLayout Layout { get; private set; }

        /// <summary>Inverts a search.</summary>
        /// <param name="search">Search to invert.</param>
        /// <returns>A new search instance.</returns>
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
                    return new Search(search.Mode, !search.Inverted, search.SearchA, search.SearchB);
                case SearchMode.Equals:
                case SearchMode.Like:
                case SearchMode.Greater:
                case SearchMode.Smaller:
                case SearchMode.GreaterOrEqual:
                case SearchMode.SmallerOrEqual:
                case SearchMode.In:
                    return new Search(search.Mode, !search.Inverted, search.FieldName, search.FieldValue);
                default:
                    throw new ArgumentException($"Invalid mode {search.Mode}!");
            }
        }

        /// <summary>Combines to searches with AND.</summary>
        /// <param name="left">The first search.</param>
        /// <param name="right">The second search.</param>
        /// <returns>A new search instance.</returns>
        public static Search operator &(Search left, Search right)
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

            return left.Mode == SearchMode.None ? right : new Search(SearchMode.And, false, left, right);
        }

        /// <summary>Combines to searches with OR.</summary>
        /// <param name="left">The first search.</param>
        /// <param name="right">The second search.</param>
        /// <returns>A new search instance.</returns>
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

        /// <summary>Prepares a search using like to find a full match of all of the specified parts.</summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="parts">The parts.</param>
        /// <returns>Rows containing all specified parts.</returns>
        public static Search FieldContainsAllOf(string fieldName, string[] parts)
        {
            var result = None;
            foreach (var part in parts)
            {
                result &= FieldLike(fieldName, TextLike("%" + part + "%"));
            }

            return result;
        }

        /// <summary>Prepares a search using like to find a single match of one of the specified parts.</summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="parts">The parts.</param>
        /// <returns>A new search instance.</returns>
        public static Search FieldContainsOneOf(string fieldName, string[] parts)
        {
            var result = None;
            foreach (var part in parts)
            {
                result |= FieldLike(fieldName, TextLike("%" + part + "%"));
            }

            return result;
        }

        /// <summary>Builds a search text from the specified text.</summary>
        /// <param name="text">The text.</param>
        /// <returns>The search text.</returns>
        /// <remarks>Space, Point, Star, Percent, Underscore and Questionmark are used as wildcard.</remarks>
        public static string TextLike(string text)
        {
            var result = text.ReplaceChars(" .*%_?", "%");
            while (result.Contains("%%"))
            {
                result = result.Replace("%%", "%");
            }

            return result;
        }

        /// <summary>Creates a search for matching specified fields of a row.</summary>
        /// <param name="table">The table.</param>
        /// <param name="row">The row.</param>
        /// <param name="fieldNames">The field names.</param>
        /// <returns>A new search instance.</returns>
        public static Search FullMatch(ITable table, Row row, params string[] fieldNames)
        {
            var search = None;
            foreach (var field in fieldNames)
            {
                var index = table.Layout.GetFieldIndex(field, true);
                var value = row[index];
                search &= FieldEquals(field, value);
            }

            return search;
        }

        /// <summary>Creates a search for matching specified fields of a row.</summary>
        /// <param name="table">The table.</param>
        /// <param name="row">The row.</param>
        /// <returns>A new search instance.</returns>
        public static Search IdentifierMatch(ITable table, Row row)
        {
            var search = None;
            foreach (var field in table.Layout.Identifier)
            {
                var value = row[field.Index];
                search &= FieldEquals(field.Name, value);
            }

            return search;
        }

        /// <summary>Creates a search for matching a given row excluding the ID field.</summary>
        /// <typeparam name="TStruct">Structure type.</typeparam>
        /// <param name="table">The table.</param>
        /// <param name="row">The row data to search for.</param>
        /// <param name="checkDefaultValues">if set to <c>true</c> [check default values].</param>
        /// <returns>Returns a new search instance.</returns>
        public static Search FullMatch<TStruct>(ITable table, TStruct row, bool checkDefaultValues = false)
            where TStruct : struct =>
            FullMatch(table, table.Layout.GetRow(row), checkDefaultValues);

        /// <summary>Creates a search for matching a given row excluding the ID field.</summary>
        /// <param name="table">The table.</param>
        /// <param name="row">The row data to search for.</param>
        /// <param name="checkDefaultValues">if set to <c>true</c> [check default values].</param>
        /// <returns>Returns a new search instance.</returns>
        public static Search FullMatch(ITable table, Row row, bool checkDefaultValues = false) => FullMatch(table, row, checkDefaultValues);

        /// <summary>Creates a search for matching a given row excluding the identifier fields.</summary>
        /// <param name="table">The table.</param>
        /// <param name="fields">The row data to search for.</param>
        /// <param name="checkDefaultValues">if set to <c>true</c> [check default values].</param>
        /// <returns>Returns a new search instance.</returns>
        public static Search FullMatch(ITable table, object[] fields, bool checkDefaultValues = false)
        {
            var search = None;
            for (var i = 0; i < table.Layout.FieldCount; i++)
            {
                var field = table.Layout[i];
                if (field.Flags.HasFlag(FieldFlags.ID))
                {
                    continue;
                }

                var value = fields.GetValue(i);
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

        /// <summary>Creates a field in value search.</summary>
        /// <param name="name">The name.</param>
        /// <param name="values">The values.</param>
        /// <returns>A new search instance.</returns>
        public static Search FieldIn(string name, params object[] values)
        {
            var s = new Set<object>();
            foreach (var val in values)
            {
                s.Include(val);
            }

            return s.Count == 0 ? None : new Search(SearchMode.In, false, name, s);
        }

        /// <summary>Creates a field in value search.</summary>
        /// <param name="name">The name.</param>
        /// <param name="values">The values.</param>
        /// <returns>A new search instance.</returns>
        public static Search FieldIn(string name, IEnumerable values)
        {
            var s = new Set<object>();
            foreach (var val in values)
            {
                s.Include(val);
            }

            return s.Count == 0 ? None : new Search(SearchMode.In, false, name, s);
        }

        /// <summary>Creates a field equals value search.</summary>
        /// <param name="name">The field name.</param>
        /// <param name="value">The value to check against.</param>
        /// <returns>A new search instance.</returns>
        public static Search FieldEquals(string name, object value) => new Search(SearchMode.Equals, false, name, value);

        /// <summary>Creates a field not equals value search.</summary>
        /// <param name="name">The field name.</param>
        /// <param name="value">The value to check against.</param>
        /// <returns>A new search instance.</returns>
        public static Search FieldNotEquals(string name, object value) => new Search(SearchMode.Equals, true, name, value);

        /// <summary>Creates a field like value search.</summary>
        /// <param name="name">The field name.</param>
        /// <param name="value">The value to check against.</param>
        /// <returns>A new search instance.</returns>
        public static Search FieldLike(string name, object value) => new Search(SearchMode.Like, false, name, value);

        /// <summary>Creates a field not like value search.</summary>
        /// <param name="name">The field name.</param>
        /// <param name="value">The value to check against.</param>
        /// <returns>A new search instance.</returns>
        public static Search FieldNotLike(string name, object value) => new Search(SearchMode.Like, true, name, value);

        /// <summary>Creates a field greater value search.</summary>
        /// <param name="name">The field name.</param>
        /// <param name="value">The value to check against.</param>
        /// <returns>A new search instance.</returns>
        public static Search FieldGreater(string name, object value) => new Search(SearchMode.Greater, false, name, value);

        /// <summary>Creates a field greater or equal value search.</summary>
        /// <param name="name">The field name.</param>
        /// <param name="value">The value to check against.</param>
        /// <returns>A new search instance.</returns>
        public static Search FieldGreaterOrEqual(string name, object value) => new Search(SearchMode.GreaterOrEqual, false, name, value);

        /// <summary>Creates a field smaller value search.</summary>
        /// <param name="name">The field name.</param>
        /// <param name="value">The value to check against.</param>
        /// <returns>A new search instance.</returns>
        public static Search FieldSmaller(string name, object value) => new Search(SearchMode.Smaller, false, name, value);

        /// <summary>Creates a field smaller or equal value search.</summary>
        /// <param name="name">The field name.</param>
        /// <param name="value">The value to check against.</param>
        /// <returns>A new search instance.</returns>
        public static Search FieldSmallerOrEqual(string name, object value) => new Search(SearchMode.SmallerOrEqual, false, name, value);

        /// <inheritdoc />
        public override string ToString()
        {
            string operation;
            switch (Mode)
            {
                case SearchMode.None: return "TRUE";
                case SearchMode.And: return (Inverted ? "NOT " : string.Empty) + "(" + SearchA + " AND " + SearchB + ")";
                case SearchMode.Or: return (Inverted ? "NOT " : string.Empty) + "(" + SearchA + " OR " + SearchB + ")";
                case SearchMode.Equals:
                    operation = Inverted ? "!=" : "==";
                    break;
                case SearchMode.Like:
                    operation = Inverted ? "NOT LIKE" : "LIKE";
                    break;
                case SearchMode.Greater:
                    operation = Inverted ? "<=" : ">";
                    break;
                case SearchMode.Smaller:
                    operation = Inverted ? ">=" : "<";
                    break;
                case SearchMode.GreaterOrEqual:
                    operation = Inverted ? "<" : ">=";
                    break;
                case SearchMode.SmallerOrEqual:
                    operation = Inverted ? ">" : "<=";
                    break;
                case SearchMode.In:
                    operation = (Inverted ? "NOT " : string.Empty) + "IN (" + ((IEnumerable) FieldValue).Join(",") + ")";
                    break;
                default: return $"Unknown mode {Mode}!";
            }

            return FieldValue == null
                ? "[{0}] {1} <null>".Format(FieldName, operation)
                : "[{0}] {1} '{2}'".Format(FieldName, operation, StringExtensions.ToString(FieldValue));
        }

        /// <inheritdoc />
        public override int GetHashCode() => ToString().GetHashCode();

        /// <summary>Scans a Table for matches with the current search.</summary>
        /// <param name="preselected">The preselected ids.</param>
        /// <param name="layout">Layout of the table.</param>
        /// <param name="indices">FieldIndices or null.</param>
        /// <param name="table">The table to scan.</param>
        /// <returns>All rows matching this search.</returns>
        public IEnumerable<Row> Scan(IEnumerable<Row> preselected, RowLayout layout, IFieldIndex[] indices, ITable table)
        {
            if (table == null) { throw new ArgumentNullException(nameof(table)); }
            if (layout == null) { throw new ArgumentNullException(nameof(layout)); }

            LoadLayout(layout);
            switch (Mode)
            {
                case SearchMode.None:
                {
                    if (Inverted)
                    {
                        throw new InvalidOperationException("Cannot invert an empty search!");
                    }

                    return table.GetRows() ?? preselected;
                }
                case SearchMode.And:
                {
                    var resultA = SearchA.Scan(preselected, layout, indices, table);
                    var result = SearchB.Scan(resultA, layout, indices, table);
                    return !Inverted ? result : Invert(preselected ?? table.GetRows(), result);
                }
                case SearchMode.Or:
                {
                    var result = SearchA.Scan(preselected, layout, indices, table);
                    var resultB = SearchB.Scan(preselected, layout, indices, table);
                    result = result.Union(resultB).ToList();
                    return Inverted ? Invert(preselected ?? table.GetRows(), result) : result;
                }
                case SearchMode.In:
                {
                    IEnumerable<Row> result = new List<Row>();
                    foreach (var value in (Set<object>) FieldValue)
                    {
                        result = result.Union(table.GetRows(FieldEquals(FieldName, value)));
                    }

                    return Inverted ? Invert(preselected ?? table.GetRows(), result) : result;
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
                        // check if we can do an index search
                        if ((indices != null) && (indices[FieldNumber] != null))
                        {
                            // field has an index
                            var result = indices[FieldNumber].Find(FieldValue).Select(r => new Row(Layout, r, true));
                            if (preselected != null)
                            {
                                result = result.Intersect(preselected);
                            }

                            return Inverted ? Invert(preselected ?? table.GetRows(), result) : result;
                        }
#if DEBUG
                        // field has no index, need table scan
                        if ((preselected == null) && (table.RowCount > 1000))
                        {
                            Debug.WriteLine(
                                $"Warning: Doing slow memory search on Table {layout.Name} Field {FieldName}! You should consider adding an index!");
                        }
#endif
                    }

                    // scan without index
                    {
                        var result = new List<Row>();
                        if (preselected != null)
                        {
                            foreach (var row in preselected)
                            {
                                if (Check(row))
                                {
                                    result.Add(row);
                                }
                            }
                        }
                        else
                        {
                            foreach (var row in table.GetRows())
                            {
                                if (Check(row))
                                {
                                    result.Add(row);
                                }
                            }
                        }

                        return result;
                    }
                }
                default: throw new NotImplementedException($"Mode {Mode} not implemented!");
            }
        }

        internal void LoadLayout(RowLayout layout)
        {
            if (Mode == SearchMode.And || Mode == SearchMode.Or || Mode == SearchMode.None)
            {
                return;
            }

            if (Layout != null)
            {
                if (ReferenceEquals(layout, Layout))
                {
                    return;
                }
            }

            Layout = layout ?? throw new ArgumentNullException(nameof(layout));
            if (FieldName == null)
            {
                throw new InvalidOperationException($"Property {nameof(FieldName)} has to be set!");
            }
            FieldNumber = layout.GetFieldIndex(FieldName, true);
            FieldProperties = layout[FieldNumber];

            if (Mode == SearchMode.In)
            {
                var result = new Set<object>();
                foreach (var value in (Set<object>) FieldValue)
                {
                    result.Add(ConvertValue(value));
                }

                FieldValue = result;
            }
            else if (Mode == SearchMode.Like)
            {
                // Do nothing
            }
            else
            {
                if ((FieldValue != null) && (FieldProperties.ValueType != FieldValue.GetType()))
                {
                    FieldValue = ConvertValue(FieldValue);
                }
            }
        }

        internal bool Check(Row row) => Check(row.Values);

        internal bool Check(object[] row)
        {
            var result = true;
            switch (Mode)
            {
                case SearchMode.None:
                    if (Inverted)
                    {
                        result = !result;
                    }

                    return result;
                case SearchMode.And:
                    result = SearchA.Check(row) && SearchB.Check(row);
                    if (Inverted)
                    {
                        result = !result;
                    }

                    return result;
                case SearchMode.Or:
                    result = SearchA.Check(row) || SearchB.Check(row);
                    if (Inverted)
                    {
                        result = !result;
                    }

                    return result;
            }

#if DEBUG
            if (FieldNumber < 0)
            {
                throw new InvalidOperationException("Use LoadLayout first!");
            }

            if (row.Length != Layout.FieldCount)
            {
                throw new ArgumentOutOfRangeException(nameof(row), "Row fieldcount does not match layout!");
            }
#endif
            switch (Mode)
            {
                case SearchMode.Greater:
                {
                    var tableValue = (IComparable) row.GetValue(FieldNumber);
                    if (FieldValue is DateTime)
                    {
                        result = Compare((DateTime) tableValue, (DateTime) FieldValue) > 0;
                    }
                    else
                    {
                        result = tableValue.CompareTo(FieldValue) > 0;
                    }

                    break;
                }
                case SearchMode.GreaterOrEqual:
                {
                    var tableValue = (IComparable) row.GetValue(FieldNumber);
                    if (FieldValue is DateTime)
                    {
                        result = Compare((DateTime) tableValue, (DateTime) FieldValue) >= 0;
                    }
                    else
                    {
                        result = tableValue.CompareTo(FieldValue) >= 0;
                    }

                    break;
                }
                case SearchMode.Smaller:
                {
                    var tableValue = (IComparable) row.GetValue(FieldNumber);
                    if (FieldValue is DateTime)
                    {
                        result = Compare((DateTime) tableValue, (DateTime) FieldValue) < 0;
                    }
                    else
                    {
                        result = tableValue.CompareTo(FieldValue) < 0;
                    }

                    break;
                }
                case SearchMode.SmallerOrEqual:
                {
                    var tableValue = (IComparable) row.GetValue(FieldNumber);
                    if (FieldValue is DateTime)
                    {
                        result = Compare((DateTime) tableValue, (DateTime) FieldValue) <= 0;
                    }
                    else
                    {
                        result = tableValue.CompareTo(FieldValue) <= 0;
                    }

                    break;
                }
                case SearchMode.In:
                {
                    var rowValue = row.GetValue(FieldNumber);
                    result = ((Set<object>) FieldValue).Contains(rowValue);
                }
                    break;
                case SearchMode.Equals:
                {
                    if (FieldValue is DateTime)
                    {
                        result = Compare((DateTime) row.GetValue(FieldNumber), (DateTime) FieldValue) == 0;
                    }
                    else
                    {
                        result = Equals(row.GetValue(FieldNumber), FieldValue);
                    }

                    break;
                }
                case SearchMode.Like:
                {
                    if (FieldProperties == null)
                    {
                        FieldProperties = Layout[FieldNumber];
                        if ((FieldValue != null) && (FieldProperties.ValueType != FieldValue.GetType()))
                        {
                            throw new Exception($"Search for field {FieldProperties}: Value has to be of type {FieldProperties.ValueType}");
                        }
                    }

                    result = Like(row.GetValue(FieldNumber));
                    break;
                }
                default: throw new NotImplementedException($"Mode {Mode} not implemented!");
            }

            if (Inverted)
            {
                result = !result;
            }

            return result;
        }

        /// <summary>Inverts the selection of the specified 'items' using the sorting present at specified 'all'.</summary>
        /// <param name="all">All ids (sorting will be kept).</param>
        /// <param name="items">The items to be inverted.</param>
        /// <returns>The inverted selection.</returns>
        static IEnumerable<Row> Invert(IEnumerable<Row> all, IEnumerable<Row> items) => all.Except(items);

        int Compare(DateTime tableValue, DateTime checkValue)
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

        bool Like(object value)
        {
            if (FieldValue == null)
            {
                return value == null;
            }

            if (value == null)
            {
                return false;
            }

            var text = value.ToString();
            if (Expression == null)
            {
                var valueString = FieldValue.ToString();
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
                Expression = new Regex(s, RegexOptions.IgnoreCase);
            }

            return Expression.IsMatch(text);
        }

        object ConvertValue(object value)
        {
            try
            {
                if (FieldProperties.ValueType.IsPrimitive && FieldProperties.ValueType.IsValueType)
                {
                    try
                    {
                        if (value is IConvertible conv)
                        {
                            return conv.ToType(FieldProperties.ValueType, CultureInfo.InvariantCulture);
                        }
                    }
                    catch { }
                }

                return FieldProperties.ParseValue(value.ToString(), null, CultureInfo.InvariantCulture);
            }
            catch (Exception ex)
            {
                throw new InvalidDataException($"Search {this} cannot convert value {value} to type {FieldProperties}!", ex);
            }
        }
    }
}
