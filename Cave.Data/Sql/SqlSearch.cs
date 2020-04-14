using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Text;
using Cave.Collections.Generic;

namespace Cave.Data.Sql
{
    /// <summary>
    /// Provides a class used during custom searches to keep up with all parameters to be
    /// added during sql command generation.
    /// </summary>
    public sealed class SqlSearch
    {
        readonly List<SqlParam> parameters = new List<SqlParam>();
        readonly IndexedSet<string> fieldNames = new IndexedSet<string>();
        readonly SqlStorage storage;
        readonly RowLayout layout;
        readonly string text;

        /// <summary>
        /// Initializes a new instance of the <see cref="SqlSearch"/> class.
        /// </summary>
        /// <param name="storage">Storage engine used.</param>
        /// <param name="layout">Layout of the table.</param>
        /// <param name="search">Search to perform.</param>
        public SqlSearch(SqlStorage storage, RowLayout layout, Search search)
        {
            this.storage = storage ?? throw new ArgumentNullException(nameof(storage));
            this.layout = layout ?? throw new ArgumentNullException(nameof(layout));
            Parameters = new ReadOnlyCollection<SqlParam>(parameters);
            FieldNames = new ReadOnlyCollection<string>(fieldNames);
            var sb = new StringBuilder();
            Flatten(sb, search);
            text = sb.ToString();
        }

        /// <summary>
        /// Gets the field names.
        /// </summary>
        public IList<string> FieldNames { get; }

        /// <summary>
        /// Gets the parameters.
        /// </summary>
        public IList<SqlParam> Parameters { get; }

        /// <summary>
        /// Checks whether all fields used at options are present and adds them if not.
        /// </summary>
        /// <param name="option">Options to check.</param>
        public void CheckFieldsPresent(ResultOption option)
        {
            foreach (var fieldName in option.FieldNames)
            {
                if (!FieldNames.Contains(fieldName))
                {
                    FieldNames.Add(fieldName);
                }
            }
        }

        /// <summary>
        /// Gets the query text as string.
        /// </summary>
        /// <returns>A the database specific search string.</returns>
        public override string ToString() => text;

        void Flatten(StringBuilder sb, Search search)
        {
            search.LoadLayout(layout);
            switch (search.Mode)
            {
                case SearchMode.None:
                    sb.Append("1=1");
                    return;

                case SearchMode.In:
                {
                    fieldNames.Include(search.FieldName);
                    var fieldName = storage.EscapeFieldName(search.FieldProperties);
                    sb.Append(fieldName);
                    sb.Append(" ");
                    if (search.Inverted)
                    {
                        sb.Append("NOT ");
                    }

                    sb.Append("IN (");
                    var i = 0;
                    foreach (var value in (Set<object>)search.FieldValue)
                    {
                        if (i++ > 0)
                        {
                            sb.Append(",");
                        }

                        var dbValue = storage.GetDatabaseValue(search.FieldProperties, value);
                        var parameter = AddParameter(dbValue);
                        sb.Append(parameter.Name);
                    }
                    sb.Append(")");
                    return;
                }

                case SearchMode.Equals:
                {
                    fieldNames.Include(search.FieldName);
                    var fieldName = storage.EscapeFieldName(search.FieldProperties);

                    // is value null -> yes return "name IS [NOT] NULL"
                    if (search.FieldValue == null)
                    {
                        sb.Append($"{fieldName} IS {(search.Inverted ? "NOT " : string.Empty)}NULL");
                    }
                    else
                    {
                        // no add parameter and return querytext
                        var dbValue = storage.GetDatabaseValue(search.FieldProperties, search.FieldValue);
                        var parameter = AddParameter(dbValue);
                        sb.Append($"{fieldName} {(search.Inverted ? "<>" : "=")} {parameter.Name}");
                    }
                    break;
                }

                case SearchMode.Like:
                {
                    fieldNames.Include(search.FieldName);
                    var fieldName = storage.EscapeFieldName(search.FieldProperties);

                    // is value null -> yes return "name IS [NOT] NULL"
                    if (search.FieldValue == null)
                    {
                        sb.Append($"{fieldName} IS {(search.Inverted ? "NOT " : string.Empty)}NULL");
                    }
                    else
                    {
                        // no add parameter and return querytext
                        var dbValue = storage.GetDatabaseValue(search.FieldProperties, search.FieldValue);
                        var parameter = AddParameter(dbValue);
                        sb.Append($"{fieldName} {(search.Inverted ? "NOT " : string.Empty)}LIKE {parameter.Name}");
                    }
                    break;
                }

                case SearchMode.Greater:
                {
                    fieldNames.Include(search.FieldName);
                    var fieldName = storage.EscapeFieldName(search.FieldProperties);
                    var dbValue = storage.GetDatabaseValue(search.FieldProperties, search.FieldValue);
                    var parameter = AddParameter(dbValue);
                    sb.Append(search.Inverted ? $"{fieldName}<={parameter.Name}" : $"{fieldName}>{parameter.Name}");
                    break;
                }

                case SearchMode.GreaterOrEqual:
                {
                    fieldNames.Include(search.FieldName);
                    var fieldName = storage.EscapeFieldName(search.FieldProperties);
                    var dbValue = storage.GetDatabaseValue(search.FieldProperties, search.FieldValue);
                    var parameter = AddParameter(dbValue);
                    sb.Append(search.Inverted ? $"{fieldName}<{parameter.Name}" : $"{fieldName}>={parameter.Name}");
                    break;
                }

                case SearchMode.Smaller:
                {
                    fieldNames.Include(search.FieldName);
                    var name = storage.EscapeFieldName(search.FieldProperties);
                    var dbValue = storage.GetDatabaseValue(search.FieldProperties, search.FieldValue);
                    var parameter = AddParameter(dbValue);
                    sb.Append(search.Inverted ? $"{name}>={parameter.Name}" : $"{name}<{parameter.Name}");
                    break;
                }

                case SearchMode.SmallerOrEqual:
                {
                    fieldNames.Include(search.FieldName);
                    var name = storage.EscapeFieldName(search.FieldProperties);
                    var dbValue = storage.GetDatabaseValue(search.FieldProperties, search.FieldValue);
                    var parameter = AddParameter(dbValue);
                    sb.Append(search.Inverted ? $"{name}>{parameter.Name}" : $"{name}<={parameter.Name}");
                    break;
                }

                case SearchMode.And:
                {
                    if (search.Inverted)
                    {
                        sb.Append("NOT ");
                    }
                    sb.Append("(");
                    Flatten(sb, search.SearchA);
                    sb.Append(" AND ");
                    Flatten(sb, search.SearchB);
                    sb.Append(")");
                    break;
                }

                case SearchMode.Or:
                {
                    if (search.Inverted)
                    {
                        sb.Append("NOT ");
                    }
                    sb.Append("(");
                    Flatten(sb, search.SearchA);
                    sb.Append(" OR ");
                    Flatten(sb, search.SearchB);
                    sb.Append(")");
                    break;
                }

                default: throw new NotImplementedException($"Mode {search.Mode} not implemented!");
            }
        }

        /// <summary>
        /// Adds a new parameter.
        /// </summary>
        /// <param name="databaseValue">The databaseValue of the parameter.</param>
        /// <returns>A new <see cref="SqlParam"/> instance.</returns>
        SqlParam AddParameter(object databaseValue)
        {
            var name = storage.SupportsNamedParameters ? $"{storage.ParameterPrefix}{Parameters.Count + 1}" : storage.ParameterPrefix;
            SqlParam parameter = new SqlParam(name, databaseValue);
            parameters.Add(parameter);
            return parameter;
        }
    }
}
