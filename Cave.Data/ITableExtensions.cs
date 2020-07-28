using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using Cave.Collections.Generic;

namespace Cave.Data
{
    /// <summary>Provides extension functions for ITable instances.</summary>
    public static class ITableExtensions
    {
        #region ITable extensions

        #region TryInsert

        /// <summary>Tries to insert the specified dataset (id has to be set).</summary>
        /// <param name="table">The table.</param>
        /// <param name="row">The row.</param>
        /// <returns>Returns true if the dataset was inserted, false otherwise.</returns>
        public static bool TryInsert(this ITable table, Row row)
        {
            // TODO, implement this without exceptions: needed at Table, SqlTable, MemoryTable
            try
            {
                table.Insert(row);
                return true;
            }
            catch
            {
                return false;
            }
        }

        #endregion

        #region TryUpdate

        /// <summary>Tries to insert the specified dataset (id has to be set).</summary>
        /// <param name="table">The table.</param>
        /// <param name="row">The row.</param>
        /// <returns>Returns true if the dataset was inserted, false otherwise.</returns>
        public static bool TryUpdate(this ITable table, Row row)
        {
            // TODO, implement this without exceptions: needed at Table, SqlTable, MemoryTable
            try
            {
                table.Update(row);
                return true;
            }
            catch
            {
                return false;
            }
        }

        #endregion

        #region TryDelete

        /// <summary>Tries to delete the dataset with the specified id.</summary>
        /// <param name="table">The table.</param>
        /// <param name="id">The identifier.</param>
        /// <returns>Returns true if the dataset was removed, false otherwise.</returns>
        /// <typeparam name="TIdentifier">Identifier type. This has to be convertable to the database <see cref="DataType" />.</typeparam>
        public static bool TryDelete<TIdentifier>(this ITable table, TIdentifier id)
        {
            var idField = table.Layout.Where(f => f.Flags.HasFlag(FieldFlags.ID)).SingleOrDefault()
             ?? throw new InvalidOperationException("Could not find identifier field!");
            return table.TryDelete(idField.Name, id) > 0;
        }

        /// <summary>Tries to delete the datasets with the specified identifiers.</summary>
        /// <param name="table">The table.</param>
        /// <param name="ids">The identifiers.</param>
        /// <returns>The number of datasets removed, 0 if the database does not support deletion count or no dataset was removed.</returns>
        /// <typeparam name="TIdentifier">Identifier type. This has to be convertable to the database <see cref="DataType" />.</typeparam>
        public static int TryDelete<TIdentifier>(this ITable table, IEnumerable<TIdentifier> ids)
        {
            var idField = table.Layout.Where(f => f.Flags.HasFlag(FieldFlags.ID)).SingleOrDefault()
             ?? throw new InvalidOperationException("Could not find identifier field!");
            return table.TryDelete(Search.FieldIn(idField.Name, ids));
        }

        /// <summary>Removes all rows from the table matching the given search.</summary>
        /// <param name="table">The table.</param>
        /// <param name="field">The fieldname to match.</param>
        /// <param name="value">The value to match.</param>
        /// <returns>The number of datasets deleted.</returns>
        public static int TryDelete(this ITable table, string field, object value) => table.TryDelete(Search.FieldEquals(field, value));

        #endregion

        #region Exist

        /// <summary>Checks a given search for any datasets matching.</summary>
        /// <param name="table">The table.</param>
        /// <param name="field">The fields name.</param>
        /// <param name="value">The value.</param>
        /// <returns>Returns true if a dataset exists, false otherwise.</returns>
        public static bool Exist(this ITable table, string field, object value) => table.Exist(Search.FieldEquals(field, value));

        #endregion

        /// <summary>Searches the table for a single row with given field value combination.</summary>
        /// <param name="table">The table.</param>
        /// <param name="field">The fieldname to match.</param>
        /// <param name="value">The value to match.</param>
        /// <returns>The row found.</returns>
        public static Row GetRow(this ITable table, string field, object value) => table.GetRow(Search.FieldEquals(field, value));

        /// <summary>Searches the table for rows with given field value combinations.</summary>
        /// <param name="table">The table.</param>
        /// <param name="field">The fieldname to match.</param>
        /// <param name="value">The value to match.</param>
        /// <returns>The rows found.</returns>
        public static IList<Row> GetRows(this ITable table, string field, object value) => table.GetRows(Search.FieldEquals(field, value));

        /// <summary>Caches the whole table into memory and provides a new ITable instance.</summary>
        /// <param name="table">The table.</param>
        /// <returns>Returns a new memory table.</returns>
        public static MemoryTable ToMemory(this ITable table)
        {
            Trace.TraceInformation("Copy {0} rows to memory table", table.RowCount);
            var result = MemoryTable.Create(table.Layout);
            result.LoadTable(table);
            return result;
        }

        /// <summary>Counts the rows with specified field value combination.</summary>
        /// <param name="table">The table.</param>
        /// <param name="field">The fieldname to match.</param>
        /// <param name="value">The value to match.</param>
        /// <returns>The number of rows found matching the criteria given.</returns>
        public static long Count(this ITable table, string field, object value) => table.Count(Search.FieldEquals(field, value), ResultOption.None);

        #region SaveTo

        /// <summary>Saves the table to a dat stream.</summary>
        /// <param name="table">Table to save.</param>
        /// <param name="stream">The stream to save to.</param>
        public static void SaveTo(this ITable table, Stream stream)
        {
            using (var writer = new DatWriter(table.Layout, stream))
            {
                writer.WriteTable(table);
            }
        }

        /// <summary>Saves the table to a dat file.</summary>
        /// <param name="table">Table to save.</param>
        /// <param name="fileName">The filename to save to.</param>
        public static void SaveTo(this ITable table, string fileName)
        {
            using (var stream = File.Create(fileName))
            using (var writer = new DatWriter(table.Layout, stream))
            {
                writer.WriteTable(table);
                writer.Close();
            }
        }

        #endregion

        #region GenerateStruct/-File (ITable)

        /// <summary>Builds the csharp code file containing the row layout structure.</summary>
        /// <param name="table">The table to use.</param>
        /// <param name="databaseName">The database name (only used for the structure name).</param>
        /// <param name="tableName">The table name (only used for the structure name).</param>
        /// <param name="className">The name of the class to generate.</param>
        /// <param name="structFile">The struct file name (defaults to classname.cs).</param>
        /// <param name="namingStrategy">Naming strategy for classes, properties, structures and fields.</param>
        /// <returns>The struct file name.</returns>
        public static GenerateTableCodeResult GenerateStructFile(this ITable table, string databaseName = null, string tableName = null,
            string className = null, string structFile = null, NamingStrategy namingStrategy = NamingStrategy.CamelCase)
            => GenerateStruct(table.Layout, databaseName, tableName, className, namingStrategy).SaveStructFile(structFile);

        /// <summary>Builds the csharp code containing the row layout structure.</summary>
        /// <param name="table">The table to use.</param>
        /// <param name="databaseName">The database name (only used for the structure name).</param>
        /// <param name="tableName">The table name (only used for the structure name).</param>
        /// <param name="className">The name of the class to generate.</param>
        /// <param name="namingStrategy">Naming strategy for classes, properties, structures and fields.</param>
        /// <returns>Returns a string containing csharp code.</returns>
        public static GenerateTableCodeResult GenerateStruct(this ITable table, string databaseName = null, string tableName = null, string className = null,
            NamingStrategy namingStrategy = NamingStrategy.CamelCase)
            => GenerateStruct(table.Layout, databaseName ?? table.Database.Name, tableName, className, namingStrategy);

        #endregion

        #region GenerateStruct/-File (RowLayout)

        /// <summary>Builds the csharp code file containing the row layout structure.</summary>
        /// <param name="layout">The layout to use.</param>
        /// <param name="databaseName">The database name (only used for the structure name).</param>
        /// <param name="tableName">The table name (only used for the structure name).</param>
        /// <param name="className">The name of the class to generate.</param>
        /// <param name="structFile">The struct file name (defaults to classname.cs).</param>
        /// <param name="namingStrategy">Naming strategy for classes, properties, structures and fields.</param>
        /// <returns>The struct file name.</returns>
        public static GenerateTableCodeResult GenerateStructFile(this RowLayout layout, string databaseName = null, string tableName = null,
            string className = null, string structFile = null, NamingStrategy namingStrategy = NamingStrategy.CamelCase)
            => GenerateStruct(layout, databaseName, tableName, className, namingStrategy).SaveStructFile(structFile);

        /// <summary>Builds the csharp code containing the row layout structure.</summary>
        /// <param name="layout">The layout to use.</param>
        /// <param name="databaseName">The database name (only used for the structure name).</param>
        /// <param name="tableName">The table name (only used for the structure name).</param>
        /// <param name="className">The name of the class to generate.</param>
        /// <param name="namingStrategy">Naming strategy for classes, properties, structures and fields.</param>
        /// <returns>Returns a string containing csharp code.</returns>
        public static GenerateTableCodeResult GenerateStruct(this RowLayout layout, string databaseName, string tableName = null, string className = null,
            NamingStrategy namingStrategy = NamingStrategy.CamelCase)
        {
            #region GetName()

            string[] GetNameParts(string text)
                => text.ReplaceInvalidChars(ASCII.Strings.Letters + ASCII.Strings.Digits, "_").Split('_').SelectMany(s => s.SplitCamelCase()).ToArray();

            // TODO: use NamingStrategy
            string GetName(string text) => namingStrategy switch
            {
                NamingStrategy.CamelCase => GetNameParts(text).JoinCamelCase(),
                NamingStrategy.SnakeCase => GetNameParts(text).JoinSnakeCase(),
                NamingStrategy.Exact => text,
                _ => throw new NotImplementedException($"Unknown NamingStrategy {namingStrategy}.")
            };

            #endregion

            if (databaseName == null)
            {
                throw new ArgumentNullException(nameof(databaseName));
            }

            if (tableName == null)
            {
                tableName = layout.Name;
            }

            var fieldNameLookup = new Dictionary<int, string>();
            var idCount = layout.Identifier.Count();
            var idFields = (idCount == 0 ? layout : layout.Identifier).ToList();
            var code = new StringBuilder();
            code.AppendLine("//-----------------------------------------------------------------------");
            code.AppendLine("// <summary>");
            code.AppendLine("// Autogenerated table class");
            code.AppendLine($"// Using {typeof(ITableExtensions).Assembly.FullName}");
            code.AppendLine("// </summary>");
            code.AppendLine("// <auto-generated />");
            code.AppendLine("//-----------------------------------------------------------------------");
            code.AppendLine();
            code.AppendLine("using System;");
            code.AppendLine("using System.Globalization;");
            code.AppendLine("using Cave.Data;");

            #region Build lookup tables

            void BuildLookupTables()
            {
                var uniqueFieldNames = new IndexedSet<string>();
                foreach (var field in layout)
                {
                    var sharpName = GetName(field.Name);
                    var i = 0;
                    while (uniqueFieldNames.Contains(sharpName))
                    {
                        sharpName = GetName(field.Name) + ++i;
                    }

                    uniqueFieldNames.Add(sharpName);
                    fieldNameLookup[field.Index] = sharpName;
                }
            }

            BuildLookupTables();

            #endregion

            if (className == null)
            {
                className = GetName(databaseName) + GetName(tableName) + "Row";
            }

            code.AppendLine();
            code.AppendLine("namespace Database");
            code.AppendLine("{");
            code.AppendLine($"\t/// <summary>Table structure for {layout}.</summary>");
            code.AppendLine($"\t[Table(\"{layout.Name}\")]");
            code.AppendLine($"\tpublic partial struct {className} : IEquatable<{className}>");
            code.AppendLine("\t{");

            #region static Parse()

            code.AppendLine($"\t\t/// <summary>Converts the string representation of a row to its {className} equivalent.</summary>");
            code.AppendLine("\t\t/// <param name=\"data\">A string that contains a row to convert.</param>");
            code.AppendLine($"\t\t/// <returns>A new {className} instance.</returns>");
            code.AppendLine($"\t\tpublic static {className} Parse(string data) => Parse(data, CultureInfo.InvariantCulture);");
            code.AppendLine();
            code.AppendLine($"\t\t/// <summary>Converts the string representation of a row to its {className} equivalent.</summary>");
            code.AppendLine("\t\t/// <param name=\"data\">A string that contains a row to convert.</param>");
            code.AppendLine("\t\t/// <param name=\"provider\">The format provider (optional).</param>");
            code.AppendLine($"\t\t/// <returns>A new {className} instance.</returns>");
            code.AppendLine($"\t\tpublic static {className} Parse(string data, IFormatProvider provider) => CsvReader.ParseRow<{className}>(data, provider);");

            #endregion

            #region Add fields

            foreach (var field in layout)
            {
                code.AppendLine();
                code.AppendLine($"\t\t/// <summary>{field} {field.Description}.</summary>");
                if (!string.IsNullOrEmpty(field.Description))
                {
                    code.AppendLine($"\t\t[Description(\"{field} {field.Description}\")]");
                }

                code.Append("\t\t[Field(");
                var i = 0;

                void AddAttribute<T>(T value, Func<string> content)
                {
                    if (Equals(value, default))
                    {
                        return;
                    }

                    if (i++ > 0)
                    {
                        code.Append(", ");
                    }

                    code.Append(content());
                }

                if (field.Flags != 0)
                {
                    code.Append("Flags = ");
                    var flagCount = 0;
                    foreach (var flag in field.Flags.GetFlags())
                    {
                        if (flagCount++ > 0)
                        {
                            code.Append(" | ");
                        }

                        code.Append("FieldFlags.");
                        code.Append(flag);
                    }

                    code.Append(", ");
                }

                var sharpName = fieldNameLookup[field.Index];
                if (sharpName != field.Name)
                {
                    AddAttribute(field.Name, () => $"Name = \"{field.Name}\"");
                }

                if (field.MaximumLength < int.MaxValue)
                {
                    AddAttribute(field.MaximumLength, () => $"Length = {(int) field.MaximumLength}");
                }

                AddAttribute(field.AlternativeNames, () => $"AlternativeNames = \"{field.AlternativeNames.Join(", ")}\"");
                AddAttribute(field.DisplayFormat, () => $"DisplayFormat = \"{field.DisplayFormat.EscapeUtf8()}\"");
                code.AppendLine(")]");
                if ((field.DateTimeKind != DateTimeKind.Unspecified) || (field.DateTimeType != DateTimeType.Undefined))
                {
                    code.AppendLine($"\t\t[DateTimeFormat({field.DateTimeKind}, {field.DateTimeType})]");
                }

                if (field.StringEncoding != 0)
                {
                    code.AppendLine($"\t\t[Cave.IO.StringFormat(Cave.IO.StringEncoding.{field.StringEncoding})]");
                }

                code.AppendLine($"\t\tpublic {field.DotNetTypeName} {sharpName};");
            }

            #endregion

            #region ToString()

            {
                code.AppendLine();
                code.AppendLine("\t\t/// <summary>Gets a string representation of this row.</summary>");
                code.AppendLine("\t\t/// <returns>Returns a string that can be parsed by <see cref=\"Parse(string)\"/>.</returns>");
                code.AppendLine("\t\tpublic override string ToString() => ToString(CultureInfo.InvariantCulture);");
                code.AppendLine();
                code.AppendLine("\t\t/// <summary>Gets a string representation of this row.</summary>");
                code.AppendLine("\t\t/// <returns>Returns a string that can be parsed by <see cref=\"Parse(string, IFormatProvider)\"/>.</returns>");
                code.AppendLine("\t\tpublic string ToString(IFormatProvider provider) => CsvWriter.RowToString(this, provider);");
            }

            #endregion

            #region GetHashCode()

            {
                code.AppendLine();
                if (idCount == 1)
                {
                    var idField = layout.Identifier.First();
                    var idFieldName = fieldNameLookup[idField.Index];
                    code.AppendLine($"\t\t/// <summary>Gets the hash code for the identifier of this row (field {idFieldName}).</summary>");
                    code.AppendLine("\t\t/// <returns>A hash code for the identifier of this row.</returns>");
                    code.Append("\t\tpublic override int GetHashCode() => ");
                    code.Append(idFieldName);
                    code.AppendLine(".GetHashCode();");
                }
                else
                {
                    if (idCount == 0)
                    {
                        code.AppendLine("\t\t/// <summary>Gets the hash code based on all values of this row (no identififer defined).</summary>");
                    }
                    else
                    {
                        var names = idFields.Select(field => fieldNameLookup[field.Index]).Join(", ");
                        code.AppendLine($"\t\t/// <summary>Gets the hash code for the identifier of this row (fields {names}).</summary>");
                    }

                    code.AppendLine("\t\t/// <returns>A hash code for the identifier of this row.</returns>");
                    code.AppendLine("\t\tpublic override int GetHashCode() =>");
                    var first = true;
                    foreach (var idField in idFields)
                    {
                        if (first)
                        {
                            first = false;
                        }
                        else
                        {
                            code.AppendLine(" ^");
                        }

                        code.Append($"\t\t\t{fieldNameLookup[idField.Index]}.GetHashCode()");
                    }

                    code.AppendLine(";");
                }
            }

            #endregion

            #region Equals()

            {
                code.AppendLine();
                code.AppendLine("\t\t/// <inheritdoc/>");
                code.AppendLine($"\t\tpublic override bool Equals(object other) => other is {className} row && Equals(row);");
                code.AppendLine();
                code.AppendLine("\t\t/// <inheritdoc/>");
                code.AppendLine($"\t\tpublic bool Equals({className} other)");
                code.AppendLine("\t\t{");
                code.AppendLine("\t\t\treturn");
                {
                    var first = true;
                    foreach (var field in layout)
                    {
                        if (first)
                        {
                            first = false;
                        }
                        else
                        {
                            code.AppendLine(" &&");
                        }

                        var name = fieldNameLookup[field.Index];
                        code.Append($"\t\t\t\tEquals(other.{name}, {name})");
                    }

                    code.AppendLine(";");
                }
                code.AppendLine("\t\t}");
            }

            #endregion

            code.AppendLine("\t}");
            code.AppendLine("}");
            code.Replace("\t", "    ");
            return new GenerateTableCodeResult
            {
                ClassName = className, TableName = tableName, DatabaseName = databaseName, Code = code.ToString()
            };
        }

        #endregion

        #region SaveStructFile

        /// <summary>Saves the generated code to a file and returns the updated result.</summary>
        /// <param name="result">Result to update.</param>
        /// <param name="structFile">Name of the file to write to (optional).</param>
        /// <returns>Returns an updated result instance.</returns>
        public static GenerateTableCodeResult SaveStructFile(this GenerateTableCodeResult result, string structFile = null)
        {
            if (result.FileName == null)
            {
                if (structFile == null)
                {
                    result.FileName = result.ClassName + ".cs";
                }
                else
                {
                    result.FileName = structFile;
                }
            }

            File.WriteAllText(result.FileName, result.Code);
            return result;
        }

        #endregion

        #endregion

        #region ITable<TStruct> extensions

        /// <summary>Caches the whole table into memory and provides a new ITable{TStruct} instance.</summary>
        /// <typeparam name="TStruct">Structure type.</typeparam>
        /// <param name="table">The table.</param>
        /// <returns>Returns a new memory table.</returns>
        public static ITable<TStruct> ToMemory<TStruct>(this ITable<TStruct> table)
            where TStruct : struct
        {
            Trace.TraceInformation("Copy {0} rows to memory table", table.RowCount);
            var result = MemoryTable.Create(table.Layout);
            result.LoadTable(table);
            return new Table<TStruct>(result);
        }

        /// <summary>Searches the table for rows with given field value combinations.</summary>
        /// <typeparam name="TStruct">Structure type.</typeparam>
        /// <param name="table">The table.</param>
        /// <param name="field">The fieldname to match.</param>
        /// <param name="value">The value to match.</param>
        /// <returns>The rows found.</returns>
        public static IList<TStruct> GetStructs<TStruct>(this ITable<TStruct> table, string field, object value)
            where TStruct : struct =>
            table.GetStructs(Search.FieldEquals(field, value));

        /// <summary>Tries to get the row with the specified <paramref name="value" /> from <paramref name="table" />.</summary>
        /// <typeparam name="TStruct">The row structure type.</typeparam>
        /// <param name="table">The table.</param>
        /// <param name="field">The fieldname to match.</param>
        /// <param name="value">The value to match.</param>
        /// <param name="row">Returns the result row.</param>
        /// <returns>Returns true on success, false otherwise.</returns>
        /// <exception cref="InvalidOperationException">The result sequence contains more than one element.</exception>
        public static bool TryGetStruct<TStruct>(this ITable<TStruct> table, string field, object value, out TStruct row)
            where TStruct : struct
        {
            var result = GetStructs(table, field, value);
            if (result.Count > 0)
            {
                row = result.Single();
                return true;
            }

            row = default;
            return false;
        }

        #endregion

        #region ITable<TKey, TStruct> extensions

        /// <summary>Tries to get the row with the specified <paramref name="key" /> from <paramref name="table" />.</summary>
        /// <typeparam name="TKey">The identifier field type.</typeparam>
        /// <typeparam name="TStruct">The row structure type.</typeparam>
        /// <param name="table">The table.</param>
        /// <param name="key">The identifier value.</param>
        /// <param name="row">Returns the result row.</param>
        /// <returns>Returns true on success, false otherwise.</returns>
        /// <exception cref="InvalidOperationException">The result sequence contains more than one element.</exception>
        public static bool TryGetStruct<TKey, TStruct>(this ITable<TKey, TStruct> table, TKey key, out TStruct row)
            where TKey : IComparable<TKey>
            where TStruct : struct
        {
            var result = table.GetStructs(new[] { key });
            if (result.Count > 0)
            {
                row = result.Single();
                return true;
            }

            row = default;
            return false;
        }

        /// <summary>Caches the whole table into memory and provides a new ITable{TStruct} instance.</summary>
        /// <typeparam name="TKey">Key identifier type.</typeparam>
        /// <typeparam name="TStruct">Row structure type.</typeparam>
        /// <param name="table">The table.</param>
        /// <returns>Returns a new memory table.</returns>
        public static ITable<TKey, TStruct> ToMemory<TKey, TStruct>(this ITable<TKey, TStruct> table)
            where TKey : IComparable<TKey>
            where TStruct : struct
        {
            var result = MemoryTable.Create(table.Layout);
            result.LoadTable(table);
            return new Table<TKey, TStruct>(result);
        }

        #endregion
    }
}
