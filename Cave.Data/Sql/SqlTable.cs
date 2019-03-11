using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Text;
using Cave.Collections.Generic;

namespace Cave.Data.Sql
{
    /// <summary>
    /// Provides a table implementation for generic sql92 databases.
    /// </summary>
    public abstract class SqlTable : Table
    {
        /// <summary>
        /// Obtains the command to retrieve the last inserted row.
        /// </summary>
        /// <param name="row">The row to be inserted.</param>
        /// <returns></returns>
        protected virtual string GetLastInsertedIDCommand(Row row)
        {
            if (!SqlStorage.SupportsNamedParameters)
            {
                throw new NotSupportedException(string.Format("The default GetLastInsertedIDCommand is not available for databases not supporting named parameters!"));
            }
            StringBuilder commandBuilder = new StringBuilder();
            commandBuilder.Append("SELECT ");
            commandBuilder.Append(SqlStorage.EscapeFieldName(Layout.IDField));
            commandBuilder.Append(" FROM ");
            commandBuilder.Append(FQTN);
            commandBuilder.Append(" WHERE ");
            int n = 0;
            for (int i = 0; i < FieldCount; i++)
            {
                FieldProperties fieldProperties = Layout.GetProperties(i);
                if ((fieldProperties.Flags & FieldFlags.ID) != 0)
                {
                    continue;
                }

                if (++n > 1)
                {
                    commandBuilder.Append(" AND ");
                }

                commandBuilder.Append(SqlStorage.EscapeFieldName(fieldProperties));
                commandBuilder.Append("=");
                commandBuilder.Append(SqlStorage.ParameterPrefix);
                if (SqlStorage.SupportsNamedParameters)
                {
                    commandBuilder.Append(fieldProperties.NameAtDatabase);
                }
            }
            commandBuilder.AppendLine(";");
            return commandBuilder.ToString();
        }

        #region public properties

        /// <summary>
        /// Obtains the full qualified table name.
        /// </summary>
        public readonly string FQTN;

        #endregion

        #region protected properties

        /// <summary>
        /// The used <see cref="SqlStorage"/> backend.
        /// </summary>
        protected SqlStorage SqlStorage { get; private set; }

        #endregion

        #region constructor

        static RowLayout GetLayout(IDatabase database, string table)
        {
            if (database == null)
            {
                throw new ArgumentNullException("Database");
            }

            SqlStorage storage = database.Storage as SqlStorage;
            if (storage == null)
            {
                throw new InvalidOperationException(string.Format("Database has to be a SqlDatabase!"));
            }

            return storage.QuerySchema(database.Name, table);
        }

        /// <summary>
        /// Creates a new SqlTable instance (retrieves layout from database).
        /// </summary>
        /// <param name="database">The database this table belongs to.</param>
        /// <param name="name">The name of the table.</param>
        protected SqlTable(IDatabase database, string name)
            : this(database, GetLayout(database, name))
        {
        }

        /// <summary>
        /// Creates a new SqlTable instance (checks layout against database).
        /// </summary>
        /// <param name="database">The database this table belongs to.</param>
        /// <param name="layout">The layout of the table.</param>
        protected SqlTable(IDatabase database, RowLayout layout)
            : base(database, layout)
        {
            SqlStorage = database.Storage as SqlStorage;
            if (SqlStorage == null)
            {
                throw new InvalidOperationException(string.Format("Database has to be a SqlDatabase!"));
            }

            FQTN = SqlStorage.FQTN(database.Name, layout.Name);
            RowLayout schema = SqlStorage.QuerySchema(database.Name, layout.Name);
            SqlStorage.CheckLayout(Name, schema, Layout);
        }
        #endregion

        #region implemented functionality

        #region ITable interface implementation

        /// <summary>
        /// Sets the specified value to a field.
        /// </summary>
        /// <param name="field">The name of the field.</param>
        /// <param name="value">The value.</param>
        public override void SetValue(string field, object value)
        {
            string command = "UPDATE " + FQTN + " SET " + SqlStorage.EscapeFieldName(Layout.GetProperties(field));
            if (value == null)
            {
                SqlStorage.Execute(Database.Name, Name, command + "=NULL");
            }
            else
            {
                DatabaseParameter parameter = new DatabaseParameter(field, value);
                if (SqlStorage.SupportsNamedParameters)
                {
                    SqlStorage.Execute(Database.Name, Name, command + "=" + SqlStorage.ParameterPrefix + parameter.Name + ";", parameter);
                }
                else
                {
                    SqlStorage.Execute(Database.Name, Name, command + "=" + SqlStorage.ParameterPrefix + ";", parameter);
                }
            }
            IncreaseSequenceNumber();
        }

        /// <summary>
        /// Obtains a row from the table.
        /// </summary>
        /// <param name="id">The ID of the row to be fetched.</param>
        /// <returns>Returns the row.</returns>
        public override Row GetRow(long id)
        {
            return SqlStorage.QueryRow(Layout, Database.Name, Name, "SELECT * FROM " + FQTN + " WHERE " + SqlStorage.EscapeFieldName(Layout.IDField) + "=" + id.ToString());
        }

        /// <summary>
        /// Obtains an array with all rows.
        /// </summary>
        /// <returns></returns>
        public override List<Row> GetRows()
        {
            return SqlStorage.Query(Layout, Database.Name, Name, "SELECT * FROM " + FQTN);
        }

        /// <summary>
        /// Obtains the rows with the given ids.
        /// </summary>
        /// <param name="ids">IDs of the rows to fetch from the table.</param>
        /// <returns>Returns the rows.</returns>
        public override List<Row> GetRows(IEnumerable<long> ids)
        {
            if (ids == null)
            {
                throw new ArgumentNullException("IDs");
            }

            StringBuilder command = new StringBuilder();
            command.AppendLine($"SELECT * FROM {FQTN} WHERE");
            bool first = true;
            string idFieldName = Layout.IDField.NameAtDatabase;
            foreach (long id in ids.AsSet())
            {
                if (first)
                {
                    first = false;
                    command.AppendLine($"({idFieldName}={id})");
                }
                else
                {
                    command.AppendLine($"OR({idFieldName}={id})");
                }
            }
            return first ? new List<Row>() : SqlStorage.Query(Layout, Database.Name, Name, command.ToString());
        }

        /// <summary>Obtains all different field values of a given field.</summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="field"></param>
        /// <param name="includeNull">allow null value to be added to the results.</param>
        /// <param name="ids"></param>
        /// <returns></returns>
        public override IItemSet<T> GetValues<T>(string field, bool includeNull = false, IEnumerable<long> ids = null)
        {
            string fieldName = SqlStorage.EscapeFieldName(Layout.GetProperties(field));
            RowLayout layout = RowLayout.CreateUntyped("Layout", new FieldProperties(Name, FieldFlags.None, DataType.String, field));
            string query;
            if (ids == null)
            {
                query = $"SELECT {fieldName} FROM {FQTN} WHERE {Layout.IDField.Name} GROUP BY {fieldName}";
            }
            else
            {
                query = $"SELECT {fieldName} FROM {FQTN} WHERE {Layout.IDField.Name} IN ({StringExtensions.Join(ids, ",")}) GROUP BY {fieldName}";
            }
            List<Row> rows = SqlStorage.Query(layout, Database.Name, Name, query);
            Set<T> result = new Set<T>();
            foreach (Row row in rows)
            {
                T value = (T)Fields.ConvertValue(typeof(T), row.GetValue(0), CultureInfo.InvariantCulture);
                if (includeNull || (value != null))
                {
                    result.Include(value);
                }
            }
            return result;
        }

        /// <summary>Calculates the sum of the specified field name for all matching rows.</summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="search">The search.</param>
        /// <returns></returns>
        public override double Sum(string fieldName, Search search = null)
        {
            search = search ?? Search.None;
            SqlSearch s = search.ToSql(Layout, SqlStorage);
            StringBuilder command = new StringBuilder();
            command.Append("SELECT SUM(");
            command.Append(SqlStorage.EscapeFieldName(Layout.GetProperties(fieldName)));
            command.Append(") FROM ");
            command.Append(FQTN);
            command.Append(" WHERE ");
            command.Append(s.ToString());
            double result = Convert.ToDouble(SqlStorage.QueryValue(Database.Name, Name, command.ToString(), s.Parameters.ToArray()));
            FieldProperties field = Layout.GetProperties(fieldName);
            switch (field.DataType)
            {
                case DataType.Binary:
                case DataType.DateTime:
                case DataType.String:
                case DataType.User:
                case DataType.Unknown:
                    throw new NotSupportedException($"Sum() is not supported for field {field}!");

                case DataType.TimeSpan: result *= TimeSpan.TicksPerSecond; break;
                default: break;
            }
            return result;
        }

        /// <summary>
        /// Checks a given ID for existance.
        /// </summary>
        /// <param name="id">The dataset ID to look for.</param>
        /// <returns>Returns whether the dataset exists or not.</returns>
        public override bool Exist(long id)
        {
            object value = SqlStorage.QueryValue(Database.Name, Name, "SELECT COUNT(*) FROM " + FQTN + " WHERE " + SqlStorage.EscapeFieldName(Layout.IDField) + "=" + id.ToString());
            return int.Parse(value.ToString()) > 0;
        }

        #region sql92 commands
        /// <summary>Creates the insert command.</summary>
        /// <param name="commandBuilder">The command builder.</param>
        /// <param name="row">The row.</param>
        /// <returns>Returns a value &gt; 0 (valid ID) or &lt;= 0 for autoincrement ids.</returns>
        public virtual long CreateInsert(SqlCommandBuilder commandBuilder, Row row)
        {
            commandBuilder.Append("INSERT INTO ");
            commandBuilder.Append(FQTN);
            commandBuilder.Append(" (");
            StringBuilder parameterBuilder = new StringBuilder();
            bool firstCommand = true;
            bool autoSetID = false;
            bool autoIncrementID = false;

            // autoset id ?
            long id = Layout.GetID(row);
            if (id <= 0)
            {
                autoSetID = true;

                // yes, autoinc ?
                autoIncrementID = ((Layout.IDField.Flags & FieldFlags.AutoIncrement) != 0);
            }

            // prepare ID field
            if (autoSetID && !autoIncrementID)
            {
                commandBuilder.Append(SqlStorage.EscapeFieldName(Layout.IDField));
                id = GetNextFreeID();
                parameterBuilder.Append(id);
                firstCommand = false;
            }

            for (int i = 0; i < FieldCount; i++)
            {
                if (autoSetID && (i == Layout.IDFieldIndex))
                {
                    continue;
                }

                if (firstCommand)
                {
                    firstCommand = false;
                }
                else
                {
                    commandBuilder.Append(", ");
                    parameterBuilder.Append(", ");
                }

                FieldProperties fieldProperties = Layout.GetProperties(i);

                commandBuilder.Append(SqlStorage.EscapeFieldName(fieldProperties));

                object value = SqlStorage.GetDatabaseValue(fieldProperties, row.GetValue(i));
                if (value == null)
                {
                    parameterBuilder.Append("NULL");
                }
                else if (!TransactionsUseParameters)
                {
                    parameterBuilder.Append(SqlStorage.EscapeFieldValue(Layout.GetProperties(i), value));
                }
                else
                {
                    DatabaseParameter parameter = new DatabaseParameter(fieldProperties.NameAtDatabase, value);
                    commandBuilder.AddParameter(parameter);
                    parameterBuilder.Append(SqlStorage.ParameterPrefix);
                    if (SqlStorage.SupportsNamedParameters)
                    {
                        parameterBuilder.Append(parameter.Name);
                    }
                }
            }

            commandBuilder.Append(") VALUES (");
            commandBuilder.Append(parameterBuilder.ToString());
            commandBuilder.Append(")");

            commandBuilder.AppendLine(";");
            return id;
        }

        /// <summary>Creates an update command.</summary>
        /// <param name="commandBuilder">The command builder.</param>
        /// <param name="row">The row.</param>
        protected virtual void CreateUpdate(SqlCommandBuilder commandBuilder, Row row)
        {
            commandBuilder.Append("UPDATE ");
            commandBuilder.Append(FQTN);
            commandBuilder.Append(" SET ");
            for (int i = 0; i < Layout.FieldCount; i++)
            {
                if (i > 0)
                {
                    commandBuilder.Append(",");
                }

                FieldProperties fieldProperties = Layout.GetProperties(i);
                commandBuilder.Append(SqlStorage.EscapeFieldName(fieldProperties));
                object value = row.GetValue(i);
                if (value == null)
                {
                    commandBuilder.Append("=NULL");
                }
                else
                {
                    commandBuilder.Append("=");
                    value = SqlStorage.GetDatabaseValue(Layout.GetProperties(i), value);
                    if (TransactionsUseParameters)
                    {
                        commandBuilder.CreateAndAddParameter(value);
                    }
                    else
                    {
                        commandBuilder.Append(SqlStorage.EscapeFieldValue(Layout.GetProperties(i), value));
                    }
                }
            }
            commandBuilder.Append(" WHERE ");
            commandBuilder.Append(SqlStorage.EscapeFieldName(Layout.IDField));
            commandBuilder.Append("=");
            commandBuilder.Append(SqlStorage.GetDatabaseValue(Layout.IDField, Layout.GetID(row)).ToString());
            commandBuilder.AppendLine(";");
        }

        /// <summary>Creates a replace command.</summary>
        /// <param name="cb">The command builder.</param>
        /// <param name="row">The row.</param>
        protected virtual void CreateReplace(SqlCommandBuilder cb, Row row)
        {
            cb.Append("REPLACE INTO ");
            cb.Append(FQTN);
            cb.Append(" VALUES (");
            for (int i = 0; i < Layout.FieldCount; i++)
            {
                if (i > 0)
                {
                    cb.Append(",");
                }

                object value = row.GetValue(i);
                if (value == null)
                {
                    cb.Append("NULL");
                }
                else
                {
                    value = SqlStorage.GetDatabaseValue(Layout.GetProperties(i), value);
                    if (TransactionsUseParameters)
                    {
                        cb.CreateAndAddParameter(value);
                    }
                    else
                    {
                        cb.Append(SqlStorage.EscapeFieldValue(Layout.GetProperties(i), value));
                    }
                }
            }
            cb.AppendLine(");");
        }
        #endregion

        #region sql92 insert
        /// <summary>
        /// Inserts a row to the table. If an ID <![CDATA[<]]> 0 is given an automatically generated ID will be used to add the dataset.
        /// </summary>
        /// <param name="row">The row to insert.</param>
        /// <returns>Returns the ID of the inserted dataset.</returns>
        public override long Insert(Row row)
        {
            SqlCommandBuilder commandBuilder = new SqlCommandBuilder(Database);
            long id = CreateInsert(commandBuilder, row);
            if (id <= 0)
            {
                commandBuilder.Append(GetLastInsertedIDCommand(row));
                id = Convert.ToInt64(SqlStorage.QueryValue(Database.Name, Name, commandBuilder.ToString(), commandBuilder.Parameters));
            }
            else
            {
                SqlStorage.Execute(Database.Name, Name, commandBuilder.ToString(), commandBuilder.Parameters);
            }
            IncreaseSequenceNumber();
            return id;
        }

        #endregion

        #region replace (available at most databases via replace alias)
        /// <summary>
        /// Replaces a row at the table. The ID has to be given. This inserts (if the row does not exist) or updates (if it exists) the row.
        /// </summary>
        /// <param name="row">The row to replace (valid ID needed).</param>
        public override void Replace(Row row)
        {
            long id = Layout.GetID(row);
            if (id <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(row), "Row ID is invalid!");
            }

            SqlCommandBuilder commandBuilder = new SqlCommandBuilder(Database);
            CreateReplace(commandBuilder, row);
            commandBuilder.Execute();
            IncreaseSequenceNumber();
        }

        #endregion

        #region sql92 update
        /// <summary>
        /// Updates a row to the table. The row must exist already!.
        /// </summary>
        /// <param name="row">The row to update.</param>
        public override void Update(Row row)
        {
            long id = Layout.GetID(row);
            if (id <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(row), "Row ID is invalid!");
            }

            SqlCommandBuilder commandBuilder = new SqlCommandBuilder(Database);
            CreateUpdate(commandBuilder, row);
            commandBuilder.Execute();
            IncreaseSequenceNumber();
        }
        #endregion

        #region sql92 delete
        /// <summary>
        /// Removes a row from the table.
        /// </summary>
        /// <param name="id">The dataset ID to remove.</param>
        public override void Delete(long id)
        {
            StringBuilder commandBuilder = new StringBuilder();
            commandBuilder.Append("DELETE FROM ");
            commandBuilder.Append(FQTN);
            commandBuilder.Append(" WHERE ");
            commandBuilder.Append(SqlStorage.EscapeFieldName(Layout.IDField));
            commandBuilder.Append("=");
            commandBuilder.Append(SqlStorage.GetDatabaseValue(Layout.IDField, id));
            SqlStorage.Execute(Database.Name, Name, commandBuilder.ToString());
            IncreaseSequenceNumber();
        }

        /// <summary>Removes all rows from the table matching the specified search.</summary>
        /// <param name="search">The Search used to identify rows for removal.</param>
        /// <returns>Returns the number of dataset deleted.</returns>
        public override int TryDelete(Search search)
        {
            if (search == null)
            {
                search = Search.None;
            }

            SqlSearch s = search.ToSql(Layout, SqlStorage);
            string command = "DELETE FROM " + FQTN + " WHERE " + s.ToString();
            int result = SqlStorage.Execute(Database.Name, Name, command, s.Parameters.ToArray());
            IncreaseSequenceNumber();
            return result;
        }

        #endregion

        /// <summary>
        /// Clears all rows of the table.
        /// </summary>
        public override void Clear()
        {
            SqlStorage.Execute(Database.Name, Name, "DELETE FROM " + FQTN);
            IncreaseSequenceNumber();
        }

        /// <summary>
        /// Counts the results of a given search.
        /// </summary>
        /// <param name="search">The search to run.</param>
        /// <param name="resultOption">Options for the search and the result set.</param>
        /// <returns></returns>
        public override long Count(Search search = default(Search), ResultOption resultOption = default(ResultOption))
        {
            if (search == null)
            {
                search = Search.None;
            }

            if (resultOption == null)
            {
                resultOption = ResultOption.None;
            }

            SqlSearch s = search.ToSql(Layout, SqlStorage);
            return resultOption != ResultOption.None
                ? SqlCountIDs(s, resultOption)
                : Convert.ToInt64(SqlStorage.QueryValue(Database.Name, Name, "SELECT COUNT(*) FROM " + FQTN + " WHERE " + s.ToString(), s.Parameters.ToArray()));
        }

        /// <summary>
        /// Checks a given search for any datasets matching.
        /// </summary>
        /// <param name="search"></param>
        /// <returns></returns>
        public override bool Exist(Search search)
        {
            if (search == null)
            {
                search = Search.None;
            }

            SqlSearch s = search.ToSql(Layout, SqlStorage);
            string query = "SELECT DISTINCT 1 FROM " + FQTN + " WHERE " + s.ToString();
            return SqlStorage.Query(null, Database.Name, Name, query, s.Parameters.ToArray()).Count > 0;
        }

        /// <summary>
        /// Obtains the RowCount.
        /// </summary>
        public override long RowCount
        {
            get
            {
                object value = SqlStorage.QueryValue(Database.Name, Name, "SELECT COUNT(*) FROM " + FQTN);
                return Convert.ToInt64(value);
            }
        }

        #region FindRow(s) amd FindGroup implementation

        #region sql92 protected find functions
        /// <summary>
        /// Searches for grouped datasets and returns the id of the first occurence (sql handles this differently).
        /// </summary>
        /// <param name="search"></param>
        /// <param name="option"></param>
        /// <returns></returns>
        protected internal virtual List<Row> SqlGetGroupRows(SqlSearch search, ResultOption option)
        {
            RowLayout layout;
            StringBuilder command = new StringBuilder();
            command.Append("SELECT ");
            if (SqlStorage.SupportsAllFieldsGroupBy)
            {
                layout = Layout;
                command.Append("*");
            }
            else
            {
                layout = null;
                int fieldNumber = 0;
                foreach (string fieldName in search.FieldNames)
                {
                    if (fieldNumber++ > 0)
                    {
                        command.Append(", ");
                    }

                    command.Append(SqlStorage.EscapeFieldName(Layout.GetProperties(fieldName)));
                }
            }
            command.Append(" FROM ");
            command.Append(FQTN);
            command.Append(" WHERE ");

            command.Append(search.ToString());

            int groupCount = 0;
            foreach (ResultOption o in option.ToArray(ResultOptionMode.Group))
            {
                if (groupCount++ == 0)
                {
                    command.Append(" GROUP BY ");
                }
                else
                {
                    command.Append(",");
                }
                command.Append(SqlStorage.EscapeFieldName(Layout.GetProperties(o.Parameter)));
            }

            return SqlStorage.Query(layout, Database.Name, Name, command.ToString(), search.Parameters.ToArray());
        }

        /// <summary>
        /// Searches for grouped datasets and returns the id of the first occurence (sql handles this differently).
        /// </summary>
        /// <param name="search"></param>
        /// <param name="option"></param>
        /// <returns></returns>
        protected internal virtual List<long> SqlFindGroupIDs(SqlSearch search, ResultOption option)
        {
            List<Row> rows = SqlGetGroupRows(search, option);
            List<long> result = new List<long>(rows.Count);
            if (SqlStorage.SupportsAllFieldsGroupBy)
            {
                foreach (Row row in rows)
                {
                    result.Add(Layout.GetID(row));
                }
            }
            else
            {
                foreach (Row row in rows)
                {
                    Search singleSearch = Search.None;
                    for (int i = 0; i < Layout.FieldCount; i++)
                    {
                        if (i == Layout.IDFieldIndex)
                        {
                            continue;
                        }

                        singleSearch &= Search.FieldEquals(Layout.GetProperties(i).Name, row.GetValue(i));
                    }
                    result.Add(FindRow(singleSearch, ResultOption.Limit(1) + ResultOption.SortDescending(SqlStorage.EscapeFieldName(Layout.IDField))));
                }
            }
            return result;
        }

        /// <summary>
        /// Searches the table for rows with given field value combinations.
        /// </summary>
        /// <param name="search">The search to run.</param>
        /// <param name="option">Options for the search and the result set.</param>
        /// <returns>Returns the ID of the row found or -1.</returns>
        protected internal virtual List<long> SqlFindIDs(SqlSearch search, ResultOption option)
        {
            if (option.Contains(ResultOptionMode.Group))
            {
                return SqlFindGroupIDs(search, option);
            }
            StringBuilder command = new StringBuilder();
            command.Append("SELECT ");
            if (Layout.IDField != null)
            {
                command.Append(SqlStorage.EscapeFieldName(Layout.IDField));
            }

            foreach (string fieldName in search.FieldNames)
            {
                if (fieldName == Layout.IDField.Name)
                {
                    continue;
                }

                if (Layout.GetFieldIndex(fieldName) < 0)
                {
                    throw new DataException(string.Format("Field {0} could not be found!", fieldName));
                }

                command.Append(", ");
                command.Append(SqlStorage.EscapeFieldName(Layout.GetProperties(fieldName)));
            }

            command.Append(" FROM ");
            command.Append(FQTN);
            command.Append(" WHERE ");

            command.Append(search.ToString());

            int orderCount = 0;
            foreach (ResultOption o in option.ToArray(ResultOptionMode.SortAsc, ResultOptionMode.SortDesc))
            {
                if (orderCount++ == 0)
                {
                    command.Append(" ORDER BY ");
                }
                else
                {
                    command.Append(",");
                }
                command.Append(SqlStorage.EscapeFieldName(Layout.GetProperties(o.Parameter)));
                if (o.Mode == ResultOptionMode.SortAsc)
                {
                    command.Append(" ASC");
                }
                else
                {
                    command.Append(" DESC");
                }
            }

            if (option.Contains(ResultOptionMode.Group))
            {
                throw new InvalidOperationException(string.Format("Cannot use Option.Group and Option.Sort at once!"));
            }

            int limit = 0;
            foreach (ResultOption o in option.ToArray(ResultOptionMode.Limit))
            {
                if (limit++ > 0)
                {
                    throw new InvalidOperationException(string.Format("Cannot set two different limits!"));
                }

                command.Append(" LIMIT " + o.Parameter);
            }
            int offset = 0;
            foreach (ResultOption o in option.ToArray(ResultOptionMode.Offset))
            {
                if (offset++ > 0)
                {
                    throw new InvalidOperationException(string.Format("Cannot set two different offsets!"));
                }

                if (limit == 0)
                {
                    throw new InvalidOperationException(string.Format("Cannot use offset without limit!"));
                }

                command.Append(" OFFSET " + o.Parameter);
            }

            List<Row> rows = SqlStorage.Query(null, Database.Name, Name, command.ToString(), search.Parameters.ToArray());
            List<long> result = new List<long>();
            foreach (Row row in rows)
            {
                result.Add(Convert.ToInt64(row.GetValue(0)));
            }
            return result;
        }

        /// <summary>
        /// Searches for grouped datasets and returns the id of the first occurence (sql handles this differently).
        /// </summary>
        /// <param name="search"></param>
        /// <param name="option"></param>
        /// <returns></returns>
        protected internal virtual long SqlCountGroupIDs(SqlSearch search, ResultOption option)
        {
            StringBuilder command = new StringBuilder();
            command.Append("SELECT COUNT(");
            if (SqlStorage.SupportsAllFieldsGroupBy)
            {
                command.Append("*");
            }
            else
            {
                int fieldNumber = 0;
                foreach (string fieldName in search.FieldNames)
                {
                    if (fieldNumber++ > 0)
                    {
                        command.Append(", ");
                    }

                    command.Append(SqlStorage.EscapeFieldName(Layout.GetProperties(fieldName)));
                }
            }
            command.Append(") FROM ");
            command.Append(FQTN);
            command.Append(" WHERE ");

            command.Append(search.ToString());

            if (option.Contains(ResultOptionMode.Limit) | option.Contains(ResultOptionMode.Offset))
            {
                throw new InvalidOperationException(string.Format("Cannot use Option.Group and Option.Limit/Offset at once!"));
            }

            int groupCount = 0;
            foreach (ResultOption o in option.ToArray(ResultOptionMode.Group))
            {
                if (groupCount++ == 0)
                {
                    command.Append(" GROUP BY ");
                }
                else
                {
                    command.Append(",");
                }
                command.Append(SqlStorage.EscapeFieldName(Layout.GetProperties(o.Parameter)));
            }

            return Convert.ToInt64(SqlStorage.QueryValue(Database.Name, Name, command.ToString(), search.Parameters.ToArray()));
        }

        /// <summary>
        /// Searches the table for rows with given field value combinations.
        /// </summary>
        /// <param name="search">The search to run.</param>
        /// <param name="option">Options for the search and the result set.</param>
        /// <returns>Returns the ID of the row found or -1.</returns>
        protected internal virtual long SqlCountIDs(SqlSearch search, ResultOption option)
        {
            if (option.Contains(ResultOptionMode.Group))
            {
                return SqlCountGroupIDs(search, option);
            }
            StringBuilder command = new StringBuilder();
            command.Append("SELECT COUNT(*) FROM ");
            command.Append(FQTN);
            command.Append(" WHERE ");
            command.Append(search.ToString());
            foreach (ResultOption o in option.ToArray())
            {
                switch (o.Mode)
                {
                    case ResultOptionMode.SortAsc:
                    case ResultOptionMode.SortDesc:
                    case ResultOptionMode.None:
                        break;
                    default:
                        throw new InvalidOperationException(string.Format("ResultOptionMode {0} not supported!", o.Mode));
                }
            }
            return Convert.ToInt64(SqlStorage.QueryValue(Database.Name, Name, command.ToString(), search.Parameters.ToArray()));
        }

        /// <summary>
        /// Searches the table for rows with given field value combinations.
        /// </summary>
        /// <param name="search">The search to run.</param>
        /// <param name="option">Options for the search and the result set.</param>
        /// <returns>Returns the ID of the row found or -1.</returns>
        protected internal virtual List<Row> SqlGetRows(SqlSearch search, ResultOption option)
        {
            if (option.Contains(ResultOptionMode.Group))
            {
                return SqlGetGroupRows(search, option);
            }
            StringBuilder command = new StringBuilder();
            command.Append("SELECT * FROM ");
            command.Append(FQTN);
            command.Append(" WHERE ");

            command.Append(search.ToString());

            int orderCount = 0;
            foreach (ResultOption o in option.ToArray(ResultOptionMode.SortAsc, ResultOptionMode.SortDesc))
            {
                if (orderCount++ == 0)
                {
                    command.Append(" ORDER BY ");
                }
                else
                {
                    command.Append(",");
                }
                command.Append(SqlStorage.EscapeFieldName(Layout.GetProperties(o.Parameter)));
                if (o.Mode == ResultOptionMode.SortAsc)
                {
                    command.Append(" ASC");
                }
                else
                {
                    command.Append(" DESC");
                }
            }

            int limit = 0;
            foreach (ResultOption o in option.ToArray(ResultOptionMode.Limit))
            {
                if (limit++ > 0)
                {
                    throw new InvalidOperationException(string.Format("Cannot set two different limits!"));
                }

                command.Append(" LIMIT " + o.Parameter);
            }
            int offset = 0;
            foreach (ResultOption o in option.ToArray(ResultOptionMode.Offset))
            {
                if (offset++ > 0)
                {
                    throw new InvalidOperationException(string.Format("Cannot set two different offsets!"));
                }

                command.Append(" OFFSET " + o.Parameter);
            }

            return SqlStorage.Query(Layout, Database.Name, Name, command.ToString(), search.Parameters.ToArray());
        }
        #endregion

        /// <summary>
        /// Searches the table for rows with given field value combinations.
        /// </summary>
        /// <param name="search">The search to run.</param>
        /// <param name="resultOption">Options for the search and the result set.</param>
        /// <returns>Returns the ID of the row found or -1.</returns>
        public override List<long> FindRows(Search search = default(Search), ResultOption resultOption = default(ResultOption))
        {
            if (search == null)
            {
                search = Search.None;
            }

            if (resultOption == null)
            {
                resultOption = ResultOption.None;
            }

            SqlSearch s = search.ToSql(Layout, SqlStorage);
            s.CheckFieldsPresent(resultOption);
            return SqlFindIDs(s, resultOption);
        }

        /// <summary>
        /// Searches the table for rows with given field value combinations.
        /// </summary>
        /// <param name="search">The search to run.</param>
        /// <param name="resultOption">Options for the search and the result set.</param>
        /// <returns>Returns the rows found.</returns>
        public override List<Row> GetRows(Search search = default(Search), ResultOption resultOption = default(ResultOption))
        {
            if (search == null)
            {
                search = Search.None;
            }

            if (resultOption == null)
            {
                resultOption = ResultOption.None;
            }

            SqlSearch s = search.ToSql(Layout, SqlStorage);
            s.CheckFieldsPresent(resultOption);
            return SqlGetRows(s, resultOption);
        }

        #endregion

        #region free / used id lookup

        /// <summary>
        /// Obtains the next used ID at the table (positive values are valid, negative ones are invalid, 0 is not defined!).
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public override long GetNextUsedID(long id)
        {
            object obj = SqlStorage.QueryValue(Database.Name, Name, "SELECT MIN(" + SqlStorage.EscapeFieldName(Layout.IDField) + ") FROM " + FQTN +
                " WHERE " + SqlStorage.EscapeFieldName(Layout.IDField) + " > " + id);
            return obj == null ? -1 : Convert.ToInt64(obj);
        }

        /// <summary>
        /// Obtains the next free ID at the table.
        /// </summary>
        /// <returns></returns>
        public override long GetNextFreeID()
        {
            long count = Convert.ToInt64(SqlStorage.QueryValue(Database.Name, Name, "SELECT COUNT(*) FROM " + FQTN));
            if (count == 0)
            {
                return 1L;
            }

            long value = Convert.ToInt64(SqlStorage.QueryValue(Database.Name, Name, "SELECT MAX(" + SqlStorage.EscapeFieldName(Layout.IDField) + ") FROM " + FQTN));
            return Math.Max(1L, 1L + value);
        }

        #endregion

        #endregion

        #region Transaction interface

        /// <summary>Gets or sets a value indicating whether [transactions use parameters during commit].</summary>
        /// <value>
        /// <c>true</c> if [transactions use parameters]; otherwise, <c>false</c>.
        /// </value>
        /// <remarks>
        /// Using parameters is recommended since escaping is not needed and when transfering binary data the transmission size is much smaller.
        /// On the other side even with escaping all values execution without parameters is faster. This is because
        /// the use of parameters increase the computation time on the server side by an average of 5-10%.
        /// </remarks>
        public virtual bool TransactionsUseParameters { get; }

        void InternalCommit(Transaction[] transactions)
        {
            SqlCommandBuilder commandBuilder = new SqlCommandBuilder(Database);
            commandBuilder.AppendLine("START TRANSACTION;");
            foreach (Transaction transaction in transactions)
            {
                switch (transaction.Type)
                {
                    #region TransactionType.Inserted
                    case TransactionType.Inserted:
                    {
                        CreateInsert(commandBuilder, transaction.Row);
                    }
                    break;
                    #endregion

                    #region TransactionType.Replaced
                    case TransactionType.Replaced:
                    {
                        CreateReplace(commandBuilder, transaction.Row);
                    }
                    break;
                    #endregion

                    #region TransactionType.Updated
                    case TransactionType.Updated:
                    {
                        CreateUpdate(commandBuilder, transaction.Row);
                    }
                    break;
                    #endregion

                    #region TransactionType.Deleted
                    case TransactionType.Deleted:
                    {
                        commandBuilder.Append("DELETE FROM ");
                        commandBuilder.Append(FQTN);
                        commandBuilder.Append(" WHERE ");
                        commandBuilder.Append(SqlStorage.EscapeFieldName(Layout.IDField));
                        commandBuilder.Append("=");
                        commandBuilder.Append(SqlStorage.GetDatabaseValue(Layout.IDField, transaction.ID).ToString());
                        commandBuilder.AppendLine(";");
                    }
                    break;
                    #endregion

                    default: throw new NotImplementedException();
                }
            }
            commandBuilder.AppendLine("COMMIT;");
            try
            {
                commandBuilder.Execute();
                IncreaseSequenceNumber();
            }
            catch (Exception ex)
            {
                Trace.TraceWarning("Error committing transaction[{0}] to table <red>{1}\n{2}", transactions.Length, FQTN, ex);
                Trace.TraceInformation("Command: {0}", commandBuilder.Text);
                throw;
            }
        }

        /// <summary>Commits a whole TransactionLog to the table.</summary>
        /// <param name="transactionLog">The transaction log to read.</param>
        /// <param name="flags">The flags to use.</param>
        /// <param name="count">Number of transactions to combine at one write.</param>
        /// <returns>Returns the number of transactions done or -1 if unknown.</returns>
        public override int Commit(TransactionLog transactionLog, TransactionFlags flags = TransactionFlags.Default, int count = -1)
        {
            Transaction[] transactions = transactionLog.Dequeue(count);
            if (transactions.Length == 0)
            {
                return 0;
            }

            try
            {
                InternalCommit(transactions);
                return transactions.Length;
            }
            catch
            {
                if (0 != (flags & TransactionFlags.AllowRequeue))
                {
                    transactionLog.Requeue(true, transactions);
                }

                if (0 != (flags & TransactionFlags.ThrowExceptions))
                {
                    throw;
                }

                return -1;
            }
        }

        #endregion

        #endregion

        /// <summary>Gets or sets the default automatic increment value (used if no id is specified).</summary>
        /// <value>The automatic increment value.</value>
        public string AutoIncrementValue { get; set; } = "NULL";

        /// <summary>
        /// "Database.Table".
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return Database + "." + Name;
        }
    }

    /// <summary>
    /// Provides a table implementation for generic sql92 databases.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public abstract class SqlTable<T> : SqlTable, ITable<T>
        where T : struct
    {
        /// <summary>Creates a new SqlTable instance.</summary>
        /// <param name="database">The database this table belongs to.</param>
        /// <param name="layout">The layout of the table.</param>
        protected SqlTable(IDatabase database, RowLayout layout)
            : base(database, CheckTypedLayout(typeof(T), database, layout))
        {
        }

        #region sql92 protected find functions

        /// <summary>Runs a sql group function and returns the result as structures.</summary>
        /// <param name="search">The search.</param>
        /// <param name="options">The options.</param>
        /// <returns>Returns a list of structures.</returns>
        protected internal virtual List<T> SqlGetGroupStructs(SqlSearch search, ResultOption options)
        {
            List<Row> rows = SqlGetGroupRows(search, options);
            List<T> result = new List<T>(rows.Count);
            if (SqlStorage.SupportsAllFieldsGroupBy)
            {
                foreach (Row row in rows)
                {
                    result.Add(row.GetStruct<T>(Layout));
                }
            }
            else
            {
                foreach (Row row in rows)
                {
                    Search singleSearch = Search.None;
                    for (int i = 0; i < Layout.FieldCount; i++)
                    {
                        if (i == Layout.IDFieldIndex)
                        {
                            continue;
                        }

                        singleSearch &= Search.FieldEquals(Layout.GetProperties(i).Name, row.GetValue(i));
                    }
                    result.Add(GetStruct(singleSearch, ResultOption.Limit(1) + ResultOption.SortDescending(SqlStorage.EscapeFieldName(Layout.IDField))));
                }
            }
            return result;
        }

        /// <summary>
        /// Searches the table for rows with given field value combinations.
        /// </summary>
        /// <param name="search">The search to run.</param>
        /// <param name="options">Options for the search and the result set.</param>
        /// <returns>Returns the ID of the row found or -1.</returns>
        protected internal virtual List<T> SqlGetStructs(SqlSearch search, ResultOption options)
        {
            if (options.Contains(ResultOptionMode.Group))
            {
                return SqlGetGroupStructs(search, options);
            }
            StringBuilder command = new StringBuilder();
            command.Append("SELECT * FROM ");
            command.Append(FQTN);
            command.Append(" WHERE ");

            command.Append(search.ToString());

            int orderCount = 0;
            foreach (ResultOption o in options.ToArray(ResultOptionMode.SortAsc, ResultOptionMode.SortDesc))
            {
                if (orderCount++ == 0)
                {
                    command.Append(" ORDER BY ");
                }
                else
                {
                    command.Append(",");
                }
                command.Append(SqlStorage.EscapeFieldName(Layout.GetProperties(o.Parameter)));
                if (o.Mode == ResultOptionMode.SortAsc)
                {
                    command.Append(" ASC");
                }
                else
                {
                    command.Append(" DESC");
                }
            }

            int limit = 0;
            foreach (ResultOption o in options.ToArray(ResultOptionMode.Limit))
            {
                if (limit++ > 0)
                {
                    throw new InvalidOperationException(string.Format("Cannot set two different limits!"));
                }

                command.Append(" LIMIT " + o.Parameter);
            }
            int offset = 0;
            foreach (ResultOption o in options.ToArray(ResultOptionMode.Offset))
            {
                if (offset++ > 0)
                {
                    throw new InvalidOperationException(string.Format("Cannot set two different offsets!"));
                }

                command.Append(" OFFSET " + o.Parameter);
            }

            return SqlStorage.Query<T>(Database.Name, Name, command.ToString(), search.Parameters.ToArray());
        }
        #endregion

        #region implemented functionality

        #region non virtual

        /// <summary>
        /// Obtains the row struct with the given index.
        /// This allows a memorytable to be used as virtual list for listviews, ...
        /// Note that indices may change on each update, insert, delete and sorting is not garanteed!.
        /// </summary>
        /// <param name="index">The rows index (0..RowCount-1).</param>
        /// <returns></returns>
        /// <exception cref="IndexOutOfRangeException"></exception>
        public T GetStructAt(int index)
        {
            return GetRowAt(index).GetStruct<T>(Layout);
        }

        /// <summary>
        /// Obtains the rows with the given ids.
        /// </summary>
        /// <param name="ids">IDs of the rows to fetch from the table.</param>
        /// <returns>Returns the rows.</returns>
        public List<T> GetStructs(IEnumerable<long> ids)
        {
            if (ids == null)
            {
                throw new ArgumentNullException("IDs");
            }

            StringBuilder command = new StringBuilder();
            command.AppendLine($"SELECT * FROM {FQTN} WHERE");
            bool first = true;
            string idFieldName = Layout.IDField.NameAtDatabase;
            foreach (long id in ids.AsSet())
            {
                if (first)
                {
                    first = false;
                    command.AppendLine($"({idFieldName}={id})");
                }
                else
                {
                    command.AppendLine($"OR({idFieldName}={id})");
                }
            }
            return first ? new List<T>() : SqlStorage.Query<T>(Database.Name, Name, command.ToString());
        }

        /// <summary>
        /// Inserts rows into the table using a transaction.
        /// </summary>
        /// <param name="rows">The rows to insert.</param>
        public void Insert(IEnumerable<T> rows)
        {
            if (rows == null)
            {
                throw new ArgumentNullException("Rows");
            }

            TransactionLog<T> l = new TransactionLog<T>();
            foreach (T r in rows) { l.AddInserted(r); }
            Commit(l);
        }

        /// <summary>
        /// Updates rows at the table. The rows must exist already!.
        /// </summary>
        /// <param name="rows">The rows to update.</param>
        public void Update(IEnumerable<T> rows)
        {
            if (rows == null)
            {
                throw new ArgumentNullException("Rows");
            }

            TransactionLog<T> l = new TransactionLog<T>();
            foreach (T r in rows) { l.AddUpdated(r); }
            Commit(l);
        }

        /// <summary>
        /// Replaces rows at the table. This inserts (if the row does not exist) or updates (if it exists) each row.
        /// </summary>
        /// <param name="rows">The rows to replace (valid ID needed).</param>
        public void Replace(IEnumerable<T> rows)
        {
            if (rows == null)
            {
                throw new ArgumentNullException("Rows");
            }

            TransactionLog<T> l = new TransactionLog<T>();
            foreach (T i in rows) { l.AddReplaced(i); }
            Commit(l);
        }

        /// <summary>Tries to get the (unique) row with the given fieldvalue.</summary>
        /// <param name="search">The search.</param>
        /// <param name="row">The row.</param>
        /// <returns>Returns true on success, false otherwise.</returns>
        public bool TryGetStruct(Search search, out T row)
        {
            long id = FindRow(search);
            if (id > -1)
            {
                row = GetStruct(id);
                return true;
            }
            row = new T();
            return false;
        }
        #endregion

        #region virtual
        /// <summary>
        /// Obtains a row from the table.
        /// </summary>
        /// <param name="id">The ID of the row to be fetched.</param>
        /// <returns>Returns the row.</returns>
        public virtual T GetStruct(long id)
        {
            return SqlStorage.QueryRow<T>(Database.Name, Name, "SELECT * FROM " + FQTN + " WHERE " + SqlStorage.EscapeFieldName(Layout.IDField) + "=" + id.ToString());
        }

        /// <summary>
        /// Searches the table for a single row with given search.
        /// </summary>
        /// <param name="search">The search to run.</param>
        /// <param name="resultOption">Options for the search and the result set.</param>
        /// <returns>Returns the row found.</returns>
        public virtual T GetStruct(Search search = default(Search), ResultOption resultOption = default(ResultOption))
        {
            if (search == null)
            {
                search = Search.None;
            }

            if (resultOption == null)
            {
                resultOption = ResultOption.None;
            }

            SqlSearch s = search.ToSql(Layout, SqlStorage);
            s.CheckFieldsPresent(resultOption);
            List<T> rows = SqlGetStructs(s, resultOption);
            if (rows.Count == 0)
            {
                throw new ArgumentException(string.Format("Dataset could not be found!"));
            }

            if (rows.Count > 1)
            {
                throw new InvalidDataException("Dataset not unique!");
            }

            return rows[0];
        }

        /// <summary>
        /// Searches the table for rows with given field value combinations.
        /// </summary>
        /// <param name="search">The search to run.</param>
        /// <param name="resultOption">Options for the search and the result set.</param>
        /// <returns>Returns the rows found.</returns>
        public virtual List<T> GetStructs(Search search = default(Search), ResultOption resultOption = default(ResultOption))
        {
            if (search == null)
            {
                search = Search.None;
            }

            if (resultOption == null)
            {
                resultOption = ResultOption.None;
            }

            SqlSearch s = search.ToSql(Layout, SqlStorage);
            s.CheckFieldsPresent(resultOption);
            return SqlGetStructs(s, resultOption);
        }

        /// <summary>
        /// Inserts a row to the table. If an ID <![CDATA[<=]]> 0 is given an automatically generated ID will be used to add the dataset.
        /// </summary>
        /// <param name="row">The row to insert.</param>
        /// <returns>Returns the ID of the inserted dataset.</returns>
        public virtual long Insert(T row)
        {
            return Insert(Row.Create(Layout, row));
        }

        /// <summary>
        /// Updates a row to the table. The row must exist already!.
        /// </summary>
        /// <param name="row">The row to update.</param>
        public virtual void Update(T row)
        {
            Update(Row.Create(Layout, row));
        }

        /// <summary>
        /// Replaces a row at the table. The ID has to be given. This inserts (if the row does not exist) or updates (if it exists) the row.
        /// </summary>
        /// <param name="row">The row to replace (valid ID needed).</param>
        public virtual void Replace(T row)
        {
            Replace(Row.Create(Layout, row));
        }

        /// <summary>
        /// Checks whether a row is present unchanged at the database and removes it.
        /// (Use Delete(ID) to delete a DataSet without any checks).
        /// </summary>
        /// <param name="row"></param>
        public virtual void Delete(T row)
        {
            long id = Layout.GetID(row);
            T data = GetStruct(id);
            if (!data.Equals(row))
            {
                throw new DataException(string.Format("Row does not match row at database!"));
            }

            Delete(id);
        }

        /// <summary>
        /// Provides access to the row with the specified ID.
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public virtual T this[long id]
        {
            get => GetStruct(id);
            set
            {
                long i = Layout.GetID(value);
                if (i != id)
                {
                    throw new InvalidDataException(string.Format("ID mismatch!"));
                }

                Replace(value);
            }
        }

        /// <summary>
        /// Copies all rows to a given array.
        /// </summary>
        /// <param name="rowArray"></param>
        /// <param name="startIndex"></param>
        public void CopyTo(T[] rowArray, int startIndex)
        {
            GetStructs().CopyTo(rowArray, startIndex);
        }

        #endregion

        #endregion
    }
}
