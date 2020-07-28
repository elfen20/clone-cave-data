using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Cave.Collections.Generic;

namespace Cave.Data.Sql
{
    /// <summary>Provides a table implementation for generic sql92 databases.</summary>
    public abstract class SqlTable : Table
    {
        /// <summary>Gets the name of the table.</summary>
        /// <returns>Database.Tablename.</returns>
        public override string ToString() => Storage.FQTN(Database.Name, Name);

        /// <summary>Gets the command to retrieve the last inserted row.</summary>
        /// <param name="commandBuilder">The command builder to append to.</param>
        /// <param name="row">The row to retrieve.</param>
        protected abstract void CreateLastInsertedRowCommand(SqlCommandBuilder commandBuilder, Row row);

        /// <summary>Retrieves the full layout information for this table.</summary>
        /// <param name="database">Database name.</param>
        /// <param name="table">Table name.</param>
        /// <returns>Returns a new <see cref="RowLayout" /> instance.</returns>
        protected virtual RowLayout QueryLayout(string database, string table)
        {
            RowLayout layout = null;
            Storage.Query($"SELECT * FROM {Storage.FQTN(database, table)} WHERE 1 = 0", ref layout, database, table);
            return layout;
        }

        #region constructor

        #endregion

        #region properties

        #region FQTN

        /// <summary>Gets the full qualified table name.</summary>
        public string FQTN { get; private set; }

        #endregion

        #region RowCount

        /// <inheritdoc />
        public override long RowCount
        {
            get
            {
                var value = Storage.QueryValue(database: Database.Name, table: Name, cmd: "SELECT COUNT(*) FROM " + FQTN);
                if (value == null)
                {
                    throw new InvalidDataException($"Could not read value from {FQTN}!");
                }

                return Convert.ToInt64(value);
            }
        }

        #endregion

        /// <summary>Gets or sets the used <see cref="Sql.SqlStorage" /> backend.</summary>
        public new SqlStorage Storage { get; set; }

        #endregion

        #region public ITable functions

        /// <inheritdoc />
        public override void Connect(IDatabase database, TableFlags flags, RowLayout layout)
        {
            Storage = database.Storage as SqlStorage;
            if (Storage == null)
            {
                throw new InvalidOperationException("Database has to be a SqlDatabase!");
            }

            FQTN = Storage.FQTN(database.Name, layout.Name);
            var schema = QueryLayout(database.Name, layout.Name);
            Storage.CheckLayout(layout, schema);
            base.Connect(database, flags, schema);
        }

        /// <summary>Initializes the interface class. This is the first method to call after create.</summary>
        /// <param name="database">Database the table belongs to.</param>
        /// <param name="flags">Flags used to connect to the table.</param>
        /// <param name="tableName">Table name to load.</param>
        public void Initialize(IDatabase database, TableFlags flags, string tableName)
        {
            Storage = database.Storage as SqlStorage;
            if (Storage == null)
            {
                throw new InvalidOperationException("Database has to be a SqlDatabase!");
            }

            FQTN = Storage.FQTN(database.Name, tableName);
            var schema = QueryLayout(database.Name, tableName);
            base.Connect(database, flags, schema);
        }

        #region SetValue

        /// <inheritdoc />
        public override void SetValue(string fieldName, object value)
        {
            var field = Layout[fieldName];
            var command = "UPDATE " + FQTN + " SET " + Storage.EscapeFieldName(field);
            if (value == null)
            {
                Storage.Execute(command + "=NULL");
            }
            else
            {
                var parameter = new SqlParam(Storage.ParameterPrefix, value);
                Execute(new SqlCmd($"{command}={parameter.Name};", parameter));
            }

            IncreaseSequenceNumber();
        }

        #endregion

        #region GetValues

        /// <inheritdoc />
        public override IList<TValue> GetValues<TValue>(string fieldname, Search search = null)
        {
            var escapedFieldName = Storage.EscapeFieldName(Layout[fieldname]);
            var field = new FieldProperties
            {
                Name = fieldname,
                NameAtDatabase = fieldname,
                Flags = FieldFlags.None,
                DataType = DataType.String,
                TypeAtDatabase = DataType.String
            };
            field.Validate();
            SqlCmd query;
            if (search == null)
            {
                query = $"SELECT {escapedFieldName} FROM {FQTN}";
            }
            else
            {
                var s = ToSqlSearch(search);
                query = new SqlCmd($"SELECT {escapedFieldName} FROM {FQTN} WHERE {s}", s.Parameters);
            }

            var rows = Storage.Query(query, Database.Name, Name);
            var result = new List<TValue>();
            foreach (var row in rows)
            {
                var value = (TValue) Fields.ConvertValue(typeof(TValue), row[0], CultureInfo.InvariantCulture);
                result.Add(value);
            }

            return result.AsList();
        }

        /// <inheritdoc />
        public override IList<TValue> Distinct<TValue>(string fieldname, Search search = null)
        {
            var escapedFieldName = Storage.EscapeFieldName(Layout[fieldname]);
            var field = new FieldProperties
            {
                Name = fieldname,
                NameAtDatabase = fieldname,
                Flags = FieldFlags.None,
                DataType = DataType.String,
                TypeAtDatabase = DataType.String
            };
            field.Validate();
            string query;
            if (search == null)
            {
                query = $"SELECT DISTINCT {escapedFieldName} FROM {FQTN}";
            }
            else
            {
                var s = ToSqlSearch(search);
                query = $"SELECT DISTINCT {escapedFieldName} FROM {FQTN} WHERE {s}";
            }

            var rows = Storage.Query(query, Database.Name, Name);
            var result = new Set<TValue>();
            foreach (var row in rows)
            {
                var value = (TValue) Fields.ConvertValue(typeof(TValue), row[0], CultureInfo.InvariantCulture);
                result.Include(value);
            }

            return result.AsList();
        }

        #endregion

        #region Sum

        /// <inheritdoc />
        public override double Sum(string fieldName, Search search = null)
        {
            var field = Layout[fieldName];
            search ??= Search.None;
            var s = ToSqlSearch(search);
            var command = new StringBuilder();
            command.Append("SELECT SUM(");
            command.Append(Storage.EscapeFieldName(field));
            command.Append(") FROM ");
            command.Append(FQTN);
            command.Append(" WHERE ");
            command.Append(s);
            var result = double.NaN;
            var value = Storage.QueryValue(new SqlCmd(command.ToString(), s.Parameters.ToArray()));
            if (value == null)
            {
                throw new InvalidDataException($"Could not read value from {FQTN}!");
            }

            switch (field.DataType)
            {
                case DataType.Binary:
                case DataType.DateTime:
                case DataType.String:
                case DataType.User:
                case DataType.Unknown:
                    throw new NotSupportedException($"Sum() is not supported for field {field}!");
                case DataType.TimeSpan:
                    switch (field.DateTimeType)
                    {
                        case DateTimeType.BigIntHumanReadable:
                        case DateTimeType.Undefined:
                            throw new NotSupportedException($"Sum() is not supported for field {field}!");
                        case DateTimeType.BigIntTicks:
                            result = Convert.ToDouble(value) / TimeSpan.TicksPerSecond;
                            break;
                        case DateTimeType.DecimalSeconds:
                        case DateTimeType.Native:
                        case DateTimeType.DoubleSeconds:
                            result = Convert.ToDouble(value);
                            break;
                    }

                    break;
                default:
                    result = Convert.ToDouble(value);
                    break;
            }

            return result;
        }

        #endregion

        #region Insert

        /// <inheritdoc />
        public override Row Insert(Row row)
        {
            var commandBuilder = new SqlCommandBuilder(Storage);
            CreateInsert(commandBuilder, row, true);
            CreateLastInsertedRowCommand(commandBuilder, row);
            var result = QueryRow(commandBuilder);
            IncreaseSequenceNumber();
            return result;
        }

        #endregion

        #region Replace

        /// <inheritdoc />
        public override void Replace(Row row)
        {
            var commandBuilder = new SqlCommandBuilder(Storage);
            CreateReplace(commandBuilder, row, true);
            Execute(commandBuilder);
            IncreaseSequenceNumber();
        }

        #endregion

        #region Clear

        /// <inheritdoc />
        public override void Clear()
        {
            Storage.Execute(database: Database.Name, table: Name, cmd: "DELETE FROM " + FQTN);
            IncreaseSequenceNumber();
        }

        #endregion

        #region Count

        /// <inheritdoc />
        public override long Count(Search search = default, ResultOption resultOption = default)
        {
            if (search == null)
            {
                search = Search.None;
            }

            if (resultOption == null)
            {
                resultOption = ResultOption.None;
            }

            var s = ToSqlSearch(search);
            if (resultOption != ResultOption.None)
            {
                return SqlCount(s, resultOption);
            }

            var value = QueryValue(new SqlCmd("SELECT COUNT(*) FROM " + FQTN + " WHERE " + s, s.Parameters.ToArray()));
            if (value == null)
            {
                throw new InvalidDataException($"Could not read row count from {FQTN}!");
            }

            return Convert.ToInt64(value);
        }

        #endregion

        #region Exist

        /// <inheritdoc />
        public override bool Exist(Search search)
        {
            if (search == null)
            {
                search = Search.None;
            }

            var s = ToSqlSearch(search);
            var query = "SELECT DISTINCT 1 FROM " + FQTN + " WHERE " + s;
            RowLayout layout = null;
            return Query(new SqlCmd(query, s.Parameters.ToArray()), ref layout).Count > 0;
        }

        /// <inheritdoc />
        public override bool Exist(Row row)
        {
            var search = Search.None;
            var i = 0;
            foreach (var field in Layout.Identifier)
            {
                i++;
                search &= Search.FieldEquals(field.Name, row[field.Index]);
            }

            if (i < 1)
            {
                throw new Exception("At least one identifier field needed!");
            }

            return Exist(search);
        }

        #endregion

        #region GetRows()

        /// <inheritdoc />
        public override IList<Row> GetRows() => Query("SELECT * FROM " + FQTN);

        #endregion

        #region GetRows(Search, ResultOption)

        /// <inheritdoc />
        public override IList<Row> GetRows(Search search = null, ResultOption resultOption = null)
        {
            if (search == null)
            {
                search = Search.None;
            }

            if (resultOption == null)
            {
                resultOption = ResultOption.None;
            }

            var s = ToSqlSearch(search);
            s.CheckFieldsPresent(resultOption);
            return SqlGetRows(s, resultOption);
        }

        #endregion

        #region GetRowAt(index)

        /// <inheritdoc />
        public override Row GetRowAt(int index) => GetRow(Search.None, ResultOption.Limit(1) + ResultOption.Offset(index));

        #endregion

        #region GetRow(Search, ResultOption)

        /// <inheritdoc />
        public override Row GetRow(Search search = null, ResultOption resultOption = null) => GetRows(search, resultOption).Single();

        #endregion

        #region Delete(Row)

        /// <inheritdoc />
        public override void Delete(Row row)
        {
            var commandBuilder = new SqlCommandBuilder(Storage);
            commandBuilder.Append("DELETE FROM ");
            commandBuilder.Append(FQTN);
            AppendWhereClause(commandBuilder, row);
            Storage.Execute(database: Database.Name, table: Name, cmd: commandBuilder);
            IncreaseSequenceNumber();
        }

        #endregion

        #region TryDelete

        /// <inheritdoc />
        public override int TryDelete(Search search)
        {
            var s = ToSqlSearch(search);
            var command = "DELETE FROM " + FQTN + " WHERE " + s;
            var result = Execute(new SqlCmd(command, s.Parameters.ToArray()));
            IncreaseSequenceNumber();
            return result;
        }

        #endregion

        #region Update

        /// <inheritdoc />
        public override void Update(Row row)
        {
            var commandBuilder = new SqlCommandBuilder(Storage);
            CreateUpdate(commandBuilder, row, true);
            Execute(commandBuilder);
            IncreaseSequenceNumber();
        }

        #endregion

        #region QueryRow(SqlCmd cmd, ...)

        /// <summary>Queries for a dataset (selected fields, one row).</summary>
        /// <param name="cmd">The database dependent sql statement.</param>
        /// <param name="layout">The expected schema layout (if unset the layout is returned).</param>
        /// <returns>The result row.</returns>
        public Row QueryRow(SqlCmd cmd, ref RowLayout layout) => Query(cmd, ref layout).Single();

        /// <summary>Queries for a dataset (selected fields, one row).</summary>
        /// <param name="cmd">The database dependent sql statement.</param>
        /// <returns>The result row.</returns>
        public Row QueryRow(SqlCmd cmd)
        {
            var layout = Layout;
            return QueryRow(cmd, ref layout);
        }

        #endregion

        #region QueryValue(SqlCmd cmd, ...

        /// <summary>Querys a single value with a database dependent sql statement.</summary>
        /// <param name="cmd">The database dependent sql statement.</param>
        /// <param name="value">The result.</param>
        /// <param name="fieldName">Name of the field (optional, only needed if multiple columns are returned).</param>
        /// <returns>true if the value could be found and read, false otherwise.</returns>
        /// <typeparam name="TValue">Result value type.</typeparam>
        public bool QueryValue<TValue>(SqlCmd cmd, out TValue value, string fieldName = null)
            where TValue : struct
            => Storage.QueryValue(cmd, out value, Database.Name, Name, fieldName);

        /// <summary>Querys a single value with a database dependent sql statement.</summary>
        /// <param name="cmd">The database dependent sql statement.</param>
        /// <param name="fieldName">Name of the field (optional, only needed if multiple columns are returned).</param>
        /// <returns>The result value or null.</returns>
        public object QueryValue(SqlCmd cmd, string fieldName = null)
            => Storage.QueryValue(cmd, Database.Name, Name, fieldName);

        #endregion

        #region Execute(SqlCmd cmd, ...)

        /// <summary>Executes a database dependent sql statement silently.</summary>
        /// <param name="cmd">the database dependent sql statement.</param>
        /// <returns>Number of affected rows (if supported by the database).</returns>
        public int Execute(SqlCmd cmd) => Storage.Execute(cmd, Database.Name, Name);

        #endregion

        #region Query(SqlCmd, ...)

        /// <summary>Queries for all matching datasets.</summary>
        /// <param name="cmd">The database dependent sql statement.</param>
        /// <param name="layout">The expected schema layout (if unset the layout is returned).</param>
        /// <returns>The result rows.</returns>
        public IList<Row> Query(SqlCmd cmd, ref RowLayout layout) => Storage.Query(cmd, ref layout, Database.Name, Name);

        /// <summary>Queries for all matching datasets.</summary>
        /// <param name="cmd">The database dependent sql statement.</param>
        /// <returns>The result rows.</returns>
        public IList<Row> Query(SqlCmd cmd)
        {
            var layout = Layout;
            return Query(cmd, ref layout);
        }

        #endregion

        #region Commit

        /// <inheritdoc />
        public override int Commit(IEnumerable<Transaction> transactions, TransactionFlags flags = TransactionFlags.Default)
        {
            try
            {
                return InternalCommit(transactions, true);
            }
            catch
            {
                if ((flags & TransactionFlags.ThrowExceptions) != 0)
                {
                    throw;
                }

                return -1;
            }
        }

        #endregion

        #region Maximum

        /// <inheritdoc />
        public override TValue? Maximum<TValue>(string fieldName, Search search = null)
        {
            if (search == null)
            {
                search = Search.None;
            }

            var command = new SqlCommandBuilder(Storage);
            command.Append("SELECT MAX(");
            command.Append(Storage.EscapeFieldName(Layout[fieldName]));
            command.Append(") FROM ");
            command.Append(FQTN);
            command.Append(" WHERE ");
            command.Append(ToSqlSearch(search).ToString());
            var value = Storage.QueryValue(database: Database.Name, table: Name, cmd: command);
            return value == null ? (TValue?) null : (TValue) value;
        }

        #endregion

        #region Minimum

        /// <inheritdoc />
        public override TValue? Minimum<TValue>(string fieldName, Search search = null)
        {
            if (search == null)
            {
                search = Search.None;
            }

            var command = new SqlCommandBuilder(Storage);
            command.Append("SELECT MIN(");
            command.Append(Storage.EscapeFieldName(Layout[fieldName]));
            command.Append(") FROM ");
            command.Append(FQTN);
            command.Append(" WHERE ");
            command.Append(ToSqlSearch(search).ToString());
            var value = Storage.QueryValue(database: Database.Name, table: Name, cmd: command);
            return value == null ? (TValue?) null : (TValue) value;
        }

        #endregion

        #endregion

        #region protected sql92 find functions

        /// <summary>Searches for grouped datasets and returns the id of the first occurence (sql handles this differently).</summary>
        /// <param name="search">Search definition.</param>
        /// <param name="option">Options for the search.</param>
        /// <returns>Returns a list of rows matching the specified criteria.</returns>
        protected internal virtual IList<Row> SqlGetGroupRows(SqlSearch search, ResultOption option)
        {
            RowLayout layout;
            var command = new StringBuilder();
            command.Append("SELECT ");
            if (Storage.SupportsAllFieldsGroupBy)
            {
                layout = Layout;
                command.Append("*");
            }
            else
            {
                layout = null;
                var fieldNumber = 0;
                foreach (var fieldName in search.FieldNames)
                {
                    if (fieldNumber++ > 0)
                    {
                        command.Append(", ");
                    }

                    command.Append(Storage.EscapeFieldName(Layout[fieldName]));
                }
            }

            command.Append(" FROM ");
            command.Append(FQTN);
            command.Append(" WHERE ");
            command.Append(search);
            var groupCount = 0;
            foreach (var o in option.Filter(ResultOptionMode.Group))
            {
                if (groupCount++ == 0)
                {
                    command.Append(" GROUP BY ");
                }
                else
                {
                    command.Append(",");
                }

                command.Append(Storage.EscapeFieldName(Layout[o.Parameter]));
            }

            return Query(new SqlCmd(command.ToString(), search.Parameters.ToArray()), ref layout);
        }

        /// <summary>Searches for grouped datasets and returns the number of items found.</summary>
        /// <param name="search">Search definition.</param>
        /// <param name="option">Options for the search.</param>
        /// <returns>Numer of items found.</returns>
        protected internal virtual long SqlCountGroupBy(SqlSearch search, ResultOption option)
        {
            if (search is null)
            {
                throw new ArgumentNullException(nameof(search));
            }

            if (option is null)
            {
                throw new ArgumentNullException(nameof(option));
            }

            var command = new StringBuilder();
            command.Append("SELECT COUNT(");
            if (Storage.SupportsAllFieldsGroupBy)
            {
                command.Append("*");
            }
            else
            {
                var fieldNumber = 0;
                foreach (var fieldName in search.FieldNames)
                {
                    if (fieldNumber++ > 0)
                    {
                        command.Append(", ");
                    }

                    command.Append(Storage.EscapeFieldName(Layout[fieldName]));
                }
            }

            command.Append(") FROM ");
            command.Append(FQTN);
            command.Append(" WHERE ");
            command.Append(search);
            if (option.Contains(ResultOptionMode.Limit) | option.Contains(ResultOptionMode.Offset))
            {
                throw new InvalidOperationException("Cannot use Option.Group and Option.Limit/Offset at once!");
            }

            var groupCount = 0;
            foreach (var o in option.Filter(ResultOptionMode.Group))
            {
                if (groupCount++ == 0)
                {
                    command.Append(" GROUP BY ");
                }
                else
                {
                    command.Append(",");
                }

                command.Append(Storage.EscapeFieldName(Layout[o.Parameter]));
            }

            var value = QueryValue(new SqlCmd(command.ToString(), search.Parameters.ToArray()));
            if (value == null)
            {
                throw new InvalidDataException($"Could not read value from {FQTN}!");
            }

            return Convert.ToInt64(value);
        }

        /// <summary>Searches the table for rows with given field value combinations.</summary>
        /// <param name="search">The search to run.</param>
        /// <param name="option">Options for the search and the result set.</param>
        /// <returns>Returns number of rows found.</returns>
        protected internal virtual long SqlCount(SqlSearch search, ResultOption option)
        {
            if (search is null)
            {
                throw new ArgumentNullException(nameof(search));
            }

            if (option is null)
            {
                throw new ArgumentNullException(nameof(option));
            }

            if (option.Contains(ResultOptionMode.Group))
            {
                return SqlCountGroupBy(search, option);
            }

            var command = new StringBuilder();
            command.Append("SELECT COUNT(*) FROM ");
            command.Append(FQTN);
            command.Append(" WHERE ");
            command.Append(search);
            foreach (var o in option.ToArray())
            {
                switch (o.Mode)
                {
                    case ResultOptionMode.SortAsc:
                    case ResultOptionMode.SortDesc:
                    case ResultOptionMode.None:
                        break;
                    default:
                        throw new InvalidOperationException($"ResultOptionMode {o.Mode} not supported!");
                }
            }

            var value = Storage.QueryValue(new SqlCmd(command.ToString(), search.Parameters.ToArray()));
            if (value == null)
            {
                throw new InvalidDataException($"Could not read value from {FQTN}!");
            }

            return Convert.ToInt64(value);
        }

        /// <summary>Searches the table for rows with given field value combinations.</summary>
        /// <param name="search">The search to run.</param>
        /// <param name="option">Options for the search and the result set.</param>
        /// <returns>Returns the ID of the row found or -1.</returns>
        protected internal virtual IList<Row> SqlGetRows(SqlSearch search, ResultOption option)
        {
            if (option.Contains(ResultOptionMode.Group))
            {
                return SqlGetGroupRows(search, option);
            }

            var command = new StringBuilder();
            command.Append("SELECT * FROM ");
            command.Append(FQTN);
            command.Append(" WHERE ");
            command.Append(search);
            var orderCount = 0;
            foreach (var o in option.Filter(ResultOptionMode.SortAsc, ResultOptionMode.SortDesc))
            {
                if (orderCount++ == 0)
                {
                    command.Append(" ORDER BY ");
                }
                else
                {
                    command.Append(",");
                }

                command.Append(Storage.EscapeFieldName(Layout[o.Parameter]));
                if (o.Mode == ResultOptionMode.SortAsc)
                {
                    command.Append(" ASC");
                }
                else
                {
                    command.Append(" DESC");
                }
            }

            var limit = 0;
            foreach (var o in option.Filter(ResultOptionMode.Limit))
            {
                if (limit++ > 0)
                {
                    throw new InvalidOperationException("Cannot set two different limits!");
                }

                command.Append(" LIMIT " + o.Parameter);
            }

            var offset = 0;
            foreach (var o in option.Filter(ResultOptionMode.Offset))
            {
                if (offset++ > 0)
                {
                    throw new InvalidOperationException("Cannot set two different offsets!");
                }

                command.Append(" OFFSET " + o.Parameter);
            }

            var layout = Layout;
            return Query(new SqlCmd(command.ToString(), search.Parameters.ToArray()), ref layout);
        }

        /// <summary>Converts the specified search to a <see cref="SqlSearch" />.</summary>
        /// <param name="search">Search definition.</param>
        /// <returns>Returns a new <see cref="SqlSearch" /> instance.</returns>
        protected SqlSearch ToSqlSearch(Search search) => new SqlSearch(Storage, Layout, search);

        #endregion

        #region protected sql92 commands

        /// <summary>Creates the insert command.</summary>
        /// <param name="commandBuilder">The command builder.</param>
        /// <param name="row">The row.</param>
        /// <param name="useParameters">Use database parameters instead of escaped command string.</param>
        protected virtual void CreateInsert(SqlCommandBuilder commandBuilder, Row row, bool useParameters)
        {
            commandBuilder.Append("INSERT INTO ");
            commandBuilder.Append(FQTN);
            commandBuilder.Append(" (");
            var parameterBuilder = new StringBuilder();
            var firstCommand = true;
            if (Layout.FieldCount != row.FieldCount)
            {
                throw new ArgumentException("Invalid fieldcount at row.", nameof(row));
            }

            for (var i = 0; i < Layout.FieldCount; i++)
            {
                var field = Layout[i];
                if (field.Flags.HasFlag(FieldFlags.AutoIncrement))
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

                commandBuilder.Append(Storage.EscapeFieldName(field));
                var value = Storage.GetDatabaseValue(field, row[i]);
                if (value == null)
                {
                    parameterBuilder.Append("NULL");
                }
                else if (!useParameters)
                {
                    parameterBuilder.Append(Storage.EscapeFieldValue(Layout[i], value));
                }
                else
                {
                    var parameter = commandBuilder.CreateParameter(value);
                    parameterBuilder.Append(parameter.Name);
                }
            }

            commandBuilder.Append(") VALUES (");
            commandBuilder.Append(parameterBuilder.ToString());
            commandBuilder.Append(")");
            commandBuilder.AppendLine(";");
        }

        /// <summary>Creates an update command.</summary>
        /// <param name="commandBuilder">The command builder.</param>
        /// <param name="row">The row.</param>
        /// <param name="useParameters">Use database parameters instead of escaped command string.</param>
        protected virtual void CreateUpdate(SqlCommandBuilder commandBuilder, Row row, bool useParameters)
        {
            commandBuilder.Append("UPDATE ");
            commandBuilder.Append(FQTN);
            commandBuilder.Append(" SET ");
            for (var i = 0; i < Layout.FieldCount; i++)
            {
                var field = Layout[i];
                if (field.Flags.HasFlag(FieldFlags.ID))
                {
                    continue;
                }

                if (i > 0)
                {
                    commandBuilder.Append(",");
                }

                commandBuilder.Append(Storage.EscapeFieldName(field));
                var value = row[i];
                if (value == null)
                {
                    commandBuilder.Append("=NULL");
                }
                else
                {
                    commandBuilder.Append("=");
                    value = Storage.GetDatabaseValue(Layout[i], value);
                    if (useParameters)
                    {
                        commandBuilder.CreateAndAddParameter(value);
                    }
                    else
                    {
                        commandBuilder.Append(Storage.EscapeFieldValue(Layout[i], value));
                    }
                }
            }

            AppendWhereClause(commandBuilder, row);
            commandBuilder.AppendLine(";");
        }

        /// <summary>Creates a replace command.</summary>
        /// <param name="cb">The command builder.</param>
        /// <param name="row">The row.</param>
        /// <param name="useParameters">Use database parameters instead of escaped command string.</param>
        protected virtual void CreateReplace(SqlCommandBuilder cb, Row row, bool useParameters)
        {
            cb.Append("REPLACE INTO ");
            cb.Append(FQTN);
            cb.Append(" VALUES (");
            for (var i = 0; i < Layout.FieldCount; i++)
            {
                if (i > 0)
                {
                    cb.Append(",");
                }

                var value = row[i];
                if (value == null)
                {
                    cb.Append("NULL");
                }
                else
                {
                    value = Storage.GetDatabaseValue(Layout[i], value);
                    if (useParameters)
                    {
                        cb.CreateAndAddParameter(value);
                    }
                    else
                    {
                        cb.Append(Storage.EscapeFieldValue(Layout[i], value));
                    }
                }
            }

            cb.AppendLine(");");
        }

        #endregion

        #region private functions

        void AppendWhereClause(SqlCommandBuilder commandBuilder, Row row)
        {
            foreach (var field in Layout.Identifier)
            {
                commandBuilder.Append(" WHERE ");
                commandBuilder.Append(Storage.EscapeFieldName(field));
                commandBuilder.Append("=");
                commandBuilder.Append(Storage.GetDatabaseValue(field, row[field.Index]).ToString());
            }
        }

        int InternalCommit(IEnumerable<Transaction> transactions, bool useParameters)
        {
            var n = 0;
            var complete = false;
            var iterator = transactions.GetEnumerator();
            Task execute = null;
            while (!complete && iterator.MoveNext())
            {
                var commandBuilder = new SqlCommandBuilder(Storage);
                commandBuilder.AppendLine("START TRANSACTION;");
                var i = 0;
                complete = true;
                do
                {
                    var transaction = iterator.Current;
                    switch (transaction.Type)
                    {
                        #region TransactionType.Inserted

                        case TransactionType.Inserted:
                        {
                            CreateInsert(commandBuilder, transaction.Row, useParameters);
                        }
                            break;

                        #endregion

                        #region TransactionType.Replaced

                        case TransactionType.Replaced:
                        {
                            CreateReplace(commandBuilder, transaction.Row, useParameters);
                        }
                            break;

                        #endregion

                        #region TransactionType.Updated

                        case TransactionType.Updated:
                        {
                            CreateUpdate(commandBuilder, transaction.Row, useParameters);
                        }
                            break;

                        #endregion

                        #region TransactionType.Deleted

                        case TransactionType.Deleted:
                        {
                            commandBuilder.Append("DELETE FROM ");
                            commandBuilder.Append(FQTN);
                            AppendWhereClause(commandBuilder, transaction.Row);
                            commandBuilder.AppendLine(";");
                        }
                            break;

                        #endregion

                        default: throw new NotImplementedException();
                    }

                    if (++i >= Storage.TransactionRowCount)
                    {
                        complete = false;
                        break;
                    }
                }
                while (iterator.MoveNext());

                commandBuilder.AppendLine("COMMIT;");
                try
                {
                    if (execute != null)
                    {
                        execute.Wait();
                        IncreaseSequenceNumber();
                        Trace.TraceInformation("{0} transactions committed to {1}.", n, FQTN);
                    }

                    execute = Task.Factory.StartNew(cmd => Execute((SqlCommandBuilder)cmd), commandBuilder);
                    n += i;
                }
                catch (Exception ex)
                {
                    Trace.TraceWarning($"Error committing transactions to table <red>{FQTN}\n{ex}");
                    Trace.TraceInformation("Command: {0}", commandBuilder.Text);
                    throw;
                }
            }

            execute?.Wait();
            Trace.TraceInformation("{0} transactions committed to {1}.", n, FQTN);
            IncreaseSequenceNumber();
            return n;
        }

        #endregion
    }
}
