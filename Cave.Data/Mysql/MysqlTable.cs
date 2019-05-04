using System;
using System.Collections.Generic;
using System.Text;
using Cave.Data.Sql;

namespace Cave.Data.Mysql
{
    /// <summary>
    /// Provides a mysql table implementation.
    /// </summary>
    public class MySqlTable : SqlTable
    {
        #region MySql specific overrides

        /// <summary>
        /// This function does a lookup on the ids of the table and returns the row with the n-th ID where n is the specified index.
        /// Note that indices may change on each update, insert, delete and sorting is not garanteed!.
        /// <param name="index">The index of the row to be fetched</param>
        /// </summary>
        /// <returns>Returns the row.</returns>
        public override Row GetRowAt(int index)
        {
            var id = (long)SqlStorage.QueryValue(Database.Name, Name, "SELECT ID FROM " + FQTN + " ORDER BY ID LIMIT " + index + ",1");
            return GetRow(id);
        }

        /// <summary>
        /// Gets the command to retrieve the last inserted row.
        /// </summary>
        /// <param name="row">The row to be inserted.</param>
        /// <returns></returns>
        protected override string GetLastInsertedIDCommand(Row row)
        {
            return "SELECT LAST_INSERT_ID();";
        }

        /// <summary>
        /// Inserts a row to the table. If an ID <![CDATA[<]]> 0 is specified an automatically generated ID will be used to add the dataset.
        /// </summary>
        /// <param name="row">The row to insert.</param>
        /// <returns>Returns the ID of the inserted dataset.</returns>
        public override long Insert(Row row)
        {
            var commandBuilder = new StringBuilder();
            commandBuilder.Append("INSERT ");
            if (Storage.UseDelayedWrites)
            {
                commandBuilder.Append("DELAYED ");
            }

            commandBuilder.Append("INTO ");
            commandBuilder.Append(FQTN);
            commandBuilder.Append(" (");
            var parameterBuilder = new StringBuilder();
            var parameters = new List<DatabaseParameter>(FieldCount);
            var firstCommand = true;
            var autoSetID = false;
            var autoIncrementID = false;

            // autoset id ?
            var id = Layout.GetID(row);
            if (id <= 0)
            {
                autoSetID = true;

                // yes, autoinc ?
                autoIncrementID = (Layout.IDField.Flags & FieldFlags.AutoIncrement) != 0;
            }

            // prepare ID field
            if (autoSetID && !autoIncrementID)
            {
                commandBuilder.Append(SqlStorage.EscapeFieldName(Layout.IDField));
                id = GetNextFreeID();
                parameterBuilder.Append(id);
                firstCommand = false;
            }

            for (var i = 0; i < FieldCount; i++)
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

                var value = SqlStorage.GetDatabaseValue(fieldProperties, row.GetValue(i));
                if (value == null)
                {
                    parameterBuilder.Append("NULL");
                }
                else
                {
                    var parameter = new DatabaseParameter(fieldProperties.NameAtDatabase, value);
                    parameters.Add(parameter);
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
            if (autoIncrementID)
            {
                commandBuilder.Append(GetLastInsertedIDCommand(row));
                return Convert.ToInt64(SqlStorage.QueryValue(Database.Name, Name, commandBuilder.ToString(), parameters.ToArray()));
            }
            else
            {
                SqlStorage.Execute(Database.Name, Name, commandBuilder.ToString(), parameters.ToArray());
                return id;
            }
        }

        string[] MysqlInternalCommand(string cmd)
        {
            var results = new List<string>();
            List<Row> rows = SqlStorage.Query(null, Database.Name, Name, cmd);
            foreach (Row row in rows)
            {
                var i = Layout.GetFieldIndex("Msg_text");
                var text = row.GetValue(i).ToString();
                i = Layout.GetFieldIndex("Msg_type");
                var type = row.GetValue(i).ToString();
                results.Add($"{type} {text}");
            }
            return results.ToArray();
        }

        /// <summary>Runs the repair table command.</summary>
        /// <returns></returns>
        public string[] Repair()
        {
            return MysqlInternalCommand("REPAIR TABLE " + FQTN + " EXTENDED");
        }

        /// <summary>Runs the optimize table command.</summary>
        /// <returns></returns>
        public string[] Optimize()
        {
            return MysqlInternalCommand("OPTIMIZE TABLE " + FQTN);
        }

        #endregion

        /// <summary>
        /// Initializes a new instance of the <see cref="MySqlTable"/> class.
        /// </summary>
        /// <param name="database">The database the table belongs to.</param>
        /// <param name="layout">Layout of the table.</param>
        public MySqlTable(MySqlDatabase database, RowLayout layout)
            : base(database, layout)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MySqlTable"/> class.
        /// </summary>
        /// <param name="database">The database the table belongs to.</param>
        /// <param name="table">Name of the table.</param>
        public MySqlTable(MySqlDatabase database, string table)
            : base(database, table)
        {
        }
    }
}
