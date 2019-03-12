using System.Collections.Generic;
using System.Data;
using System.Text;

namespace Cave.Data.Sql
{
    /// <summary>
    /// Provides a sql command builder.
    /// </summary>
    public sealed class SqlCommandBuilder
    {
        SqlStorage storage;
        SqlDatabase database;
        StringBuilder text = new StringBuilder();
        List<DatabaseParameter> parameters = new List<DatabaseParameter>();

        /// <summary>Creates a new sql command builder instance.</summary>
        /// <param name="database">The database.</param>
        public SqlCommandBuilder(IDatabase database)
        {
            this.database = (SqlDatabase)database;
            this.storage = (SqlStorage)database.Storage;
        }

        /// <summary>
        /// Appends a command text.
        /// </summary>
        /// <param name="text"></param>
        public void Append(string text)
        {
            this.text.Append(text);
        }

        /// <summary>
        /// Appends a command text.
        /// </summary>
        /// <param name="text"></param>
        public void AppendLine(string text)
        {
            this.text.AppendLine(text);
        }

        /// <summary>Appends a parameter to the command text and parameter list.</summary>
        /// <param name="para">The parameter.</param>
        public void AddParameter(DatabaseParameter para)
        {
            parameters.Add(para);
        }

        /// <summary>
        /// Appends a parameter to the command text and parameter list.
        /// </summary>
        /// <param name="value">The value of the parameter.</param>
        /// <returns>Returns the name of the created parameter.</returns>
        public void CreateAndAddParameter(object value)
        {
            var name = (parameters.Count + 1).ToString();
            parameters.Add(new DatabaseParameter(name, value));
            text.Append(storage.ParameterPrefix);
            if (storage.SupportsNamedParameters)
            {
                text.Append(name);
            }
        }

        /// <summary>
        /// Gets the full command text.
        /// </summary>
        public string Text => text.ToString();

        /// <summary>
        /// Gets all parameters present.
        /// </summary>
        public DatabaseParameter[] Parameters => parameters.ToArray();

        /// <summary>
        /// Gets the length of the command text.
        /// </summary>
        public int Length => text.Length;

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2100:SQL-Abfragen auf Sicherheitsrisiken überprüfen")]
        internal void Execute()
        {
            SqlConnection sqlConnection = storage.GetConnection(database.Name);
            var error = false;
            try
            {
                using (IDbCommand dbCommand = sqlConnection.CreateCommand())
                {
                    dbCommand.CommandText = Text;
                    if (storage.LogVerboseMessages)
                    {
                        storage.LogQuery(dbCommand);
                    }

                    foreach (DatabaseParameter parameter in parameters)
                    {
                        IDbDataParameter dataParameter = dbCommand.CreateParameter();
                        if (storage.SupportsNamedParameters)
                        {
                            dataParameter.ParameterName = storage.ParameterPrefix + parameter.Name;
                        }

                        dataParameter.Value = parameter.Value;
                        dbCommand.Parameters.Add(dataParameter);
                    }
                    dbCommand.ExecuteNonQuery();
                    dbCommand.Parameters.Clear();
                    dbCommand.CommandText = string.Empty;
                }
            }
            catch
            {
                error = true;
                throw;
            }
            finally
            {
                storage.ReturnConnection(ref sqlConnection, error);
            }
        }
    }
}
