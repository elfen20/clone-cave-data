using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Text;

namespace Cave.Data.Sql
{
    /// <summary>
    /// Provides a sql command builder.
    /// </summary>
    public sealed class SqlCommandBuilder
    {
        readonly SqlStorage storage;
        readonly StringBuilder text = new StringBuilder();
        readonly List<SqlParam> parameters = new List<SqlParam>();

        /// <summary>
        /// Initializes a new instance of the <see cref="SqlCommandBuilder"/> class.
        /// </summary>
        /// <param name="storage">The storage engine.</param>
        public SqlCommandBuilder(SqlStorage storage)
        {
            this.storage = storage ?? throw new ArgumentNullException(nameof(storage));
            Parameters = new ReadOnlyCollection<SqlParam>(parameters);
        }

        /// <summary>
        /// Gets the full command text.
        /// </summary>
        public string Text => text.ToString();

        /// <summary>
        /// Gets all parameters present.
        /// </summary>
        public IList<SqlParam> Parameters { get; }

        /// <summary>
        /// Gets the parameter count.
        /// </summary>
        public int ParameterCount => parameters.Count;

        /// <summary>
        /// Gets the length of the command text.
        /// </summary>
        public int Length => text.Length;

        /// <summary>
        /// Converts to a <see cref="SqlCmd"/> instance.
        /// </summary>
        /// <param name="builder">The builder to convert.</param>
        public static implicit operator SqlCmd(SqlCommandBuilder builder) => new SqlCmd(builder.ToString(), builder.Parameters);

        /// <summary>
        /// Appends a command text.
        /// </summary>
        /// <param name="text">Text to add.</param>
        public void Append(string text)
        {
            this.text.Append(text);
        }

        /// <summary>
        /// Appends a command text.
        /// </summary>
        /// <param name="text">Text to add.</param>
        public void AppendLine(string text)
        {
            this.text.AppendLine(text);
        }

        /// <summary>
        /// Appends a parameter to the parameter list.
        /// </summary>
        /// <param name="databaseValue">The value at the database.</param>
        /// <returns>A new parameter instance.</returns>
        public SqlParam CreateParameter(object databaseValue)
        {
            var name = storage.ParameterPrefix;
            if (storage.SupportsNamedParameters)
            {
                name += parameters.Count;
            }
            var parameter = new SqlParam(name, databaseValue);
            parameters.Add(parameter);
            return parameter;
        }

        /// <summary>
        /// Appends a parameter to the command text and parameter list.
        /// </summary>
        /// <param name="vdatabaseValuelue">The value at the database.</param>
        public void CreateAndAddParameter(object vdatabaseValuelue)
        {
            text.Append(CreateParameter(vdatabaseValuelue).Name);
        }

        /// <summary>
        /// Gets the full command text.
        /// </summary>
        /// <returns>Command text.</returns>
        public override string ToString() => text.ToString();
    }
}
