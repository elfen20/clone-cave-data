using System;
using System.Collections.Generic;

namespace Cave.Data.Sql
{
    /// <summary>
    /// Provides a class used during custom searches to keep up with all parameters to be
    /// added during sql command generation.
    /// </summary>
    public sealed class SqlSearch
    {
        string m_Text;

        /// <summary>
        /// Creates a new instance for the specified <see cref="SqlStorage"/>.
        /// </summary>
        /// <param name="storage"></param>
        public SqlSearch(SqlStorage storage)
        {
            Storage = storage;
        }

        /// <summary>
        /// Checks whether all fields used at options are present and adds them if not.
        /// </summary>
        /// <param name="option"></param>
        public void CheckFieldsPresent(ResultOption option)
        {
            foreach (string fieldName in option.FieldNames)
            {
                if (!FieldNames.Contains(fieldName))
                {
                    FieldNames.Add(fieldName);
                }
            }
        }

        /// <summary>
        /// provides the <see cref="SqlStorage"/> this search works at.
        /// </summary>
        public SqlStorage Storage { get; }

        /// <summary>
        /// provides the field names.
        /// </summary>
        public List<string> FieldNames { get; } = new List<string>();

        /// <summary>
        /// provides the parameters.
        /// </summary>
        public List<DatabaseParameter> Parameters { get; } = new List<DatabaseParameter>();

        /// <summary>
        /// Adds a new parameter.
        /// </summary>
        /// <param name="field">The fieldproperties (can be set to null if you do not need value conversion).</param>
        /// <param name="paramName">The name of the parameter.</param>
        /// <param name="value">The value of the parameter.</param>
        public void AddParameter(FieldProperties field, string paramName, object value)
        {
            if (field != null)
            {
                if (!FieldNames.Contains(field.Name))
                {
                    FieldNames.Add(field.Name);
                }

                value = Storage.GetDatabaseValue(field, value);
            }
            Parameters.Add(new DatabaseParameter(paramName, value));
        }

        /// <summary>
        /// sets the query text (part of the where clause).
        /// </summary>
        /// <param name="text"></param>
        public void SetText(string text)
        {
            if (m_Text != null)
            {
                throw new InvalidOperationException();
            }

            m_Text = text;
        }

        /// <summary>
        /// Obtains the query text as string.
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return m_Text;
        }
    }
}
