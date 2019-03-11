using System;

namespace Cave.Data
{
    /// <summary>
    /// Provides a basic implementation of the <see cref="IDatabaseParameter"/> interface.
    /// </summary>
    public class DatabaseParameter : IDatabaseParameter
    {
        object m_Value;
        string m_Name;

        /// <summary>
        /// Creates a new parameter with the specified name and value.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="value"></param>
        public DatabaseParameter(string name, object value)
        {
            if (string.IsNullOrEmpty(name))
            {
                throw new ArgumentNullException("Name");
            }

            m_Name = name.ReplaceInvalidChars(ASCII.Strings.Letters + ASCII.Strings.Digits, "_");
            m_Value = value;
        }

        /// <summary>
        /// Gets/sets the name of the <see cref="DatabaseParameter"/>.
        /// </summary>
        public virtual string Name => m_Name;

        /// <summary>
        /// Gets/sets the value of the <see cref="DatabaseParameter"/>.
        /// </summary>
        public virtual object Value => m_Value;

        /// <summary>
        /// Provides name and value of the parameter.
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            string value = m_Value == null ? "<null>" : m_Value.ToString();
            return Name + " = " + value;
        }

        /// <summary>
        /// Obtains the hascode for this parameter.
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            return ToString().GetHashCode();
        }
    }
}
