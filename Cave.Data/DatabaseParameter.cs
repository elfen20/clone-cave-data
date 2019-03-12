using System;

namespace Cave.Data
{
    /// <summary>
    /// Provides a basic implementation of the <see cref="IDatabaseParameter"/> interface.
    /// </summary>
    public class DatabaseParameter : IDatabaseParameter
    {
        object value;
        string name;

        /// <summary>
        /// Initializes a new instance of the <see cref="DatabaseParameter"/> class.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="value"></param>
        public DatabaseParameter(string name, object value)
        {
            if (string.IsNullOrEmpty(name))
            {
                throw new ArgumentNullException("Name");
            }

            this.name = name.ReplaceInvalidChars(ASCII.Strings.Letters + ASCII.Strings.Digits, "_");
            this.value = value;
        }

        /// <summary>
        /// Gets/sets the name of the <see cref="DatabaseParameter"/>.
        /// </summary>
        public virtual string Name => name;

        /// <summary>
        /// Gets/sets the value of the <see cref="DatabaseParameter"/>.
        /// </summary>
        public virtual object Value => value;

        /// <summary>
        /// Provides name and value of the parameter.
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            var value = this.value == null ? "<null>" : this.value.ToString();
            return Name + " = " + value;
        }

        /// <summary>
        /// Gets the hascode for this parameter.
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            return ToString().GetHashCode();
        }
    }
}
