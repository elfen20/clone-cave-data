using System;

namespace Cave.Data
{
    /// <summary>
    /// Provides a table <see cref="Attribute"/> for table settings at database structs
    /// </summary>

    [AttributeUsage(AttributeTargets.Struct, Inherited = false)]
    public sealed class TableAttribute : Attribute
    {
        /// <summary>Gets the name set within a TableAttribute for the specified type.</summary>
        /// <param name="type">Type to search for attributes.</param>
        /// <returns></returns>
        /// <exception cref="Exception">Type {0} does not define a TableAttribute!</exception>
        public static string GetName(Type type)
        {
            if (type == null)
            {
                throw new ArgumentNullException("type");
            }

            foreach (Attribute attribute in type.GetCustomAttributes(false))
            {
                TableAttribute tableAttribute = attribute as TableAttribute;
                if (tableAttribute != null)
                {
                    return tableAttribute.Name;
                }
            }
            throw new ArgumentException(string.Format("Type {0} does not define a TableAttribute!", type));
        }

        /// <summary>
        /// Gets/sets the "real" field name (at the database)
        /// </summary>
        public string Name { get; private set; }

        /// <summary>
        /// Creates a new <see cref="TableAttribute"/>
        /// </summary>
        public TableAttribute()
        {
        }

        /// <summary>
        /// Creates a new <see cref="TableAttribute"/> with the specified name
        /// </summary>
        /// <param name="name">Name for the table</param>
        public TableAttribute(string name)
        {
            Name = name;
        }
    }
}
