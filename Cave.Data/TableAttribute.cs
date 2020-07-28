using System;

namespace Cave.Data
{
    /// <summary>Provides a table <see cref="Attribute" /> for table settings at database structs.</summary>
    [AttributeUsage(AttributeTargets.Struct)]
    public sealed class TableAttribute : Attribute
    {
        /// <summary>Initializes a new instance of the <see cref="TableAttribute" /> class.</summary>
        public TableAttribute() { }

        /// <summary>Initializes a new instance of the <see cref="TableAttribute" /> class.</summary>
        /// <param name="name">Name for the table.</param>
        public TableAttribute(string name) => Name = name;

        /// <summary>Gets or sets the field name at the database.</summary>
        public string Name { get; set; }

        /// <summary>Gets or sets the <see cref="NamingStrategy" /> used for this table (name and fields).</summary>
        public NamingStrategy NamingStrategy { get; set; }

        /// <summary>Gets the name set within a TableAttribute for the specified type.</summary>
        /// <param name="type">Type to search for attributes.</param>
        /// <returns>A <see cref="TableAttribute" /> or null.</returns>
        internal static TableAttribute Get(Type type)
        {
            if (type == null)
            {
                throw new ArgumentNullException(nameof(type));
            }

            foreach (Attribute attribute in type.GetCustomAttributes(false))
            {
                if (attribute is TableAttribute result)
                {
                    return result;
                }
            }

            return new TableAttribute { Name = type.Name };
        }
    }
}
