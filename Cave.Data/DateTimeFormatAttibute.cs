using System;

namespace Cave
{
    /// <summary>
    /// Provides an <see cref="Attribute"/> for configuring datetime fields
    /// </summary>
    [AttributeUsage(AttributeTargets.Field, Inherited = false)]
    public sealed class DateTimeFormatAttribute : Attribute
    {
        /// <summary>
        /// Gets/sets the <see cref="DateTimeKind"/>
        /// </summary>
        public DateTimeKind Kind { get; private set; }

        /// <summary>
        /// Gets/sets the <see cref="DateTimeType"/>
        /// </summary>
        public DateTimeType Type { get; private set; }

        /// <summary>
        /// Creates a new <see cref="DateTimeFormatAttribute"/>
        /// </summary>
        /// <param name="kind"><see cref="DateTimeKind"/></param>
        /// <param name="type"><see cref="DateTimeType"/></param>
        public DateTimeFormatAttribute(DateTimeKind kind, DateTimeType type)
        {
            Kind = kind;
            Type = type;
        }
    }
}
