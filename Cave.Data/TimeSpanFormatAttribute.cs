using System;

namespace Cave
{
    /// <summary>
    /// Provides an <see cref="Attribute"/> for configuring timespan fields.
    /// </summary>
    [AttributeUsage(AttributeTargets.Field, AllowMultiple = false, Inherited = false)]
    public sealed class TimeSpanFormatAttribute : Attribute
    {
        /// <summary>
        /// Gets/sets the <see cref="DateTimeType"/>.
        /// </summary>
        public DateTimeType Type { get; private set; }

        /// <summary>
        /// Creates a new <see cref="TimeSpanFormatAttribute"/>.
        /// </summary>
        /// <param name="type"><see cref="DateTimeType"/>.</param>
        public TimeSpanFormatAttribute(DateTimeType type)
        {
            Type = type;
        }
    }
}
