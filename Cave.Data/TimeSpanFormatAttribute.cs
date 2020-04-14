using System;

namespace Cave.Data
{
    /// <summary>
    /// Provides an <see cref="Attribute"/> for configuring timespan fields.
    /// </summary>
    [AttributeUsage(AttributeTargets.Field, AllowMultiple = false, Inherited = false)]
    public sealed class TimeSpanFormatAttribute : Attribute
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="TimeSpanFormatAttribute"/> class.
        /// </summary>
        /// <param name="type"><see cref="DateTimeType"/>.</param>
        public TimeSpanFormatAttribute(DateTimeType type)
        {
            Type = type;
        }

        /// <summary>
        /// Gets the <see cref="DateTimeType"/>.
        /// </summary>
        public DateTimeType Type { get; private set; }
    }
}
