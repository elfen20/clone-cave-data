using System;

namespace Cave.Data
{
    /// <summary>Provides an <see cref="Attribute" /> for configuring datetime fields.</summary>
    [AttributeUsage(AttributeTargets.Field)]
    public sealed class DateTimeFormatAttribute : Attribute
    {
        /// <summary>Initializes a new instance of the <see cref="DateTimeFormatAttribute" /> class.</summary>
        /// <param name="kind"><see cref="DateTimeKind" />.</param>
        /// <param name="type"><see cref="DateTimeType" />.</param>
        public DateTimeFormatAttribute(DateTimeKind kind, DateTimeType type)
        {
            Kind = kind;
            Type = type;
        }

        /// <summary>Gets the <see cref="DateTimeKind" />.</summary>
        public DateTimeKind Kind { get; }

        /// <summary>Gets the <see cref="DateTimeType" />.</summary>
        public DateTimeType Type { get; }
    }
}
