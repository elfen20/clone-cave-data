using System;
using System.Runtime.Serialization;

namespace Cave.Data
{
    /// <summary>
    /// The table layout is already fixed and can no longer be changed !.
    /// </summary>
    [Serializable]
    public class TableLayoutFixedException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="TableLayoutFixedException"/> class.
        /// </summary>
        public TableLayoutFixedException()
            : base(string.Format("The table layout is already fixed and can no longer be changed!"))
        {
        }

        /// <summary>Initializes a new instance of the <see cref="TableLayoutFixedException"/> class.</summary>
        /// <param name="msg">The message.</param>
        public TableLayoutFixedException(string msg)
            : base(msg)
        {
        }

        /// <summary>Initializes a new instance of the <see cref="TableLayoutFixedException"/> class.</summary>
        /// <param name="msg">The message.</param>
        /// <param name="innerException">The inner exception.</param>
        public TableLayoutFixedException(string msg, Exception innerException)
            : base(msg, innerException)
        {
        }

        /// <summary>Initializes a new instance of the <see cref="TableLayoutFixedException"/> class.</summary>
        /// <param name="info">The information.</param>
        /// <param name="context">The context.</param>
        protected TableLayoutFixedException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }
}
