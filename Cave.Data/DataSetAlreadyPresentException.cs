using System;
using System.Runtime.Serialization;

namespace Cave.Data
{
    /// <summary>
    /// The dataset ID {0} is already present!.
    /// </summary>
    [Serializable]
    public class DataSetAlreadyPresentException : Exception
    {
        /// <summary>Initializes a new instance of the <see cref="DataSetAlreadyPresentException"/> class.</summary>
        public DataSetAlreadyPresentException()
            : base("The dataset is already present!")
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DataSetAlreadyPresentException"/> class.
        /// </summary>
        public DataSetAlreadyPresentException(long id)
            : base(string.Format("The dataset ID {0} is already present!", id))
        {
        }

        /// <summary>Initializes a new instance of the <see cref="DataSetAlreadyPresentException"/> class.</summary>
        /// <param name="msg">The message.</param>
        public DataSetAlreadyPresentException(string msg)
            : base(msg)
        {
        }

        /// <summary>Initializes a new instance of the <see cref="DataSetAlreadyPresentException"/> class.</summary>
        /// <param name="msg">The message.</param>
        /// <param name="innerException">The inner exception.</param>
        public DataSetAlreadyPresentException(string msg, Exception innerException)
            : base(msg, innerException)
        {
        }

        /// <summary>Initializes a new instance of the <see cref="DataSetAlreadyPresentException"/> class.</summary>
        /// <param name="info">The information.</param>
        /// <param name="context">The context.</param>
        protected DataSetAlreadyPresentException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }
}
