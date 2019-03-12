using System;
using System.Reflection;

namespace Cave
{
    /// <summary>
    /// Provides public access the Cave System Data Assembly instance.
    /// </summary>
    public static class CaveSystemData
    {
        /// <summary>
        /// Provides the date time format for big int date time values.
        /// </summary>
        public const string BigIntDateTimeFormat = "yyyyMMddHHmmssfff";

        static int currentTransactionRowCount = 5000;

        /// <summary>Gets the type.</summary>
        /// <value>The type.</value>
        public static Type Type => typeof(CaveSystemData);

        /// <summary>
        /// Gets the assembly.
        /// </summary>
        public static Assembly Assembly => Type.Assembly;

        /// <summary>
        /// Gets the <see cref="AssemblyVersionInfo"/> for the <see cref="Assembly"/>.
        /// </summary>
        public static AssemblyVersionInfo VersionInfo => AssemblyVersionInfo.FromAssembly(Assembly);

        /// <summary>
        /// Number of rows per chunk on big data operations.
        /// </summary>
        public static int TransactionRowCount
        {
            get => currentTransactionRowCount;
            set => currentTransactionRowCount = Math.Max(1, value);
        }

        /// <summary>
        /// Calculates a database id based on crc64.
        /// You can use this for ID fields based on a unique name field.
        /// </summary>
        /// <param name="text"></param>
        /// <returns></returns>
        public static long CalculateID(string text)
        {
            if (string.IsNullOrEmpty(text))
            {
                throw new ArgumentNullException(nameof(text));
            }

            return text.GetHashCode() + 1L - int.MinValue;
        }
    }
}
