namespace Cave.Data.SQLite
{
    /// <summary>
    /// Provides access to a subversion entry.
    /// </summary>
    public class SubversionEntry
    {
        string[] m_Data;

        /// <summary>
        /// Obtains the subversion entry version.
        /// </summary>
        public readonly int Version;

        /// <summary>
        /// Obtains whether the entry is valid or not.
        /// </summary>
        public readonly bool IsValid;

        /// <summary>
        /// Creates a new <see cref="SubversionEntry"/> from the specified cdata.
        /// </summary>
        /// <param name="data"></param>
        /// <param name="version"></param>
        public SubversionEntry(string[] data, int version)
        {
            Version = version;
            m_Data = data;
            IsValid = (Version >= 8) || (Version <= 10);
        }

        /// <summary>
        /// Obtains the type of the <see cref="SubversionEntry"/>.
        /// </summary>
        public SubversionEntryType Type
        {
            get
            {
                switch (m_Data[1])
                {
                    case "dir": return SubversionEntryType.Directory;
                    case "file": return SubversionEntryType.File;
                    default: return SubversionEntryType.Unknown;
                }
            }
        }

        /// <summary>
        /// Obtains the name of the <see cref="SubversionEntry"/>.
        /// </summary>
        public string Name => m_Data[0];

        /// <summary>
        /// Checks whether the entry was deleted or not.
        /// </summary>
        public bool Deleted
        {
            get
            {
                switch (Version)
                {
                    case 8:
                    case 9:
                    case 10:
                        return (m_Data[5] == "delete");
                    default:
                        return false;
                }
            }
        }

        /// <summary>
        /// Provides a Name and Type string.
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return Name + " " + Type;
        }

        /// <summary>
        /// Provides a hascode for this instance.
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            return ToString().GetHashCode();
        }
    }
}
