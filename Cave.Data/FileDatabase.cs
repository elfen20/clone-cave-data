using System;
using System.IO;

namespace Cave.Data
{
    /// <summary>
    /// Provides an abstract base class for file databases containing multiple tables
    /// </summary>
    public abstract class FileDatabase : Database, IDisposable
    {
        string m_Folder;

        /// <summary>
        /// Checks whether the instance was already closed
        /// </summary>
        public override bool Closed => m_Folder == null;

        /// <summary>
        /// The path (directory) the database can be found at
        /// </summary>
        public string Folder => m_Folder;

        /// <summary>
        /// Creates a new FileDatabase instance
        /// </summary>
        /// <param name="storage">The storage engine</param>
        /// <param name="directory">The directory the database can be found at</param>
        protected FileDatabase(FileStorage storage, string directory)
            : base(storage, Path.GetFileName(directory))
        {
            if (directory == null)
            {
                throw new ArgumentNullException("Directory");
            }

            m_Folder = directory;
            Directory.CreateDirectory(m_Folder);
        }

        #region IDatabase Member

        /// <summary>
        /// Closes the instance and flushes all cached data
        /// </summary>
        public override void Close()
        {
            m_Folder = null;
        }

        #endregion

        /// <summary>
        /// Obtains the name of the database
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return Name;
        }

        #region IDisposable Support        
        /// <summary>Releases unmanaged and - optionally - managed resources.</summary>
        /// <param name="disposing"><c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources.</param>
        protected virtual void Dispose(bool disposing)
        {
            Close();
        }

        /// <summary>Releases unmanaged and - optionally - managed resources.</summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        #endregion
    }
}
