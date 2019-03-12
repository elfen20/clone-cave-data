using System;
using System.Collections.Generic;
using System.IO;

namespace Cave.Data
{
    /// <summary>
    /// Provides an abstract base class for file storage containing multiple databases.
    /// </summary>
    public abstract class FileStorage : Storage, IDisposable
    {
        /// <summary>
        /// Gets the base path used for the file storage.
        /// </summary>
        public string Folder { get; private set; }

        #region constructors

        /// <summary>
        /// Opens a file storage.
        /// <para>
        /// Following formats are supported:<br />
        /// file://server/relativepath<br />
        /// file:absolutepath.<br /></para>
        /// </summary>
        /// <param name="connectionString">ConnectionString of the storage.</param>
        /// <param name="options">The options.</param>
        /// <exception cref="NotSupportedException"></exception>
        /// <exception cref="DirectoryNotFoundException"></exception>
        protected FileStorage(ConnectionString connectionString, DbConnectionOptions options)
            : base(connectionString, options)
        {
            if (string.IsNullOrEmpty(connectionString.Server))
            {
                connectionString.Server = "localhost";
            }

            if (connectionString.Server != "localhost" && connectionString.Server != ".")
            {
                throw new NotSupportedException(string.Format("Remote access via server setting is not supported atm.! (use localhost or .)"));
            }
            if (string.IsNullOrEmpty(connectionString.Location) || !connectionString.Location.Contains("/"))
            {
                connectionString.Location = $"./{connectionString.Location}";
            }
            Folder = Path.GetFullPath(Path.GetDirectoryName(connectionString.Location));
            if (!Directory.Exists(Folder.ToString()))
            {
                try
                {
                    Directory.CreateDirectory(Folder.ToString());
                }
                catch (Exception ex)
                {
                    throw new DirectoryNotFoundException(string.Format("The directory '{0}' cannot be found or created!", connectionString.Location), ex);
                }
            }
        }

        #endregion

        #region IStorage Member

        /// <summary>
        /// closes the connection to the storage engine.
        /// </summary>
        /// <exception cref="ObjectDisposedException"></exception>
        public override void Close()
        {
            Folder = null;
            base.Close();
        }

        /// <summary>
        /// Checks whether the database with the specified name exists at the database or not.
        /// </summary>
        /// <param name="database">The name of the database.</param>
        /// <returns></returns>
        public override bool HasDatabase(string database)
        {
            if (Closed)
            {
                throw new ObjectDisposedException(ToString());
            }

            return Directory.Exists(Path.Combine(Folder, database));
        }

        /// <summary>
        /// Gets all available database names.
        /// </summary>
        /// <exception cref="ObjectDisposedException"></exception>
        public override string[] DatabaseNames
        {
            get
            {
                if (Closed)
                {
                    throw new ObjectDisposedException(ToString());
                }

                var result = new List<string>();
                foreach (var directory in Directory.GetDirectories(Folder.ToString(), "*", SearchOption.TopDirectoryOnly))
                {
                    result.Add(Path.GetFileName(directory));
                }
                return result.ToArray();
            }
        }

        /// <summary>
        /// Adds a new database with the specified name.
        /// </summary>
        /// <param name="database">The name of the database.</param>
        /// <returns></returns>
        /// <exception cref="ObjectDisposedException"></exception>
        public override IDatabase CreateDatabase(string database)
        {
            if (Closed)
            {
                throw new ObjectDisposedException(ToString());
            }

            try
            {
                Directory.CreateDirectory((Folder + database).ToString());
                return GetDatabase(database);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(string.Format("The database {0} cannot be created!", database), ex);
            }
        }

        /// <summary>
        /// Removes the specified database.
        /// </summary>
        /// <param name="database"></param>
        /// <exception cref="ObjectDisposedException"></exception>
        public override void DeleteDatabase(string database)
        {
            if (Closed)
            {
                throw new ObjectDisposedException(ToString());
            }

            Directory.Delete(Path.Combine(Folder, database), true);
        }

        #endregion

        /// <summary>
        /// Gets "FileStorage[Path]".
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return "FileStorage[" + Folder.ToString() + "]";
        }

        #region IDisposable Member

        /// <summary>
        /// Frees all used resources.
        /// </summary>
        /// <param name="disposing"></param>
        protected virtual void Dispose(bool disposing)
        {
            Close();
        }

        /// <summary>
        /// Frees all used resources.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        #endregion
    }
}
