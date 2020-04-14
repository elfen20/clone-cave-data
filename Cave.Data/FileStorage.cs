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
        #region constructors

        /// <summary>
        /// Initializes a new instance of the <see cref="FileStorage"/> class.
        /// <para>
        /// Following formats are supported:<br />
        /// file://server/relativepath<br />
        /// file:absolutepath.<br /></para>
        /// </summary>
        /// <param name="connectionString">ConnectionString of the storage.</param>
        /// <param name="options">The options.</param>
        protected FileStorage(ConnectionString connectionString, ConnectionFlags options)
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

        /// <inheritdoc/>
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
        /// Gets the base path used for the file storage.
        /// </summary>
        public string Folder { get; private set; }

        #region IStorage functions

        /// <inheritdoc/>
        public override void Close()
        {
            Folder = null;
            base.Close();
        }

        /// <inheritdoc/>
        public override bool HasDatabase(string database)
        {
            if (Closed)
            {
                throw new ObjectDisposedException(ToString());
            }

            return Directory.Exists(Path.Combine(Folder, database));
        }

        /// <inheritdoc/>
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

        /// <inheritdoc/>
        public override void DeleteDatabase(string database)
        {
            if (Closed)
            {
                throw new ObjectDisposedException(ToString());
            }

            Directory.Delete(Path.Combine(Folder, database), true);
        }

        #endregion

        /// <inheritdoc/>
        public override string ToString() => $"file://{Folder}";

        #region IDisposable Member

        /// <inheritdoc/>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>Releases unmanaged and - optionally - managed resources.</summary>
        /// <param name="disposing"><c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources.</param>
        protected virtual void Dispose(bool disposing)
        {
            Close();
        }
        #endregion
    }
}
