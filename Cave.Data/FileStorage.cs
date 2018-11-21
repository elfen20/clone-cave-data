#region CopyRight 2018
/*
    Copyright (c) 2005-2018 Andreas Rohleder (andreas@rohleder.cc)
    All rights reserved
*/
#endregion
#region License LGPL-3
/*
    This program/library/sourcecode is free software; you can redistribute it
    and/or modify it under the terms of the GNU Lesser General Public License
    version 3 as published by the Free Software Foundation subsequent called
    the License.

    You may not use this program/library/sourcecode except in compliance
    with the License. The License is included in the LICENSE file
    found at the installation directory or the distribution package.

    Permission is hereby granted, free of charge, to any person obtaining
    a copy of this software and associated documentation files (the
    "Software"), to deal in the Software without restriction, including
    without limitation the rights to use, copy, modify, merge, publish,
    distribute, sublicense, and/or sell copies of the Software, and to
    permit persons to whom the Software is furnished to do so, subject to
    the following conditions:

    The above copyright notice and this permission notice shall be included
    in all copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
    EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
    MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
    NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
    LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
    OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
    WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/
#endregion License
#region Authors & Contributors
/*
   Author:
     Andreas Rohleder <andreas@rohleder.cc>

   Contributors:
 */
#endregion Authors & Contributors

using System;
using System.Collections.Generic;
using System.IO;
using Cave.IO;
using Cave.Text;

namespace Cave.Data
{
	/// <summary>
	/// Provides an abstract base class for file storage containing multiple databases
	/// </summary>
	public abstract class FileStorage : Storage, IDisposable
    {
        string m_Folder;

        /// <summary>
        /// Obtains the base path used for the file storage
        /// </summary>
        public string Folder { get { return m_Folder; } }

		#region constructors

		/// <summary>
		/// Opens a file storage.
		/// <para>
		/// Following formats are supported:<br />
		/// file://server/relativepath<br />
		/// file:absolutepath<br /></para>
		/// </summary>
		/// <param name="connectionString">ConnectionString of the storage</param>
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
            m_Folder = Path.GetFullPath(Path.GetDirectoryName(connectionString.Location));
            if (!Directory.Exists(m_Folder.ToString()))
            {
                try
                {
					Directory.CreateDirectory(m_Folder.ToString());
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
        /// closes the connection to the storage engine
        /// </summary>
        /// <exception cref="ObjectDisposedException"></exception>
        public override void Close()
        {
            m_Folder = null;
            base.Close();
        }

        /// <summary>
        /// Checks whether the database with the specified name exists at the database or not
        /// </summary>
        /// <param name="database">The name of the database</param>
        /// <returns></returns>
        public override bool HasDatabase(string database)
        {
            if (Closed)
            {
                throw new ObjectDisposedException(ToString());
            }

            return (Directory.Exists(Path.Combine(m_Folder, database)));
        }

        /// <summary>
        /// Obtains all available database names
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

                List<string> result = new List<string>();
                foreach (string directory in Directory.GetDirectories(m_Folder.ToString(), "*", SearchOption.TopDirectoryOnly))
                {
                    result.Add(Path.GetFileName(directory));
                }
                return result.ToArray();
            }
        }

        /// <summary>
        /// Adds a new database with the specified name
        /// </summary>
        /// <param name="database">The name of the database</param>
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
				Directory.CreateDirectory((m_Folder + database).ToString());
                return GetDatabase(database);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(string.Format("The database {0} cannot be created!", database), ex);
            }
        }

        /// <summary>
        /// Removes the specified database
        /// </summary>
        /// <param name="database"></param>
        /// <exception cref="ObjectDisposedException"></exception>
        public override void DeleteDatabase(string database)
        {
            if (Closed)
            {
                throw new ObjectDisposedException(ToString());
            }

            Directory.Delete(Path.Combine(m_Folder, database), true);
        }

        #endregion

        /// <summary>
        /// Obtains "FileStorage[Path]"
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return "FileStorage[" + m_Folder.ToString() + "]";
        }

        #region IDisposable Member
        /// <summary>
        /// Frees all used resources
        /// </summary>
        /// <param name="disposing"></param>
        protected virtual void Dispose(bool disposing)
        {
            Close();
        }

        /// <summary>
        /// Frees all used resources
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        #endregion
    }
}
