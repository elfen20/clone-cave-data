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

using Cave.Data.Sql;
using Cave.Data.SQLite;
using Cave.IO;
using System;
using System.IO;

namespace Cave.Data.SQLite
{
    /// <summary>
    /// Provides an interface to subversioned files and directories (reads .svn/entries)
    /// </summary>
    public static class Subversion
    {
        /// <summary>
        /// Obtains the root .svn path of the repository
        /// </summary>
        /// <param name="path"></param>
        /// <returns></returns>
        static string GetRootPath(string path)
        {
            path = Path.GetFullPath(path);
            while (true)
            {
                string result = Path.Combine(path, ".svn");
                if (Directory.Exists(result))
                {
                    return result;
                }

                path = Path.GetDirectoryName(path);
            }
            throw new Exception(string.Format("Could not find .svn directory!"));
        }

        #region handle versioned entries

        static string[] ReadEntries(string path)
        {
            switch (GetVersion(path))
            {
                case 8:
                case 9:
                case 10:
                    return File.ReadAllLines(Path.Combine(GetRootPath(path), "entries"));
            }
            throw new NotSupportedException();
        }

        #endregion

        /// <summary>
        /// Obtains the subversion version this repository was written by
        /// </summary>
        /// <param name="path"></param>
        /// <returns></returns>
        public static int GetVersion(string path)
        {
            string svnRoot = GetRootPath(path);
            string entriesFile = Path.Combine(svnRoot, "entries");
            if (File.Exists(entriesFile))
            {
                string[] svnEntries = File.ReadAllLines(entriesFile);
                switch (svnEntries[0])
                {
                    case "8":
                    case "9":
                    case "10":
                    case "12":
                        return int.Parse(svnEntries[0]);
                    default:
                        throw new InvalidDataException(string.Format("Unknown svn version {0}!", svnEntries[0]));
                }
            }
            else
            {
                string databaseFile = Path.Combine(svnRoot, "wc.db");
                if (File.Exists(databaseFile))
                {
                    return 100;
                }

                throw new DirectoryNotFoundException(string.Format("Could not find .svn directory!"));
            }
        }

        /// <summary>
        /// Obtains the revision of a specified directory
        /// </summary>
        /// <param name="path"></param>
        /// <returns></returns>
        public static int GetRevision(string path)
        {
            string svnRoot = GetRootPath(path);

            if (GetVersion(path) > 10)
            {
                using (SQLiteStorage storage = new SQLiteStorage(@"file:///" + svnRoot, DbConnectionOptions.None))
                {
                    long l_Revision = (long)storage.QueryValue("wc", "nodes", "SELECT MAX(revision) FROM " + storage.FQTN("wc", "nodes"));
                    return (int)l_Revision;
                }
            }

            if (!Directory.Exists(svnRoot))
            {
                throw new DirectoryNotFoundException();
            }

            try
            {
                string[] svnEntries = ReadEntries(svnRoot);
                return int.Parse(svnEntries[3]);
            }
            catch { throw new InvalidDataException("Cannot determine svn revision!"); }
        }
    }
}
