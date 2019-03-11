using System;
using System.IO;

namespace Cave.Data.SQLite
{
    /// <summary>
    /// Provides an interface to subversioned files and directories (reads .svn/entries).
    /// </summary>
    public static class Subversion
    {
        /// <summary>
        /// Obtains the root .svn path of the repository.
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
        /// Obtains the subversion version this repository was written by.
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
        /// Obtains the revision of a specified directory.
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
