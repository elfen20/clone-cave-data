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
        /// Gets the subversion version this repository was written by.
        /// </summary>
        /// <param name="path">Path to check.</param>
        /// <returns>The version code.</returns>
        public static int GetVersion(string path)
        {
            var svnRoot = GetRootPath(path);
            var entriesFile = Path.Combine(svnRoot, "entries");
            if (File.Exists(entriesFile))
            {
                var svnEntries = File.ReadAllLines(entriesFile);
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
                var databaseFile = Path.Combine(svnRoot, "wc.db");
                if (File.Exists(databaseFile))
                {
                    return 100;
                }

                throw new DirectoryNotFoundException(string.Format("Could not find .svn directory!"));
            }
        }

        /// <summary>
        /// Gets the revision of a specified directory.
        /// </summary>
        /// <param name="path">The path to check.</param>
        /// <returns>The revision number.</returns>
        public static int GetRevision(string path)
        {
            var svnRoot = GetRootPath(path);

            if (GetVersion(path) > 10)
            {
                using (var storage = new SQLiteStorage(@"file:///" + svnRoot))
                {
                    var revision = (long)storage.QueryValue(database: "wc", table: "nodes", cmd: "SELECT MAX(revision) FROM " + storage.FQTN("wc", "nodes"));
                    return (int)revision;
                }
            }

            if (!Directory.Exists(svnRoot))
            {
                throw new DirectoryNotFoundException();
            }

            try
            {
                var svnEntries = ReadEntries(svnRoot);
                return int.Parse(svnEntries[3]);
            }
            catch
            {
                throw new InvalidDataException("Cannot determine svn revision!");
            }
        }

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

        /// <summary>
        /// Gets the root .svn path of the repository.
        /// </summary>
        /// <param name="path">The current path.</param>
        /// <returns>The root path.</returns>
        static string GetRootPath(string path)
        {
            path = Path.GetFullPath(path);
            while (true)
            {
                var result = Path.Combine(path, ".svn");
                if (Directory.Exists(result))
                {
                    return result;
                }

                path = Path.GetDirectoryName(path);
            }
            throw new Exception(string.Format("Could not find .svn directory!"));
        }
    }
}
