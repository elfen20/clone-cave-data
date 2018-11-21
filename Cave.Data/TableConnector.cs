#region CopyRight 2018
/*
    Copyright (c) 2003-2018 Andreas Rohleder (andreas@rohleder.cc)
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
#endregion
#region Authors & Contributors
/*
   Author:
     Andreas Rohleder <andreas@rohleder.cc>

   Contributors:
 */
#endregion

using Cave.IO;
using Cave.Text;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;

namespace Cave.Data
{
    /// <summary>
    /// Provides an abstract base class for table connector implementations.
    /// </summary>
    /// <seealso cref="ITableConnector" />
    public abstract class TableConnector : ITableConnector
    {
        Dictionary<IMemoryTable, int> m_Tables;

        /// <summary>Connects to the given table.</summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="table">The table.</param>
        /// <param name="name">Name of the table.</param>
        /// <param name="flags">The flags.</param>
        /// <exception cref="ArgumentNullException">Database</exception>
        /// <exception cref="NotSupportedException"></exception>
        protected void ConnectTable<T>(ref ITable<T> table, string name = null, TableFlags flags = TableFlags.AllowCreate) where T : struct
        {
            if (Database == null)
            {
                table = new SynchronizedMemoryTable<T>();
                return;
            }
			table = Database.GetTable<T>(flags, name);
            Trace.TraceInformation("Loaded {0} from {1}", table, Database);
			switch (Mode)
            {
                case TableConnectorMode.BackgroundWriter: table = new BackgroundWriterTable<T>(table); break;
                case TableConnectorMode.ReadCache: table = new ReadCachedTable<T>(table); break;
                case TableConnectorMode.Direct: break;
                case TableConnectorMode.Memory:
                    m_Tables = new Dictionary<IMemoryTable, int>();
                    lock (m_Tables)
                    {
                        IMemoryTable<T> memTable = new SynchronizedMemoryTable<T>();
                        memTable.LoadTable(table);
                        table = memTable;
                        m_Tables[memTable] = memTable.SequenceNumber;
                    }
                    break;
                default: throw new NotSupportedException(string.Format("Mode {0} not supported!", Mode));
            }
        }

        /// <summary>Gets the name of the log source.</summary>
        /// <value>The name of the log source.</value>
        public abstract string LogSourceName { get; }

        /// <summary>Gets the database connection used.</summary>
        /// <value>The database.</value>
        public IDatabase Database { get; private set; }

		/// <summary>Gets the database folder used at load.</summary>
		/// <value>The folder.</value>
		public string Folder { get; private set; }

		/// <summary>Gets the current mode of the connector.</summary>
		/// <value>The <see cref="TableConnectorMode" />.</value>
		public virtual TableConnectorMode Mode { get; private set; }

        /// <summary>Gets all tables of this instance.</summary>
        /// <value>The tables.</value>
        public abstract ITable[] Tables { get; }

        /// <summary>Loads all dat files at the specified path into memory tables.</summary>
        /// <param name="path">The path.</param>
        public bool Load(string path)
        {
			bool result = true;
			Folder = path;
            m_Tables = new Dictionary<IMemoryTable, int>();
            lock (m_Tables)
            {
                Mode = TableConnectorMode.Memory;
                foreach (IMemoryTable table in Tables)
                {
                    Trace.TraceInformation("Loading {0}", table);
                    string file = Path.Combine(path, table.Name + ".dat");
					m_Tables[table] = 0;
					if (File.Exists(file))
					{
						try
						{
							table.Clear();
							DatReader.ReadTable(table, file);
							m_Tables[table] = table.SequenceNumber;
						}
						catch (Exception ex)
						{
							result = false;
                            Trace.TraceWarning("Could not load table {0}.\n{1}", table.Name, ex);
						}
					}                    
                }
            }
			return result;
        }

        /// <summary>Saves all tables to the specified path.</summary>
        public void Save()
        {
			if (Folder == null)
            {
                return;
            }

            lock (m_Tables)
            {
                foreach (IMemoryTable table in Tables)
                {
                    if (m_Tables[table] != table.SequenceNumber)
                    {
                        Trace.TraceInformation("Saving {0}", table);
                        string file = Path.Combine(Folder, table.Name + ".dat");
                        DatWriter.WriteTable(table, file);
                        m_Tables[table] = table.SequenceNumber;
                    }
                    else
                    {
                        Trace.TraceInformation("No changes at {0}", table);
                    }
                }
            }
        }

        /// <summary>Connects to the specified database using the given <see cref="TableConnectorMode" />.</summary>
        /// <param name="mode">The <see cref="TableConnectorMode" /></param>
        /// <param name="database">The database.</param>
        public void Connect(TableConnectorMode mode, IDatabase database)
        {
            if (Database != null)
            {
                throw new Exception("Already connected!");
            }

            Mode = mode;
            Database = database;
            UpdateCache();
        }

        /// <summary>Closes all tables, the database and the storage engine.</summary>
        /// <param name="closeStorage">If not set only the Tables are closed. If set closes Database and Storage, too.</param>
        public virtual void Close(bool closeStorage)
        {
			Save();
			Folder = null;
            foreach (ITable table in Tables)
            {
                if (table is ICachedTable t)
                {
                    t.Close();
                }
            }
            if (closeStorage)
            {
                if (Database != null)
                {
                    IStorage storage = Database.Storage;
                    Database.Close();
                    if (storage != null)
                    {
                        storage.Close();
                    }
                }
                Database = null;
            }
        }

        /// <summary>Updates the cache of all read cached tables.</summary>
        public virtual void UpdateCache(bool async = false)
        {
			if (async)
			{
				Parallel.ForEach(Tables, (table) =>
				{
					if (table is ReadCachedTable t)
                    {
                        t.UpdateCache();
                    }
                });
			}
			else
			{
				foreach (ITable table in Tables)
				{
					if (table is ReadCachedTable t)
                    {
                        t.UpdateCache();
                    }
                }
			}
        }
    }
}
