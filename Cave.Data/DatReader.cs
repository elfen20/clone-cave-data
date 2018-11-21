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

using Cave.Collections.Generic;
using Cave.IO;
using System;
using System.Collections.Generic;
using System.IO;

namespace Cave.Data
{
    /// <summary>
    /// Provides reading of dat files to a struct / class
    /// </summary>
    public sealed class DatReader : IDisposable
    {
        /// <summary>
        /// Reads a whole table from the specified dat file
        /// </summary>
        /// <param name="table">Table to read the dat file into</param>
        /// <param name="fileName">File name of the dat file</param>
        public static bool TryReadTable<T>(ITable<T> table, string fileName) where T : struct
        {
            if (!File.Exists(fileName))
            {
                return false;
            }

            try
            {
                using (DatReader reader = new DatReader(fileName))
                {
                    reader.ReadTable(table);
                }
                return true;
            }
            catch { return false; }
        }

        /// <summary>
        /// Reads a whole table from the specified dat file
        /// </summary>
        /// <param name="table">Table to read the dat file into</param>
        /// <param name="fileName">File name of the dat file</param>
        public static void ReadTable<T>(ITable<T> table, string fileName) where T : struct
        {
            using (DatReader reader = new DatReader(fileName))
            {
                reader.ReadTable(table);
            }
        }

        /// <summary>
        /// Reads a whole table from the specified dat file
        /// </summary>
        /// <param name="table">Table to read the dat file into</param>
        /// <param name="fileName">File name of the dat file</param>
        public static void ReadTable(ITable table, string fileName)
        {
            using (DatReader reader = new DatReader(fileName))
            {
                reader.ReadTable(table);
            }
        }

        /// <summary>Reads a whole table from the specified dat file</summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="table">Table to read the dat file into</param>
        /// <param name="stream">The stream.</param>
        public static void ReadTable<T>(ITable<T> table, Stream stream) where T : struct
        {
            using (DatReader reader = new DatReader(stream))
            {
                reader.ReadTable(table);
            }
        }

        /// <summary>Reads a whole table from the specified dat file</summary>
        /// <param name="table">Table to read the dat file into</param>
        /// <param name="stream">The stream.</param>
        public static void ReadTable(ITable table, Stream stream)
        {
            using (DatReader reader = new DatReader(stream))
            {
                reader.ReadTable(table);
            }
        }

        DataReader m_Reader;

        void Load(Stream stream)
        {
            if (stream == null)
            {
                throw new ArgumentNullException("Stream");
            }

            m_Reader = new DataReader(stream);
            int version;
            m_Layout = DatTable.LoadFieldDefinition(m_Reader, out version);
            Version = version;
        }

        /// <summary>
        /// Creates a new dat file reader
        /// </summary>
        /// <param name="fileName"></param>
        public DatReader(string fileName)
        {
            Stream stream = File.OpenRead(fileName);
            try { Load(stream); }
            catch { stream.Dispose(); throw; }
        }

        /// <summary>
        /// Creates a new dat file reader
        /// </summary>
        /// <param name="stream"></param>
        public DatReader(Stream stream)
        {
            Load(stream);
        }

        /// <summary>
        /// Holds the row layout
        /// </summary>
        RowLayout m_Layout;

        /// <summary>
        /// Obtains the layout of the table
        /// </summary>
        public RowLayout Layout { get { return m_Layout; } }

        /// <summary>
        /// Obtains the version the database was created with
        /// </summary>
        public int Version { get; private set; }

        /// <summary>
        /// Reads a row from the file
        /// </summary>
        /// <param name="row"></param>
        /// <returns></returns>
        public bool ReadRow<T>(bool checkLayout, out T row) where T : struct
        {
            RowLayout layout = RowLayout.CreateTyped(typeof(T));
            if (checkLayout)
            {
                RowLayout.CheckLayout(m_Layout, layout);
            }

            if (!m_Layout.IsTyped)
            {
                m_Layout = layout;
            }

            while (m_Reader.BaseStream.Position < m_Reader.BaseStream.Length)
            {
                Row currentRow = DatTable.ReadCurrentRow(m_Reader, Version, layout);
                if (currentRow != null)
                {
                    row = currentRow.GetStruct<T>(layout);
                    return true;
                }
            }
            row = new T();
            return false;
        }

        /// <summary>
        /// Reads the whole file to the specified table
        /// </summary>
        /// <param name="table"></param>
        public void ReadTable<T>(ITable<T> table) where T : struct
        {
            if (table == null)
            {
                throw new ArgumentNullException("Table");
            }

            RowLayout.CheckLayout(m_Layout, table.Layout);
            if (!m_Layout.IsTyped)
            {
                m_Layout = table.Layout;
            }

            while (m_Reader.BaseStream.Position < m_Reader.BaseStream.Length)
            {
                Row row = DatTable.ReadCurrentRow(m_Reader, Version, m_Layout);
                if (row != null)
                {
                    table.Insert(row);
                }
            }
        }

        /// <summary>
        /// Reads the whole file to the specified table. This does not write transactions and does not clear the table.
        /// If you want to start with a clean table clear it prior using this function.
        /// </summary>
        /// <param name="table"></param>
        public void ReadTable(ITable table)
        {
            if (table == null)
            {
                throw new ArgumentNullException("Table");
            }

            RowLayout.CheckLayout(m_Layout, table.Layout);
            if (!m_Layout.IsTyped)
            {
                m_Layout = table.Layout;
            }

            while (m_Reader.BaseStream.Position < m_Reader.BaseStream.Length)
            {
                Row row = DatTable.ReadCurrentRow(m_Reader, Version, m_Layout);
                if (row != null)
                {
                    table.Insert(row);
                }
            }
        }

        /// <summary>
        /// Reads the whole file to a new list
        /// </summary>
        /// <returns></returns>
        public List<T> ReadList<T>() where T : struct
        {
            RowLayout layout = RowLayout.CreateTyped(typeof(T));
            RowLayout.CheckLayout(m_Layout, layout);
            if (!m_Layout.IsTyped)
            {
                m_Layout = layout;
            }

            List<T> result = new List<T>();
            while (m_Reader.BaseStream.Position < m_Reader.BaseStream.Length)
            {
                Row row = DatTable.ReadCurrentRow(m_Reader, Version, layout);
                if (row != null)
                {
                    result.Add(row.GetStruct<T>(layout));
                }
            }
            return result;
        }

        /// <summary>
        /// Closes the reader
        /// </summary>
        public void Close()
        {
            if (m_Reader != null)
            {
                m_Reader.Close();
                m_Reader = null;
            }
        }

        /// <summary>
        /// Disposes the base stream
        /// </summary>
        public void Dispose()
        {
            Close();
        }
    }
}
