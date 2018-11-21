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

using Cave.IO;
using System;
using System.Collections.Generic;
using System.IO;

namespace Cave.Data
{
    /// <summary>
    /// Provides writing of csv files using a struct or class
    /// </summary>
    public sealed class DatWriter : IDisposable
    {
        /// <summary>
        /// Creates a new dat file with the specified name and writes the whole table
        /// </summary>
        /// <param name="table"></param>
        /// <param name="fileName"></param>
        public static void WriteTable<T>(ITable<T> table, string fileName) where T : struct
        {
            if (table == null)
            {
                throw new ArgumentNullException("table");
            }

            using (DatWriter writer = new DatWriter(table.Layout, fileName))
            {
                writer.WriteTable(table);
            }
        }

        /// <summary>
        /// Creates a new dat file with the specified name and writes the whole table
        /// </summary>
        /// <param name="table"></param>
        /// <param name="fileName"></param>
        public static void WriteTable(ITable table, string fileName)
        {
            if (table == null)
            {
                throw new ArgumentNullException("table");
            }

            using (DatWriter writer = new DatWriter(table.Layout, fileName))
            {
                writer.WriteTable(table);
            }
        }

        DataWriter m_Writer;

        /// <summary>
        /// Creates a new dat file writer
        /// </summary>
        /// <param name="layout"></param>
        /// <param name="fileName"></param>
        public DatWriter(RowLayout layout, string fileName)
            : this(layout, File.Create(fileName))
        {
        }

        /// <summary>
        /// Creates a new csv file writer
        /// </summary>
        /// <param name="layout"></param>
        /// <param name="stream"></param>
        public DatWriter(RowLayout layout, Stream stream)
        {
            if (stream == null)
            {
                throw new ArgumentNullException("Stream");
            }

            m_Writer = new DataWriter(stream);
            m_Layout = layout;
            DatTable.WriteFieldDefinition(m_Writer, m_Layout, DatTable.CurrentVersion);
        }

        /// <summary>
        /// row layout
        /// </summary>
        RowLayout m_Layout;

        void WriteData(byte[] data)
        {
            int entrySize = data.Length + BitCoder32.GetByteCount7BitEncoded(data.Length + 10);
            long start = m_Writer.BaseStream.Position;
            BitCoder32.Write7BitEncoded(m_Writer, entrySize);
            m_Writer.Write(data);

            long fill = (start + entrySize) - m_Writer.BaseStream.Position;
            if (fill > 0)
            {
                m_Writer.Write(new byte[fill]);
            }
            else if (fill < 0)
            {
                throw new IOException("Container too small!");
            }
        }

        /// <summary>
        /// Writes a row to the file
        /// </summary>
        /// <param name="row"></param>
        public void Write(Row row)
        {
            byte[] data = DatTable.GetData(m_Layout, row, DatTable.CurrentVersion);
            WriteData(data);
        }

        /// <summary>
        /// Writes a row to the file
        /// </summary>
        /// <param name="value"></param>
        public void Write<T>(T value) where T : struct
        {
            Write(new Row(m_Layout.GetValues(value)));
        }

        /// <summary>
        /// Writes a number of rows to the file
        /// </summary>
        /// <param name="table"></param>
        public void WriteTable(IEnumerable<Row> table)
        {
            if (table == null)
            {
                throw new ArgumentNullException("table");
            }

            foreach (Row row in table)
            {
                byte[] data = DatTable.GetData(m_Layout, row, DatTable.CurrentVersion);
                WriteData(data);
            }
        }

        /// <summary>
        /// Writes a number of rows to the file
        /// </summary>
        /// <param name="table"></param>
        public void WriteRows<T>(IEnumerable<T> table) where T : struct
        {
            if (table == null)
            {
                throw new ArgumentNullException("table");
            }

            RowLayout layout = RowLayout.CreateTyped(typeof(T));
            RowLayout.CheckLayout(m_Layout, layout);
            foreach (T dataSet in table)
            {
                Row row = new Row(layout.GetValues(dataSet));
                byte[] data = DatTable.GetData(m_Layout, row, DatTable.CurrentVersion);
                WriteData(data);
            }
        }

        /// <summary>
        /// Writes a full table of rows to the file
        /// </summary>
        /// <param name="table"></param>
        public void WriteTable(ITable table)
        {
            if (table == null)
            {
                throw new ArgumentNullException("table");
            }

            RowLayout.CheckLayout(m_Layout, table.Layout);
            foreach (Row row in table.GetRows())
            {
                byte[] data = DatTable.GetData(m_Layout, row, DatTable.CurrentVersion);
                WriteData(data);
            }
        }

        /// <summary>
        /// Closes the writer and the stream
        /// </summary>
        public void Close()
        {
            if (m_Writer != null)
            {
                m_Writer.Close();
                m_Writer = null;
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
