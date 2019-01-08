using System;
using System.Collections.Generic;
using System.IO;
using Cave.IO;

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
            m_Layout = DatTable.LoadFieldDefinition(m_Reader, out int version);
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
        public RowLayout Layout => m_Layout;

        /// <summary>
        /// Obtains the version the database was created with
        /// </summary>
        public int Version { get; private set; }

        /// <summary>
        /// Reads a row from the file
        /// </summary>
        /// <param name="checkLayout">Check layout prior read</param>
        /// <param name="row">The read row</param>
        /// <returns>Returns true is the row was read, false otherwise</returns>
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
