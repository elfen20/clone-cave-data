using System;
using System.Collections.Generic;
using System.IO;
using Cave.IO;

namespace Cave.Data
{
    /// <summary>
    /// Provides reading of dat files to a struct / class.
    /// </summary>
    public sealed class DatReader : IDisposable
    {
        /// <summary>
        /// Reads a whole table from the specified dat file.
        /// </summary>
        /// <param name="table">Table to read the dat file into.</param>
        /// <param name="fileName">File name of the dat file.</param>
        public static bool TryReadTable<T>(ITable<T> table, string fileName)
            where T : struct
        {
            if (!File.Exists(fileName))
            {
                return false;
            }

            try
            {
                using (var reader = new DatReader(fileName))
                {
                    reader.ReadTable(table);
                }
                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Reads a whole table from the specified dat file.
        /// </summary>
        /// <param name="table">Table to read the dat file into.</param>
        /// <param name="fileName">File name of the dat file.</param>
        public static void ReadTable<T>(ITable<T> table, string fileName)
            where T : struct
        {
            using (var reader = new DatReader(fileName))
            {
                reader.ReadTable(table);
            }
        }

        /// <summary>
        /// Reads a whole table from the specified dat file.
        /// </summary>
        /// <param name="table">Table to read the dat file into.</param>
        /// <param name="fileName">File name of the dat file.</param>
        public static void ReadTable(ITable table, string fileName)
        {
            using (var reader = new DatReader(fileName))
            {
                reader.ReadTable(table);
            }
        }

        /// <summary>Reads a whole table from the specified dat file.</summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="table">Table to read the dat file into.</param>
        /// <param name="stream">The stream.</param>
        public static void ReadTable<T>(ITable<T> table, Stream stream)
            where T : struct
        {
            using (var reader = new DatReader(stream))
            {
                reader.ReadTable(table);
            }
        }

        /// <summary>Reads a whole table from the specified dat file.</summary>
        /// <param name="table">Table to read the dat file into.</param>
        /// <param name="stream">The stream.</param>
        public static void ReadTable(ITable table, Stream stream)
        {
            using (var reader = new DatReader(stream))
            {
                reader.ReadTable(table);
            }
        }

        DataReader reader;

        void Load(Stream stream)
        {
            if (stream == null)
            {
                throw new ArgumentNullException("Stream");
            }

            reader = new DataReader(stream);
            Layout = DatTable.LoadFieldDefinition(reader, out var version);
            Version = version;
        }

        /// <summary>
        /// Creates a new dat file reader.
        /// </summary>
        /// <param name="fileName"></param>
        public DatReader(string fileName)
        {
            Stream stream = File.OpenRead(fileName);
            try
            {
                Load(stream);
            }
            catch
            {
                stream.Dispose();
                throw;
            }
        }

        /// <summary>
        /// Creates a new dat file reader.
        /// </summary>
        /// <param name="stream"></param>
        public DatReader(Stream stream)
        {
            Load(stream);
        }

        /// <summary>
        /// Gets the layout of the table.
        /// </summary>
        public RowLayout Layout { get; private set; }

        /// <summary>
        /// Gets the version the database was created with.
        /// </summary>
        public int Version { get; private set; }

        /// <summary>
        /// Reads a row from the file.
        /// </summary>
        /// <param name="checkLayout">Check layout prior read.</param>
        /// <param name="row">The read row.</param>
        /// <returns>Returns true is the row was read, false otherwise.</returns>
        public bool ReadRow<T>(bool checkLayout, out T row)
            where T : struct
        {
            var layout = RowLayout.CreateTyped(typeof(T));
            if (checkLayout)
            {
                RowLayout.CheckLayout(Layout, layout);
            }

            if (!Layout.IsTyped)
            {
                Layout = layout;
            }

            while (reader.BaseStream.Position < reader.BaseStream.Length)
            {
                Row currentRow = DatTable.ReadCurrentRow(reader, Version, layout);
                if (currentRow != null)
                {
                    row = currentRow.GetStruct<T>(layout);
                    return true;
                }
            }
            row = default(T);
            return false;
        }

        /// <summary>
        /// Reads the whole file to the specified table.
        /// </summary>
        /// <param name="table"></param>
        public void ReadTable<T>(ITable<T> table)
            where T : struct
        {
            if (table == null)
            {
                throw new ArgumentNullException("Table");
            }

            RowLayout.CheckLayout(Layout, table.Layout);
            if (!Layout.IsTyped)
            {
                Layout = table.Layout;
            }

            while (reader.BaseStream.Position < reader.BaseStream.Length)
            {
                Row row = DatTable.ReadCurrentRow(reader, Version, Layout);
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

            RowLayout.CheckLayout(Layout, table.Layout);
            if (!Layout.IsTyped)
            {
                Layout = table.Layout;
            }

            while (reader.BaseStream.Position < reader.BaseStream.Length)
            {
                Row row = DatTable.ReadCurrentRow(reader, Version, Layout);
                if (row != null)
                {
                    table.Insert(row);
                }
            }
        }

        /// <summary>
        /// Reads the whole file to a new list.
        /// </summary>
        /// <returns></returns>
        public List<T> ReadList<T>()
            where T : struct
        {
            var layout = RowLayout.CreateTyped(typeof(T));
            RowLayout.CheckLayout(Layout, layout);
            if (!Layout.IsTyped)
            {
                Layout = layout;
            }

            var result = new List<T>();
            while (reader.BaseStream.Position < reader.BaseStream.Length)
            {
                Row row = DatTable.ReadCurrentRow(reader, Version, layout);
                if (row != null)
                {
                    result.Add(row.GetStruct<T>(layout));
                }
            }
            return result;
        }

        /// <summary>
        /// Closes the reader.
        /// </summary>
        public void Close()
        {
            if (reader != null)
            {
                reader.Close();
                reader = null;
            }
        }

        /// <summary>
        /// Disposes the base stream.
        /// </summary>
        public void Dispose()
        {
            Close();
        }
    }
}
