using System;
using System.Collections.Generic;
using System.IO;
using Cave.IO;

namespace Cave.Data
{
    /// <summary>
    /// Provides writing of csv files using a struct or class.
    /// </summary>
    public sealed class DatWriter : IDisposable
    {
        /// <summary>
        /// Creates a new dat file with the specified name and writes the whole table.
        /// </summary>
        /// <param name="table"></param>
        /// <param name="fileName"></param>
        public static void WriteTable<T>(ITable<T> table, string fileName)
            where T : struct
        {
            if (table == null)
            {
                throw new ArgumentNullException("table");
            }

            using (var writer = new DatWriter(table.Layout, fileName))
            {
                writer.WriteTable(table);
            }
        }

        /// <summary>
        /// Creates a new dat file with the specified name and writes the whole table.
        /// </summary>
        /// <param name="table"></param>
        /// <param name="fileName"></param>
        public static void WriteTable(ITable table, string fileName)
        {
            if (table == null)
            {
                throw new ArgumentNullException("table");
            }

            using (var writer = new DatWriter(table.Layout, fileName))
            {
                writer.WriteTable(table);
            }
        }

        DataWriter writer;

        /// <summary>
        /// Creates a new dat file writer.
        /// </summary>
        /// <param name="layout"></param>
        /// <param name="fileName"></param>
        public DatWriter(RowLayout layout, string fileName)
            : this(layout, File.Create(fileName))
        {
        }

        /// <summary>
        /// Creates a new csv file writer.
        /// </summary>
        /// <param name="layout"></param>
        /// <param name="stream"></param>
        public DatWriter(RowLayout layout, Stream stream)
        {
            if (stream == null)
            {
                throw new ArgumentNullException("Stream");
            }

            writer = new DataWriter(stream);
            this.layout = layout;
            DatTable.WriteFieldDefinition(writer, this.layout, DatTable.CurrentVersion);
        }

        /// <summary>
        /// row layout.
        /// </summary>
        RowLayout layout;

        void WriteData(byte[] data)
        {
            var entrySize = data.Length + BitCoder32.GetByteCount7BitEncoded(data.Length + 10);
            var start = writer.BaseStream.Position;
            BitCoder32.Write7BitEncoded(writer, entrySize);
            writer.Write(data);

            var fill = start + entrySize - writer.BaseStream.Position;
            if (fill > 0)
            {
                writer.Write(new byte[fill]);
            }
            else if (fill < 0)
            {
                throw new IOException("Container too small!");
            }
        }

        /// <summary>
        /// Writes a row to the file.
        /// </summary>
        /// <param name="row"></param>
        public void Write(Row row)
        {
            var data = DatTable.GetData(layout, row, DatTable.CurrentVersion);
            WriteData(data);
        }

        /// <summary>
        /// Writes a row to the file.
        /// </summary>
        /// <param name="value"></param>
        public void Write<T>(T value)
            where T : struct
        {
            Write(new Row(layout.GetValues(value)));
        }

        /// <summary>
        /// Writes a number of rows to the file.
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
                var data = DatTable.GetData(layout, row, DatTable.CurrentVersion);
                WriteData(data);
            }
        }

        /// <summary>
        /// Writes a number of rows to the file.
        /// </summary>
        /// <param name="table"></param>
        public void WriteRows<T>(IEnumerable<T> table)
            where T : struct
        {
            if (table == null)
            {
                throw new ArgumentNullException("table");
            }

            var layout = RowLayout.CreateTyped(typeof(T));
            RowLayout.CheckLayout(this.layout, layout);
            foreach (T dataSet in table)
            {
                var row = new Row(layout.GetValues(dataSet));
                var data = DatTable.GetData(this.layout, row, DatTable.CurrentVersion);
                WriteData(data);
            }
        }

        /// <summary>
        /// Writes a full table of rows to the file.
        /// </summary>
        /// <param name="table"></param>
        public void WriteTable(ITable table)
        {
            if (table == null)
            {
                throw new ArgumentNullException("table");
            }

            RowLayout.CheckLayout(layout, table.Layout);
            foreach (Row row in table.GetRows())
            {
                var data = DatTable.GetData(layout, row, DatTable.CurrentVersion);
                WriteData(data);
            }
        }

        /// <summary>
        /// Closes the writer and the stream.
        /// </summary>
        public void Close()
        {
            if (writer != null)
            {
                writer.Close();
                writer = null;
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
