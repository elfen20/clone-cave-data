using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace Cave.Data
{
    /// <summary>
    /// Provides extension functions for ITable instances.
    /// </summary>
    public static class ITableExtensions
    {
        #region ITable extensions

        /// <summary>Tries to insert the specified dataset (id has to be set).</summary>
        /// <param name="table">The table.</param>
        /// <param name="row">The row.</param>
        /// <returns>Returns true if the dataset was inserted, false otherwise.</returns>
        public static bool TryInsert(this ITable table, Row row)
        {
            //TODO, implement this without exceptions: needed at Table, SqlTable, MemoryTable
            try { table.Insert(row); return true; }
            catch { return false; }
        }

        /// <summary>Tries to insert the specified dataset (id has to be set).</summary>
        /// <param name="table">The table.</param>
        /// <param name="row">The row.</param>
        /// <returns>Returns true if the dataset was inserted, false otherwise.</returns>
        public static bool TryUpdate(this ITable table, Row row)
        {
            //TODO, implement this without exceptions: needed at Table, SqlTable, MemoryTable
            try { table.Update(row); return true; }
            catch { return false; }
        }

        /// <summary>Tries to delete the dataset with the specified id.</summary>
        /// <param name="table">The table.</param>
        /// <param name="id">The identifier.</param>
        /// <returns>Returns true if the dataset was removed, false otherwise.</returns>
        public static bool TryDelete(this ITable table, long id)
        {
            return table.TryDelete(table.Layout.IDField.Name, id) > 0;
        }

        /// <summary>Tries to delete the datasets with the specified identifiers.</summary>
        /// <param name="table">The table.</param>
        /// <param name="ids">The identifiers.</param>
        /// <returns>Returns the number of datasets removed, 0 if the database does not support deletion count or no dataset was removed.</returns>
        public static int TryDelete(this ITable table, IEnumerable<long> ids)
        {
            return table.TryDelete(Search.FieldIn(table.Layout.IDField.Name, ids));
        }

        /// <summary>Removes all rows from the table matching the given search.</summary>
        /// <param name="table">The table.</param>
        /// <param name="field">The fieldname to match</param>
        /// <param name="value">The value to match</param>
        /// <returns>Returns the number of datasets deleted.</returns>
        public static int TryDelete(this ITable table, string field, object value)
        {
            return table.TryDelete(Search.FieldEquals(field, value));
        }

        /// <summary>Checks a given search for any datasets matching</summary>
        /// <param name="table">The table.</param>
        /// <param name="field">The fields name</param>
        /// <param name="value">The value</param>
        /// <returns>Returns true if a dataset exists, false otherwise.</returns>
        public static bool Exist(this ITable table, string field, object value)
        {
            return table.Exist(Search.FieldEquals(field, value));
        }

        /// <summary>Searches the table for a row with given field value combination.</summary>
        /// <param name="table">The table.</param>
        /// <param name="field">The fieldname to match</param>
        /// <param name="value">The value to match</param>
        /// <returns>Returns the ID of the row found or -1</returns>
        public static long FindRow(this ITable table, string field, object value)
        {
            return table.FindRow(Search.FieldEquals(field, value), ResultOption.None);
        }

        /// <summary>Searches the table for a single row with given field value combination.</summary>
        /// <param name="table">The table.</param>
        /// <param name="field">The fieldname to match</param>
        /// <param name="value">The value to match</param>
        /// <returns>Returns the row found</returns>
        public static Row GetRow(this ITable table, string field, object value)
        {
            return table.GetRow(Search.FieldEquals(field, value), ResultOption.None);
        }

        /// <summary>Searches the table for rows with given field value combinations.</summary>
        /// <param name="table">The table.</param>
        /// <param name="field">The fieldname to match</param>
        /// <param name="value">The value to match</param>
        /// <returns>Returns the IDs of the rows found</returns>
        public static List<long> FindRows(this ITable table, string field, object value)
        {
            return table.FindRows(Search.FieldEquals(field, value), ResultOption.None);
        }

        /// <summary>Searches the table for rows with given field value combinations.</summary>
        /// <param name="table">The table.</param>
        /// <param name="field">The fieldname to match</param>
        /// <param name="value">The value to match</param>
        /// <returns>Returns the rows found</returns>
        public static List<Row> GetRows(this ITable table, string field, object value)
        {
            return table.GetRows(Search.FieldEquals(field, value), ResultOption.None);
        }

        /// <summary>Caches the whole table into memory and provides a new ITable{T} instance</summary>
        /// <param name="table">The table.</param>
        /// <returns>Returns a new memory table.</returns>
        public static MemoryTable ToMemory(this ITable table)
        {
            Trace.TraceInformation("Copy {0} rows to memory table", table.RowCount);
            MemoryTable mem = new MemoryTable(table.Layout);
            mem.LoadTable(table);
            return mem;
        }

        /// <summary>Counts the rows with specified field value combination.</summary>
        /// <param name="table">The table.</param>
        /// <param name="field">The fieldname to match</param>
        /// <param name="value">The value to match</param>
        /// <returns>Returns the number of rows found matching the criteria given</returns>
        public static long Count(this ITable table, string field, object value)
        {
            return table.Count(Search.FieldEquals(field, value), ResultOption.None);
        }
        #endregion

        #region ITable<T> extensions

        /// <summary>Tries to insert the specified dataset (id has to be set).</summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="table">The table.</param>
        /// <param name="row">The row.</param>
        /// <returns>Returns true if the dataset was inserted, false otherwise.</returns>
        public static bool TryInsert<T>(this ITable<T> table, T row) where T : struct
        {
            //TODO, implement this without exceptions: needed at Table, SqlTable, MemoryTable
            try { table.Insert(row); return true; }
            catch { return false; }
        }

        /// <summary>Tries to insert the specified dataset (id has to be set).</summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="table">The table.</param>
        /// <param name="row">The row.</param>
        /// <returns>Returns true if the dataset was inserted, false otherwise.</returns>
        public static bool TryUpdate<T>(this ITable<T> table, T row) where T : struct
        {
            //TODO, implement this without exceptions: needed at Table, SqlTable, MemoryTable
            try { table.Update(row); return true; }
            catch { return false; }
        }

        /// <summary>Tries to get the row with the specified id.</summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="table">The table.</param>
        /// <param name="id">The identifier.</param>
        /// <returns>Returns the structure on success, an empty one otherwise</returns>
        public static T TryGetStruct<T>(this ITable<T> table, long id) where T : struct
        {
            return table.GetStructs(new long[] { id }).FirstOrDefault();
        }

        /// <summary>Tries to get the (unique) row with the specified fieldvalue.</summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="table">The table.</param>
        /// <param name="name">The name.</param>
        /// <param name="value">The value.</param>
        /// <returns>Returns the structure on success, an empty one otherwise</returns>
        public static T TryGetStruct<T>(this ITable<T> table, string name, object value) where T : struct
        {
            return table.GetStructs(name, value).FirstOrDefault();
        }

        /// <summary>Tries to get the (unique) row matching the specified search.</summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="table">The table.</param>
        /// <param name="search">The search.</param>
        /// <returns>Returns the structure on success, an empty one otherwise</returns>
        public static T TryGetStruct<T>(this ITable<T> table, Search search) where T : struct
        {
            return table.GetStructs(search).FirstOrDefault();
        }

        /// <summary>Tries to get the row with the specified id.</summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="table">The table.</param>
        /// <param name="id">The row identifier.</param>
        /// <param name="row">The row.</param>
        /// <returns>Returns true on success, false otherwise</returns>
        public static bool TryGetStruct<T>(this ITable<T> table, long id, out T row) where T : struct
        {
            List<T> results = table.GetStructs(new long[] { id });
            if (results.Count > 0)
            {
                row = results[0];
                return true;
            }
            row = default(T);
            return false;
        }

        /// <summary>Tries to get the (unique) row with the specified fieldvalue.</summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="table">The table.</param>
        /// <param name="field">Name of the field.</param>
        /// <param name="value">The value.</param>
        /// <param name="row">The row.</param>
        /// <returns>Returns true on success, false otherwise</returns>
        public static bool TryGetStruct<T>(this ITable<T> table, string field, object value, out T row) where T : struct
        {
            List<T> results = table.GetStructs(field, value);
            if (results.Count > 0)
            {
                row = results[0];
                return true;
            }
            row = default(T);
            return false;
        }

        /// <summary>Tries to get the (unique) row with the specified fieldvalue.</summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="table">The table.</param>
        /// <param name="search">The search.</param>
        /// <param name="row">The row.</param>
        /// <returns>Returns true on success, false otherwise</returns>
        public static bool TryGetStruct<T>(this ITable<T> table, Search search, out T row) where T : struct
        {
            List<T> results = table.GetStructs(search);
            if (results.Count > 0)
            {
                row = results[0];
                return true;
            }
            row = default(T);
            return false;
        }

        /// <summary>Searches the table for rows with given field value combinations.</summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="table">The table.</param>
        /// <param name="field">The fieldname to match</param>
        /// <param name="value">The value to match</param>
        /// <returns>Returns the rows found</returns>
        public static List<T> GetStructs<T>(this ITable<T> table, string field, object value) where T : struct
        {
            return table.GetStructs(Search.FieldEquals(field, value), ResultOption.None);
        }

        /// <summary>Searches the table for a single row with given field value combination.</summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="table">The table.</param>
        /// <param name="field">The fieldname to match</param>
        /// <param name="value">The value to match</param>
        /// <returns>Returns the row found</returns>
        public static T GetStruct<T>(this ITable<T> table, string field, object value) where T : struct
        {
            return table.GetStruct(Search.FieldEquals(field, value), ResultOption.None);
        }

        /// <summary>
        /// Caches the whole table into memory and provides a new ITable{T} instance
        /// </summary>
        public static MemoryTable<T> ToTypedMemory<T>(this ITable<T> table) where T : struct
        {
            MemoryTable<T> mem = new MemoryTable<T>();
            mem.LoadTable(table);
            return mem;
        }

        /// <summary>Retrieves the whole table as array.</summary>
        /// <returns></returns>
        public static T[] ToArray<T>(this ITable<T> table) where T : struct
        {
            return table.GetStructs().ToArray();
        }
        #endregion
    }
}
