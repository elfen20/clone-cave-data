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
using Cave.Data.Sql;
using Cave.IO;
using Cave.Text;
using System;
using System.Collections.Generic;
using System.Text;

namespace Cave.Data.Mysql
{
    /// <summary>
    /// Provides a mysql database implementation
    /// </summary>
    public sealed class MySqlDatabase : SqlDatabase
    {
        /// <summary>Gets a value indicating whether this instance is using a secure connection to the storage.</summary>
        /// <value>
        /// <c>true</c> if this instance is using a secure connection; otherwise, <c>false</c>.
        /// </value>
        public override bool IsSecure
        {
            get
            {
                bool error = false;
                SqlConnection connection = SqlStorage.GetConnection(Name);
                try { return connection.ConnectionString.ToUpperInvariant().Contains("SSLMODE=REQUIRED"); }
                catch { error = true; throw; }
                finally { SqlStorage.ReturnConnection(ref connection, error); }
            }
        }

        /// <summary>
        /// Creates a new mysql database instance
        /// </summary>
        /// <param name="storage">the mysql storage engine</param>
        /// <param name="name">the name of the database</param>
        public MySqlDatabase(MySqlStorage storage, string name)
            : base(storage, name)
        {
        }

        /// <summary>
        /// Obtains the available table names
        /// </summary>
        public override string[] TableNames
        {
            get
            {
                List<string> result = new List<string>();
                var rows = SqlStorage.Query(null, "information_schema", "TABLES", "SELECT table_name, table_schema FROM information_schema.TABLES where table_schema LIKE " + SqlStorage.EscapeString(Name));
                foreach (Row row in rows)
                {
                    result.Add((string)row.GetValue(0));
                }
                return result.ToArray();
            }
        }

        /// <summary>
        /// Obtains whether the specified table exists or not
        /// </summary>
        /// <param name="table">The name of the table</param>
        /// <returns></returns>
        public override bool HasTable(string table)
        {
            if (table.HasInvalidChars(ASCII.Strings.SafeName))
            {
                throw new ArgumentException("Table name contains invalid chars!");
            }
            object value = SqlStorage.QueryValue("information_schema", "TABLES", "SELECT COUNT(*) FROM information_schema.TABLES WHERE table_schema LIKE " + SqlStorage.EscapeString(Name) + " AND table_name LIKE " + SqlStorage.EscapeString(table));
            return Convert.ToInt32(value) > 0;
        }

        /// <summary>
        /// Opens the table with the specified name
        /// </summary>
        /// <param name="table">Name of the table</param>
        /// <returns>Returns an <see cref="ITable"/> instance for the specified table</returns>
        public override ITable GetTable(string table)
        {
            if (!HasTable(table))
            {
                throw new InvalidOperationException(string.Format("Table '{0}' does not exist!", table));
            }

            return new MySqlTable(this, table);
        }

        /// <summary>
        /// Opens the table with the specified name
        /// </summary>
        protected override ITable<T> OpenTable<T>(RowLayout layout)
        {
            return new MySqlTable<T>(this, layout);
        }

        /// <summary>
        /// Adds a new table with the specified name
        /// </summary>
        /// <typeparam name="T">The row struct to use for the table</typeparam>
        /// <param name="flags">The flags for table creation</param>
        /// <param name="table">Name of the table to create (optional, use this to overwrite the default table name)</param>
        /// <returns></returns>
        public override ITable<T> CreateTable<T>(TableFlags flags, string table)
        {
            RowLayout layout = RowLayout.CreateTyped(typeof(T), table, Storage);
            CreateTable(layout, flags);
            return OpenTable<T>(layout);
        }

        /// <summary>
        /// Adds a new table with the specified name
        /// </summary>
        /// <param name="layout">Layout of the table</param>
        /// <param name="flags">The flags for table creation</param>
        /// <returns>Returns an <see cref="ITable"/> instance for the specified table</returns>
        public override ITable CreateTable(RowLayout layout, TableFlags flags)
        {
            if (layout == null)
            {
                throw new ArgumentNullException("Layout");
            }

            if (layout.Name.HasInvalidChars(ASCII.Strings.SafeName))
            {
                throw new ArgumentException("Table name contains invalid chars!");
            }
            var utf8Charset = ((MySqlStorage)Storage).CharacterSet;
            LogCreateTable(layout);
            StringBuilder queryText = new StringBuilder();
            queryText.AppendFormat("CREATE TABLE {0} (", SqlStorage.FQTN(Name, layout.Name));
            for (int i = 0; i < layout.FieldCount; i++)
            {
                FieldProperties fieldProperties = layout.GetProperties(i);
                if (i > 0)
                {
                    queryText.Append(",");
                }

                queryText.Append(SqlStorage.EscapeFieldName(fieldProperties));
                queryText.Append(" ");
                switch (fieldProperties.TypeAtDatabase)
                {
                    case DataType.Binary:
                        if (fieldProperties.MaximumLength <= 0)
                        {
                            queryText.Append("LONGBLOB");
                        }
                        else if (fieldProperties.MaximumLength <= 255)
                        {
                            queryText.AppendFormat("TINYBLOB", fieldProperties.MaximumLength);
                        }
                        else if (fieldProperties.MaximumLength <= 65535)
                        {
                            queryText.Append("BLOB");
                        }
                        else if (fieldProperties.MaximumLength <= 16777215)
                        {
                            queryText.Append("MEDIUMBLOB");
                        }
                        else
                        {
                            queryText.Append("LONGBLOB");
                        }
                        break;
                    case DataType.Bool: queryText.Append("TINYINT(1)"); break;
                    case DataType.DateTime: queryText.Append("DATETIME"); break;
                    case DataType.TimeSpan: queryText.Append("DOUBLE"); break;
                    case DataType.Int8: queryText.Append("TINYINT"); break;
                    case DataType.Int16: queryText.Append("SMALLINT"); break;
                    case DataType.Int32: queryText.Append("INTEGER"); break;
                    case DataType.Int64: queryText.Append("BIGINT"); break;
                    case DataType.Single: queryText.Append("FLOAT"); break;
                    case DataType.Double: queryText.Append("DOUBLE"); break;
                    case DataType.Enum: queryText.Append("BIGINT"); break;
                    case DataType.UInt8: queryText.Append("TINYINT UNSIGNED"); break;
                    case DataType.UInt16: queryText.Append("SMALLINT UNSIGNED"); break;
                    case DataType.UInt32: queryText.Append("INTEGER UNSIGNED"); break;
                    case DataType.UInt64: queryText.Append("BIGINT UNSIGNED"); break;

                    case DataType.User:
                    case DataType.String:
                        if (fieldProperties.MaximumLength <= 0)
                        {
                            queryText.Append("LONGTEXT");
                        }
                        else if (fieldProperties.MaximumLength <= 255)
                        {
                            queryText.AppendFormat("VARCHAR({0})", fieldProperties.MaximumLength);
                        }
                        else if (fieldProperties.MaximumLength <= 65535)
                        {
                            queryText.Append("TEXT");
                        }
                        else if (fieldProperties.MaximumLength <= 16777215)
                        {
                            queryText.Append("MEDIUMTEXT");
                        }
                        else
                        {
                            queryText.Append("LONGTEXT");
                        }
                        switch (fieldProperties.StringEncoding)
                        {
                            case StringEncoding.Undefined:
                            case StringEncoding.ASCII: queryText.Append(" CHARACTER SET latin1 COLLATE latin1_general_ci"); break;
                            case StringEncoding.UTF8: queryText.Append($" CHARACTER SET utf8mb4 COLLATE {utf8Charset}_general_ci"); break;
                            case StringEncoding.UTF16: queryText.Append(" CHARACTER SET ucs2 COLLATE ucs2_general_ci"); break;
                            case StringEncoding.UTF32: queryText.Append(" CHARACTER SET utf32 COLLATE utf32_general_ci"); break;
                            default: throw new NotSupportedException(string.Format("MYSQL Server does not support {0}!", fieldProperties.StringEncoding));
                        }
                        break;

                    case DataType.Decimal:
                        if (fieldProperties.MaximumLength > 0)
                        {
                            int l_PreDecimal = (int)fieldProperties.MaximumLength;
                            float l_Temp = (fieldProperties.MaximumLength - l_PreDecimal) * 100;
                            int l_Decimal = (int)l_Temp;
                            if ((l_Decimal >= l_PreDecimal) || (l_Decimal != l_Temp))
                            {
                                throw new ArgumentOutOfRangeException(string.Format("Field {0} has an invalid MaximumLength of {1},{2}. Correct values range from s,p = 1,0 to 65,30(default value) with 0 < s < p!", fieldProperties.Name, l_PreDecimal, l_Decimal));
                            }
                            queryText.AppendFormat("DECIMAL({0},{1})", l_PreDecimal, l_Decimal);
                        }
                        else
                        {
                            queryText.Append("DECIMAL(65,30)");
                        }
                        break;

                    default: throw new NotImplementedException(string.Format("Unknown DataType {0}!", fieldProperties.DataType));
                }
                if ((fieldProperties.Flags & FieldFlags.ID) != 0)
                {
                    queryText.Append(" PRIMARY KEY");
                }
                if ((fieldProperties.Flags & FieldFlags.AutoIncrement) != 0)
                {
                    queryText.Append(" AUTO_INCREMENT");
                }
                if ((fieldProperties.Flags & FieldFlags.Unique) != 0)
                {
                    queryText.Append(" UNIQUE");
                    switch (fieldProperties.TypeAtDatabase)
                    {
                        case DataType.Bool:
                        case DataType.Char:
                        case DataType.DateTime:
                        case DataType.Decimal:
                        case DataType.Double:
                        case DataType.Enum:
                        case DataType.Int8:
                        case DataType.Int16:
                        case DataType.Int32:
                        case DataType.Int64:
                        case DataType.UInt8:
                        case DataType.UInt16:
                        case DataType.UInt32:
                        case DataType.UInt64:
                        case DataType.Single:
                        case DataType.TimeSpan:
                            break;
                        case DataType.String:
                            if (fieldProperties.MaximumLength <= 0)
                            {
                                throw new NotSupportedException(string.Format("Unique string fields without length are not supported! Please define Field.MaxLength at table {0} field {1}", layout.Name, fieldProperties.Name));
                            }
                            break;
                        default: throw new NotSupportedException(string.Format("Uniqueness for table {0} field {1} is not supported!", layout.Name, fieldProperties.Name));
                    }
                }
                if (fieldProperties.Description != null)
                {
                    if (fieldProperties.Description.HasInvalidChars(ASCII.Strings.Printable))
                    {
                        throw new ArgumentException("Description of field '{0}' contains invalid chars!", fieldProperties.Name);
                    }
                    queryText.Append(" COMMENT '" + fieldProperties.Description.Substring(0, 60) + "'");
                }
            }
            queryText.Append(")");
            if (0 != (flags & TableFlags.InMemory))
            {
                queryText.Append(" ENGINE = MEMORY");
            }
            queryText.Append($" CHARACTER SET {utf8Charset} COLLATE {utf8Charset}_general_ci");
            SqlStorage.Execute(Name, layout.Name, queryText.ToString());
            try
            {
                for (int i = 0; i < layout.FieldCount; i++)
                {
                    FieldProperties fieldProperties = layout.GetProperties(i);
                    if ((fieldProperties.Flags & FieldFlags.ID) != 0)
                    {
                        continue;
                    }

                    if ((fieldProperties.Flags & FieldFlags.Index) != 0)
                    {
                        string command;
                        switch (fieldProperties.DataType)
                        {
                            case DataType.Binary:
                            case DataType.String:
                            case DataType.User:
                                int size = (int)fieldProperties.MaximumLength;
                                if (size < 1)
                                {
                                    size = 32;
                                }

                                command = string.Format("CREATE INDEX {0} ON {1} ({2} ({3}))",
                                    "`idx_" + layout.Name + "_" + fieldProperties.Name + "`", SqlStorage.FQTN(Name, layout.Name),
                                    SqlStorage.EscapeFieldName(fieldProperties), size);
                                break;
                            case DataType.Bool:
                            case DataType.Char:
                            case DataType.DateTime:
                            case DataType.Decimal:
                            case DataType.Double:
                            case DataType.Enum:
                            case DataType.Int16:
                            case DataType.Int32:
                            case DataType.Int64:
                            case DataType.Int8:
                            case DataType.Single:
                            case DataType.TimeSpan:
                            case DataType.UInt16:
                            case DataType.UInt32:
                            case DataType.UInt64:
                            case DataType.UInt8:
                                command = string.Format("CREATE INDEX {0} ON {1} ({2})",
                                    "`idx_" + layout.Name + "_" + fieldProperties.Name + "`", SqlStorage.FQTN(Name, layout.Name),
                                    SqlStorage.EscapeFieldName(fieldProperties));
                                break;
                            default: throw new NotSupportedException(string.Format("INDEX for datatype of field {0} is not supported!", fieldProperties));
                        }
                        SqlStorage.Execute(Name, layout.Name, command);
                    }
                }
            }
            catch
            {
                DeleteTable(layout.Name);
                throw;
            }
            return GetTable(layout, TableFlags.None);
        }
    }
}
