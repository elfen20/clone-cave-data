using System;
using System.IO;
using Cave.IO;

namespace Cave.Data
{
    /// <summary>Provides Row based serialization.</summary>
    public static partial class RowSerializer
    {
        #region private Data Serializer

        static void SerializeData(DataWriter writer, RowLayout layout, Row row)
        {
            for (var i = 0; i < layout.FieldCount; i++)
            {
                var dataType = layout[i].DataType;
                switch (dataType)
                {
                    case DataType.Binary:
                    {
                        var data = (byte[]) row[i];
                        if (data == null)
                        {
                            data = new byte[0];
                        }

                        writer.Write7BitEncoded32(data.Length);
                        writer.Write(data);
                        break;
                    }
                    case DataType.Bool:
                        writer.Write((bool) row[i]);
                        break;
                    case DataType.TimeSpan:
                        writer.Write((TimeSpan) row[i]);
                        break;
                    case DataType.DateTime:
                        writer.Write((DateTime) row[i]);
                        break;
                    case DataType.Single:
                        writer.Write((float) row[i]);
                        break;
                    case DataType.Double:
                        writer.Write((double) row[i]);
                        break;
                    case DataType.Int8:
                        writer.Write((sbyte) row[i]);
                        break;
                    case DataType.Int16:
                        writer.Write((short) row[i]);
                        break;
                    case DataType.UInt8:
                        writer.Write((byte) row[i]);
                        break;
                    case DataType.UInt16:
                        writer.Write((ushort) row[i]);
                        break;
                    case DataType.Int32:
                        writer.Write7BitEncoded32((int) row[i]);
                        break;
                    case DataType.Int64:
                        writer.Write7BitEncoded64((long) row[i]);
                        break;
                    case DataType.UInt32:
                        writer.Write7BitEncoded32((uint) row[i]);
                        break;
                    case DataType.UInt64:
                        writer.Write7BitEncoded64((ulong) row[i]);
                        break;
                    case DataType.Char:
                        writer.Write((char) row[i]);
                        break;
                    case DataType.Decimal:
                        writer.Write((decimal) row[i]);
                        break;
                    case DataType.String:
                    case DataType.User:
                    {
                        var data = row[i];
                        var str = data?.ToString();
                        writer.WritePrefixed(str);
                        break;
                    }
                    case DataType.Enum:
                    {
                        var value = Convert.ToInt64(row[i]);
                        writer.Write7BitEncoded64(value);
                        break;
                    }
                    default:
                        throw new NotImplementedException($"Datatype {dataType} not implemented!");
                }
            }
        }

        #endregion

        #region private Data Deserializer

        static Row DeserializeData(DataReader reader, RowLayout layout)
        {
            var values = new object[layout.FieldCount];
            for (var i = 0; i < layout.FieldCount; i++)
            {
                var dataType = layout[i].DataType;
                switch (dataType)
                {
                    case DataType.Binary:
                        var size = reader.Read7BitEncodedInt32();
                        values[i] = reader.ReadBytes(size);
                        break;
                    case DataType.Bool:
                        values[i] = reader.ReadBool();
                        break;
                    case DataType.DateTime:
                        values[i] = reader.ReadDateTime();
                        break;
                    case DataType.TimeSpan:
                        values[i] = reader.ReadTimeSpan();
                        break;
                    case DataType.Int8:
                        values[i] = reader.ReadInt8();
                        break;
                    case DataType.Int16:
                        values[i] = reader.ReadInt16();
                        break;
                    case DataType.Int32:
                        values[i] = reader.Read7BitEncodedInt32();
                        break;
                    case DataType.Int64:
                        values[i] = reader.Read7BitEncodedInt64();
                        break;
                    case DataType.UInt32:
                        values[i] = reader.Read7BitEncodedUInt32();
                        break;
                    case DataType.UInt64:
                        values[i] = reader.Read7BitEncodedUInt64();
                        break;
                    case DataType.UInt8:
                        values[i] = reader.ReadUInt8();
                        break;
                    case DataType.UInt16:
                        values[i] = reader.ReadUInt16();
                        break;
                    case DataType.Char:
                        values[i] = reader.ReadChar();
                        break;
                    case DataType.Single:
                        values[i] = reader.ReadSingle();
                        break;
                    case DataType.Double:
                        values[i] = reader.ReadDouble();
                        break;
                    case DataType.Decimal:
                        values[i] = reader.ReadDecimal();
                        break;
                    case DataType.String:
                        values[i] = reader.ReadString();
                        break;
                    case DataType.Enum:
                        values[i] = reader.Read7BitEncodedInt64();
                        break;
                    case DataType.User:
                        values[i] = reader.ReadString();
                        break;
                    default: throw new NotImplementedException($"Datatype {dataType} not implemented!");
                }
            }

            return new Row(layout, values, false);
        }

        #endregion

        #region Serializer

        /// <summary>Serializes the specified items without layout.</summary>
        /// <typeparam name="TStruct">Structure type.</typeparam>
        /// <param name="writer">The writer.</param>
        /// <param name="items">The items.</param>
        public static void Serialize<TStruct>(this DataWriter writer, params TStruct[] items)
            where TStruct : struct
        {
            Serialize(writer, 0, items);
        }

        /// <summary>Serializes the specified items.</summary>
        /// <typeparam name="TStruct">Structure type.</typeparam>
        /// <param name="writer">The writer.</param>
        /// <param name="flags">The settings used during serialization.</param>
        /// <param name="items">The items.</param>
        /// <exception cref="ArgumentNullException">Items or Writer.</exception>
        public static void Serialize<TStruct>(this DataWriter writer, Flags flags, params TStruct[] items)
            where TStruct : struct
        {
            if (items == null)
            {
                throw new ArgumentNullException(nameof(items));
            }

            if (writer == null)
            {
                throw new ArgumentNullException(nameof(writer));
            }

            writer.Write7BitEncoded32((int) flags);
            var layout = RowLayout.CreateTyped(typeof(TStruct));
            if ((flags & Flags.WithLayout) != 0)
            {
                layout.Save(writer);
            }

            writer.Write7BitEncoded64(items.Length);
            foreach (var item in items)
            {
                SerializeData(writer, layout, layout.GetRow(item));
            }
        }

        /// <summary>Serializes the specified table.</summary>
        /// <param name="writer">The writer.</param>
        /// <param name="flags">The settings used during serialization.</param>
        /// <param name="table">The table.</param>
        /// <exception cref="ArgumentNullException">Table or Writer.</exception>
        public static void Serialize(this DataWriter writer, Flags flags, ITable table)
        {
            if (table == null)
            {
                throw new ArgumentNullException(nameof(table));
            }

            if (writer == null)
            {
                throw new ArgumentNullException(nameof(writer));
            }

            writer.Write7BitEncoded32((int) flags);
            if ((flags & Flags.WithLayout) != 0)
            {
                table.Layout.Save(writer);
            }

            writer.Write7BitEncoded64(table.RowCount);
            foreach (var row in table.GetRows())
            {
                SerializeData(writer, table.Layout, row);
            }
        }

        /// <summary>Serializes the specified table.</summary>
        /// <param name="writer">The writer.</param>
        /// <param name="flags">The flags.</param>
        /// <param name="layout">The layout.</param>
        /// <param name="rows">The rows.</param>
        /// <exception cref="ArgumentNullException">Rows or Layout or Writer.</exception>
        public static void Serialize(this DataWriter writer, Flags flags, RowLayout layout, params Row[] rows)
        {
            if (rows == null)
            {
                throw new ArgumentNullException(nameof(rows));
            }

            if (layout == null)
            {
                throw new ArgumentNullException(nameof(layout));
            }

            if (writer == null)
            {
                throw new ArgumentNullException(nameof(writer));
            }

            writer.Write7BitEncoded32((int) flags);
            if ((flags & Flags.WithLayout) != 0)
            {
                layout.Save(writer);
            }

            writer.Write7BitEncoded64(rows.Length);
            foreach (var row in rows)
            {
                SerializeData(writer, layout, row);
            }
        }

        #endregion

        #region Deserializer

        /// <summary>Deserializes an item array.</summary>
        /// <typeparam name="TStruct">Structure type.</typeparam>
        /// <param name="reader">The reader.</param>
        /// <returns>The deserialized structures.</returns>
        /// <exception cref="ArgumentNullException">Reader.</exception>
        public static TStruct[] DeserializeItems<TStruct>(this DataReader reader)
            where TStruct : struct
        {
            if (reader == null)
            {
                throw new ArgumentNullException(nameof(reader));
            }

            var flags = (Flags) reader.Read7BitEncodedInt32();
            var layout = RowLayout.CreateTyped(typeof(TStruct));
            if ((flags & Flags.WithLayout) != 0)
            {
                var otherLayout = RowLayout.Load(reader);
                RowLayout.CheckLayout(layout, otherLayout);
            }

            var results = new TStruct[reader.Read7BitEncodedInt32()];
            for (var i = 0; i < results.Length; i++)
            {
                var row = DeserializeData(reader, layout);
                results[i] = row.GetStruct<TStruct>(layout);
            }

            return results;
        }

        /// <summary>Deserializes a structure.</summary>
        /// <typeparam name="TStruct">Structure type.</typeparam>
        /// <param name="reader">The reader.</param>
        /// <returns>The deserialized structure.</returns>
        public static TStruct DeserializeStruct<TStruct>(this DataReader reader)
            where TStruct : struct
        {
            var layout = RowLayout.CreateTyped(typeof(TStruct));
            var row = DeserializeRow(reader, layout);
            return row.GetStruct<TStruct>(layout);
        }

        /// <summary>Deserializes a row.</summary>
        /// <param name="reader">The reader.</param>
        /// <param name="layout">The layout.</param>
        /// <returns>The deserialized row.</returns>
        public static Row DeserializeRow(this DataReader reader, RowLayout layout)
        {
            if (layout == null)
            {
                throw new ArgumentNullException(nameof(layout));
            }

            if (reader == null)
            {
                throw new ArgumentNullException(nameof(reader));
            }

            var flags = (Flags) reader.Read7BitEncodedInt32();
            if ((flags & Flags.WithLayout) != 0)
            {
                var otherLayout = RowLayout.Load(reader);
                RowLayout.CheckLayout(layout, otherLayout);
            }

            var count = reader.Read7BitEncodedInt32();
            if (count != 1)
            {
                throw new InvalidDataException($"Got {count} Rows at the stream but want to read exactly one!");
            }

            return DeserializeData(reader, layout);
        }

        #endregion

        #region Foreign Deserializer (needs layout at stream)

        /// <summary>Deserializes a foreign row.</summary>
        /// <remarks>
        ///     This can only deserialize rows written with layout. (Requires use of <see cref="Flags.WithLayout" /> when
        ///     serializing.)
        /// </remarks>
        /// <param name="reader">The reader to read from.</param>
        /// <returns>The deserialized row instance.</returns>
        public static Row DeserializeForeignRow(this DataReader reader)
        {
            if (reader == null)
            {
                throw new ArgumentNullException(nameof(reader));
            }

            var flags = (Flags) reader.Read7BitEncodedInt32();
            if ((flags & Flags.WithLayout) == 0)
            {
                throw new NotSupportedException(
                    "For DeserializeForeignX() functions the layout has to be written by the sender! The current decoded data does not contain a layout!");
            }

            var layout = RowLayout.Load(reader);
            var count = reader.Read7BitEncodedInt32();
            if (count != 1)
            {
                throw new InvalidDataException($"Got {count} Rows at the stream but want to read exactly one!");
            }

            return DeserializeData(reader, layout);
        }

        /// <summary>Deserializes a foreign table.</summary>
        /// <remarks>
        ///     This can only deserialize rows written with layout. (Requires use of <see cref="Flags.WithLayout" /> when
        ///     serializing.)
        /// </remarks>
        /// <param name="reader">The reader to read from.</param>
        /// <returns>The deserialized table instance.</returns>
        public static ITable DeserializeForeignTable(this DataReader reader)
        {
            if (reader == null)
            {
                throw new ArgumentNullException(nameof(reader));
            }

            var flags = (Flags) reader.Read7BitEncodedInt32();
            if ((flags & Flags.WithLayout) == 0)
            {
                throw new NotSupportedException(
                    "For DeserializeForeignX() functions the layout has to be written by the sender! The current decoded data does not contain a layout!");
            }

            var layout = RowLayout.Load(reader);
            var result = MemoryTable.Create(layout);
            var count = reader.Read7BitEncodedInt64();
            for (long l = 0; l < count; l++)
            {
                var row = DeserializeData(reader, layout);
                result.Insert(row);
            }

            return result;
        }

        #endregion
    }
}
