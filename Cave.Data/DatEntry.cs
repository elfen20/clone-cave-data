using System.Runtime.InteropServices;
using Cave.IO;

namespace Cave.Data
{
    /// <summary>
    /// Internal index entry for <see cref="DatIndex"/>.
    /// </summary>
    [StructLayout(LayoutKind.Sequential, Pack = 1, Size = 8 + 8 + 4)]
    struct DatEntry
    {
        /// <summary>
        /// Provides the ID of the entry.
        /// </summary>
        public readonly long ID;

        /// <summary>
        /// Provides the position of the entry.
        /// </summary>
        public readonly long BucketPosition;

        /// <summary>
        /// provides the length of the entry.
        /// </summary>
        public readonly int BucketLength;

        /// <summary>Initializes a new instance of the <see cref="DatEntry"/> struct.</summary>
        /// <param name="reader">The reader.</param>
        public DatEntry(DataReader reader)
        {
            ID = reader.Read7BitEncodedInt64();
            BucketPosition = reader.Read7BitEncodedInt64();
            BucketLength = reader.Read7BitEncodedInt32();
        }

        /// <summary>
        /// Creates a new <see cref="DatEntry"/>.
        /// </summary>
        /// <param name="id">ID of the entry.</param>
        /// <param name="pos">Position of the entry.</param>
        /// <param name="count">Length of the entry.</param>
        public DatEntry(long id, long pos, int count)
        {
            ID = id;
            BucketPosition = pos;
            BucketLength = count;
        }

        /// <summary>Saves the specified writer.</summary>
        /// <param name="writer">The writer.</param>
        public void Save(DataWriter writer)
        {
            writer.Write7BitEncoded64(ID);
            writer.Write7BitEncoded64(BucketPosition);
            writer.Write7BitEncoded32(BucketLength);
        }

        /// <summary>Gets the length of the index data.</summary>
        /// <value>The length of the index data.</value>
        public int Length => BitCoder64.GetByteCount7BitEncoded(ID) + BitCoder64.GetByteCount7BitEncoded(BucketPosition) + BitCoder64.GetByteCount7BitEncoded(BucketLength);

        /// <summary>
        /// Obtains "DatEntry[ID:Position Length]".
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return "DatEntry[" + ID + ":" + BucketPosition + " " + BucketLength + "]";
        }
    }

}
