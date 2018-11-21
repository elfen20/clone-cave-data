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
using System.IO;
using System.Runtime.InteropServices;

namespace Cave.Data
{
    /// <summary>
    /// Internal index entry for <see cref="DatIndex"/>
    /// </summary>
    [StructLayout(LayoutKind.Sequential, Pack = 1, Size = 8 + 8 + 4)]
    struct DatEntry
    {
        /// <summary>
        /// Provides the ID of the entry
        /// </summary>
        public readonly long ID;

        /// <summary>
        /// Provides the position of the entry
        /// </summary>
        public readonly long BucketPosition;

        /// <summary>
        /// provides the length of the entry
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
        /// Creates a new <see cref="DatEntry"/>
        /// </summary>
        /// <param name="id">ID of the entry</param>
        /// <param name="pos">Position of the entry</param>
        /// <param name="count">Length of the entry</param>
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
        public int Length
        {
            get
            {
                return BitCoder64.GetByteCount7BitEncoded(ID) + BitCoder64.GetByteCount7BitEncoded(BucketPosition) + BitCoder64.GetByteCount7BitEncoded(BucketLength);
            }
        }

        /// <summary>
        /// Obtains "DatEntry[ID:Position Length]"
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return "DatEntry[" + ID + ":" + BucketPosition + " " + BucketLength + "]";
        }
    }

}
