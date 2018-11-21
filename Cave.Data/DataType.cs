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

namespace Cave
{
    /// <summary>
    /// Provides available field data types
    /// </summary>
    public enum DataType
    {
        /// <summary>
        /// Unknown data
        /// </summary>
        Unknown = 0,

        /// <summary>
        /// Bool data<br />
        /// Equals 0 for false and any other value for true
        /// </summary>
        Bool = 1,

        /// <summary>
        /// signed value<br />
        /// 1 byte (8 bit) data
        /// </summary>
        Int8 = 2,

        /// <summary>
        /// unsigned value<br />
        /// 1 byte (8 bit) data
        /// </summary>
        UInt8 = 3,

        /// <summary>
        /// signed value<br />
        /// 2 byte (16 bit) data
        /// </summary>
        Int16 = 4,

        /// <summary>
        /// unsigned value<br />
        /// 2 byte (16 bit) data
        /// </summary>
        UInt16 = 5,

        /// <summary>
        /// signed value<br />
        /// 4 byte (32 bit) data
        /// </summary>
        Int32 = 6,

        /// <summary>
        /// unsigned value<br />
        /// 4 byte (32 bit) data
        /// </summary>
        UInt32 = 7,

        /// <summary>
        /// signed value<br />
        /// 8 byte (64 bit) data
        /// </summary>
        Int64 = 8,

        /// <summary>
        /// unsigned value<br />
        /// 8 byte (64 bit) data
        /// </summary>
        UInt64 = 9,

        /// <summary>
        /// a single character using 7..32 bit
        /// </summary>
        Char = 10,

        /// <summary>
        /// Floating point data<br />
        /// 4 byte (32 bit) single precision floating point value
        /// </summary>
        Single = 0x21,

        /// <summary>
        /// Floating point data<br />
        /// 8 byte (64 bit) double precision floating point value
        /// </summary>
        Double = 0x22,

        /// <summary>
        /// High precision floating point
        /// </summary>
        Decimal = 0x23,

        /// <summary>
        /// Datetime data<br />
        /// 8 byte (64 bit) ticks (1 tick = 100 ns) since 01.01.0000 00:00:00
        /// </summary>
        DateTime = 0x24,

        /// <summary>
        /// Timespan data<br />
        /// 8 byte (64 bit) ticks (1 tick = 100 ns)<br />
        /// most database implementations will save this as seconds (double)
        /// </summary>
        TimeSpan = 0x25,

        /// <summary>
        /// String data<br />
        /// Utf-8 encoded string
        /// </summary>
        String = 0x26,

        /// <summary>
        /// Binary data<br />
        /// Array of bytes
        /// </summary>
        Binary = 0x27,

        #region DataTypes that need value type informations
        /// <summary>
        /// Mask to check if value type informations are needed to deserialize the type
        /// </summary>
        MaskRequireValueType = 0x40,

        /// <summary>
        /// Enum datatype
        /// </summary>
        Enum = 0x41,

        /// <summary>
        /// User defined datatype (uses Parse(string) and ToString())
        /// </summary>
        User = 0x42,
        #endregion
    }
}
