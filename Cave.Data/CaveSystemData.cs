#region CopyRight 2018
/*
    Copyright (c) 2003-2018 Andreas Rohleder (andreas@rohleder.cc)
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

using System;
using System.Reflection;

namespace Cave
{
    /// <summary>
    /// Provides public access the Cave System Data Assembly instance
    /// </summary>
    public static class CaveSystemData
    {
        static int _TransactionRowCount = 5000;

        /// <summary>Gets the type.</summary>
        /// <value>The type.</value>
        public static Type Type { get { return typeof(CaveSystemData); } }

        /// <summary>
        /// Obtains the assembly
        /// </summary>
        public static Assembly Assembly { get { return Type.Assembly; } }

        /// <summary>
        /// Obtains the <see cref="AssemblyVersionInfo"/> for the <see cref="Assembly"/>
        /// </summary>
        public static AssemblyVersionInfo VersionInfo { get { return AssemblyVersionInfo.FromAssembly(Assembly); } }

        /// <summary>
        /// Number of rows per chunk on big data operations
        /// </summary>
        public static int TransactionRowCount
        {
            get { return _TransactionRowCount; }
            set { _TransactionRowCount = Math.Max(1, value); }
        }

        /// <summary>
        /// Provides the date time format for big int date time values
        /// </summary>
        public const string BigIntDateTimeFormat = "yyyyMMddHHmmssfff";

        /// <summary>
        /// Calculates a database id based on crc64.
        /// You can use this for ID fields based on a unique name field.
        /// </summary>
        /// <param name="text"></param>
        /// <returns></returns>
        public static long CalculateID(string text)
        {
            if (string.IsNullOrEmpty(text))
            {
                throw new ArgumentNullException(nameof(text));
            }

            return text.GetHashCode() + 1L - (long)int.MinValue;
        }
    }
}
