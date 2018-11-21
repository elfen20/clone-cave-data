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
#endregion
#region Authors & Contributors
/*
   Author:
     Andreas Rohleder <andreas@rohleder.cc>

   Contributors:

 */
#endregion

using System;

namespace Cave.Data
{
    /// <summary>
    /// Provides a field name <see cref="Attribute"/> for renaming fields at database rows
    /// (Using different name at struct and database)
    /// </summary>
    [AttributeUsage(AttributeTargets.Field, Inherited = false)]
    public sealed class FieldAttribute : Attribute
    {
        /// <summary>
        /// Gets/sets the "real" field name (at the database)
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Gets/sets the maximum field length (at the database)
        /// </summary>
        public uint Length { get; set; }

        /// <summary>Gets or sets the type of the data.</summary>
        /// <value>The type of the data.</value>
        public DataType DataType { get; set; }

        /// <summary>
        /// Gets/sets the flags
        /// </summary>
        public FieldFlags Flags { get; set; }

        /// <summary>Gets or sets the display format.</summary>
        /// <value>The display format.</value>
        public string DisplayFormat { get; set; }

        /// <summary>
        /// Gets/sets additional field names (at the database) that will be matched
        /// </summary>
        public string AlternativeNames { get; set; }

        /// <summary>
        /// Creates a new <see cref="FieldAttribute"/>
        /// </summary>
        public FieldAttribute()
        {
        }
    }
}
