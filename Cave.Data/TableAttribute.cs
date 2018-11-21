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
    /// Provides a table <see cref="Attribute"/> for table settings at database structs
    /// </summary>

    [AttributeUsage(AttributeTargets.Struct, Inherited = false)]
    public sealed class TableAttribute : Attribute
    {
        /// <summary>Gets the name set within a TableAttribute for the specified type.</summary>
        /// <param name="type">Type to search for attributes.</param>
        /// <returns></returns>
        /// <exception cref="Exception">Type {0} does not define a TableAttribute!</exception>
        public static string GetName(Type type)
        {
            if (type == null)
            {
                throw new ArgumentNullException("type");
            }

            foreach (Attribute attribute in type.GetCustomAttributes(false))
            {
                TableAttribute tableAttribute = attribute as TableAttribute;
                if (tableAttribute != null)
                {
                    return tableAttribute.Name;
                }
            }
            throw new ArgumentException(string.Format("Type {0} does not define a TableAttribute!", type));
        }

        /// <summary>
        /// Gets/sets the "real" field name (at the database)
        /// </summary>
        public string Name { get; private set; }

        /// <summary>
        /// Creates a new <see cref="TableAttribute"/>
        /// </summary>
        public TableAttribute()
        {
        }

        /// <summary>
        /// Creates a new <see cref="TableAttribute"/> with the specified name
        /// </summary>
        /// <param name="name">Name for the table</param>
        public TableAttribute(string name)
        {
            Name = name;
        }
    }
}
