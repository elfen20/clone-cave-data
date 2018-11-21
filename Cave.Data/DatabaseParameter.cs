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

using Cave.Text;
using System;

namespace Cave.Data
{
    /// <summary>
    /// Provides a basic implementation of the <see cref="IDatabaseParameter"/> interface
    /// </summary>
    public class DatabaseParameter : IDatabaseParameter
    {
        object m_Value;
        string m_Name;

        /// <summary>
        /// Creates a new parameter with the specified name and value
        /// </summary>
        /// <param name="name"></param>
        /// <param name="value"></param>
        public DatabaseParameter(string name, object value)
        {
            if (string.IsNullOrEmpty(name))
            {
                throw new ArgumentNullException("Name");
            }

            m_Name = name.ReplaceInvalidChars(ASCII.Strings.Letters + ASCII.Strings.Digits, "_");
            m_Value = value;
        }

        /// <summary>
        /// Gets/sets the name of the <see cref="DatabaseParameter"/>
        /// </summary>
        public virtual string Name { get { return m_Name; } }

        /// <summary>
        /// Gets/sets the value of the <see cref="DatabaseParameter"/>
        /// </summary>
        public virtual object Value { get { return m_Value; } }

        /// <summary>
        /// Provides name and value of the parameter
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            string value = m_Value == null ? "<null>" : m_Value.ToString();
            return Name + " = " + value;
        }

        /// <summary>
        /// Obtains the hascode for this parameter
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            return ToString().GetHashCode();
        }
    }
}
