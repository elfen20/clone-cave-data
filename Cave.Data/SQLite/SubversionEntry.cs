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

namespace Cave.Data.SQLite
{
    /// <summary>
    /// Provides access to a subversion entry
    /// </summary>
    public class SubversionEntry
    {
        string[] m_Data;
        
        /// <summary>
        /// Obtains the subversion entry version
        /// </summary>
        public readonly int Version;

        /// <summary>
        /// Obtains whether the entry is valid or not
        /// </summary>
        public readonly bool IsValid;

        /// <summary>
        /// Creates a new <see cref="SubversionEntry"/> from the specified cdata
        /// </summary>
        /// <param name="data"></param>
        /// <param name="version"></param>
        public SubversionEntry(string[] data, int version)
        {
            Version = version;
            m_Data = data;
            IsValid = (Version >= 8) || (Version <= 10);
        }

        /// <summary>
        /// Obtains the type of the <see cref="SubversionEntry"/>
        /// </summary>
        public SubversionEntryType Type
        {
            get
            {
                switch (m_Data[1])
                {
                    case "dir": return SubversionEntryType.Directory;
                    case "file": return SubversionEntryType.File;
                    default: return SubversionEntryType.Unknown;
                }
            }
        }

        /// <summary>
        /// Obtains the name of the <see cref="SubversionEntry"/>
        /// </summary>
        public string Name { get { return m_Data[0]; } }

        /// <summary>
        /// Checks whether the entry was deleted or not
        /// </summary>
        public bool Deleted
        {
            get
            {
                switch (Version)
                {
                    case 8:
                    case 9:
                    case 10:
                        return (m_Data[5] == "delete");
                    default:
                        return false;
                }
            }
        }

        /// <summary>
        /// Provides a Name and Type string
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return Name + " " + Type;
        }

        /// <summary>
        /// Provides a hascode for this instance
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            return ToString().GetHashCode();
        }
    }
}
