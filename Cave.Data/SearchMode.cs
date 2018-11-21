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

namespace Cave.Data
{
    /// <summary>
    /// Sql search modes
    /// </summary>
    public enum SearchMode
    {
        /// <summary>no search, returns all datasets</summary>
        None = 0,

        /// <summary>value comparison: check for equality</summary>
        Equals = 1,

        /// <summary>binary and on two searches</summary>
        And = 2,

        /// <summary>binary or on two searches</summary>
        Or = 3,

        /// <summary>value comparison: sql like</summary>
        Like = 4,

        /// <summary>value comparison: greater than specified value</summary>
        Greater = 5,

        /// <summary>value comparison: smaller than specified value</summary>
        Smaller = 6,

        /// <summary>value comparison: greater than or equal to specified value</summary>
        GreaterOrEqual = 7,

        /// <summary>value comparison: smaller than or equal to specified value</summary>
        SmallerOrEqual = 8,

        /// <summary>value comparison: value in specified list</summary>
        In = 9,
    }
}
