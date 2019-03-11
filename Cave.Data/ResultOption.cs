using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cave.Data
{
    /// <summary>
    /// Provides resultset options for search, sort and grouping functions.
    /// </summary>
    public sealed class ResultOption
    {
        /// <summary>Implements the operator ==.</summary>
        /// <param name="A">The first item.</param>
        /// <param name="B">The second item.</param>
        /// <returns>The result of the operator.</returns>
        public static bool operator ==(ResultOption A, ResultOption B)
        {
            if (ReferenceEquals(B, null))
            {
                return ReferenceEquals(A, null);
            }

            return ReferenceEquals(A, null) ? false : A.Mode == B.Mode && A.Parameter == B.Parameter;
        }

        /// <summary>Implements the operator !=.</summary>
        /// <param name="A">The first item.</param>
        /// <param name="B">The second item.</param>
        /// <returns>The result of the operator.</returns>
        public static bool operator !=(ResultOption A, ResultOption B)
        {
            return !(A == B);
        }

        /// <summary>
        /// Combines two <see cref="ResultOption"/>s with AND.
        /// </summary>
        /// <param name="A"></param>
        /// <param name="B"></param>
        /// <returns></returns>
        public static ResultOption operator +(ResultOption A, ResultOption B)
        {
            if (A == null)
            {
                throw new ArgumentNullException("A");
            }

            A.Add(B);
            return A;
        }

        /// <summary>
        /// No option.
        /// </summary>
        public static ResultOption None { get; } = new ResultOption(ResultOptionMode.None, null);

        /// <summary>
        /// Sort ascending by the specified fieldname.
        /// </summary>
        /// <param name="field"></param>
        /// <returns></returns>
        public static ResultOption SortAscending(string field)
        {
            return new ResultOption(ResultOptionMode.SortAsc, field);
        }

        /// <summary>
        /// Sort descending by the specified fieldname.
        /// </summary>
        /// <param name="field"></param>
        /// <returns></returns>
        public static ResultOption SortDescending(string field)
        {
            return new ResultOption(ResultOptionMode.SortDesc, field);
        }

        /// <summary>
        /// Limit the number of result sets.
        /// </summary>
        /// <param name="resultCount"></param>
        /// <returns></returns>
        public static ResultOption Limit(int resultCount)
        {
            if (resultCount < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(resultCount));
            }

            return new ResultOption(ResultOptionMode.Limit, resultCount.ToString());
        }

        /// <summary>
        /// Set start offset of result sets.
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public static ResultOption Offset(int offset)
        {
            if (offset < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(offset));
            }

            return new ResultOption(ResultOptionMode.Offset, offset.ToString());
        }

        /// <summary>
        /// Group the fields with the specified fieldname.
        /// </summary>
        /// <param name="field"></param>
        /// <returns></returns>
        public static ResultOption Group(string field)
        {
            return new ResultOption(ResultOptionMode.Group, field);
        }

        List<ResultOption> m_ParentList = new List<ResultOption>();

        ResultOption(ResultOptionMode mode, string parameter)
        {
            Mode = mode;
            Parameter = parameter;
            m_ParentList.Add(this);
        }

        /// <summary>Gets the mode.</summary>
        /// <value>The mode.</value>
        public ResultOptionMode Mode { get; private set; }

        /// <summary>Gets the parameter name.</summary>
        /// <value>The parameter name.</value>
        public string Parameter { get; private set; }

        /// <summary>Adds the specified option.</summary>
        /// <param name="option">The option.</param>
        /// <exception cref="InvalidOperationException"></exception>
        public void Add(ResultOption option)
        {
            if (option.m_ParentList == m_ParentList)
            {
                throw new InvalidOperationException();
            }

            m_ParentList.AddRange(option.m_ParentList);
            option.m_ParentList = m_ParentList;
        }

        /// <summary>Returns an array with all options.</summary>
        /// <returns>Returns an array with all options.</returns>
        public ResultOption[] ToArray()
        {
            return m_ParentList.Where(i => i.Mode != ResultOptionMode.None).ToArray();
        }

        /// <summary>Returns an array with all options with the specified modes.</summary>
        /// <param name="modes">The modes.</param>
        /// <returns>Returns an array with all matching options.</returns>
        public ResultOption[] ToArray(params ResultOptionMode[] modes)
        {
            List<ResultOption> results = new List<ResultOption>();
            foreach (ResultOption option in m_ParentList)
            {
                if (Array.IndexOf(modes, option.Mode) > -1)
                {
                    results.Add(option);
                }
            }
            return results.ToArray();
        }

        /// <summary>Determines whether [contains] [the specified modes].</summary>
        /// <param name="modes">The modes.</param>
        /// <returns><c>true</c> if [contains] [the specified modes]; otherwise, <c>false</c>.</returns>
        public bool Contains(params ResultOptionMode[] modes)
        {
            foreach (ResultOption option in m_ParentList)
            {
                if (Array.IndexOf(modes, option.Mode) > -1)
                {
                    return true;
                }
            }
            return false;
        }

        /// <summary>Gets the field names.</summary>
        /// <value>The field names.</value>
        public string[] FieldNames
        {
            get
            {
                List<string> result = new List<string>();
                foreach (ResultOption option in ToArray())
                {
                    switch (option.Mode)
                    {
                        case ResultOptionMode.SortAsc:
                        case ResultOptionMode.SortDesc:
                        case ResultOptionMode.Group:
                            result.Add(option.Parameter);
                            break;
                    }
                }
                return result.ToArray();
            }
        }

        /// <summary>
        /// Obtains a string describing this instance.
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            StringBuilder result = new StringBuilder();
            foreach (ResultOption option in ToArray())
            {
                if (result.Length > 0)
                {
                    result.Append(", ");
                }

                result.Append(option.Mode.ToString());
                if (Parameter != null)
                {
                    result.Append("[");
                    result.Append(option.Parameter);
                    result.Append("]");
                }
            }
            return result.ToString();
        }

        /// <summary>Determines whether the specified <see cref="object" />, is equal to this instance.</summary>
        /// <param name="obj">The <see cref="object" /> to compare with this instance.</param>
        /// <returns><c>true</c> if the specified <see cref="object" /> is equal to this instance; otherwise, <c>false</c>.</returns>
        public override bool Equals(object obj)
        {
            ResultOption o = obj as ResultOption;
            return o == null ? false : o.Mode == Mode && o.Parameter == Parameter;
        }

        /// <summary>
        /// Obtains the hashcode for this instance.
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            return ToString().GetHashCode();
        }
    }
}
