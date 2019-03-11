namespace Cave.Data
{
    /// <summary>
    /// Sql search modes.
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
