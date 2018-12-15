namespace Cave.Data
{
    /// <summary>
    /// Available result option modes
    /// </summary>
    public enum ResultOptionMode
    {
        /// <summary>No option / empty</summary>
        None,

        /// <summary>The sort ascending option</summary>
        SortAsc,

        /// <summary>The sort descending option</summary>
        SortDesc,

        /// <summary>The group option</summary>
        Group,

        /// <summary>The limit option</summary>
        Limit,

        /// <summary>The offset option</summary>
        Offset,
    }
}
