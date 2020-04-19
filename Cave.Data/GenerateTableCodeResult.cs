namespace Cave.Data
{
    /// <summary>
    /// Provides the code generated as result of a ITable.GenerateX() function.
    /// </summary>
    public struct GenerateTableCodeResult
    {
        /// <summary>
        /// Gets the name of the database.
        /// </summary>
        public string DatabaseName { get; internal set; }

        /// <summary>
        /// Gets the name of the table.
        /// </summary>
        public string TableName { get; internal set; }

        /// <summary>
        /// Gets the generated code.
        /// </summary>
        public string Code { get; internal set; }

        /// <summary>
        /// Gets the filename the code was saved to. This may be null if code was not saved to a file.
        /// </summary>
        public string FileName { get; internal set; }

        /// <summary>
        /// Gets the class name of the generated table structure.
        /// </summary>
        public string ClassName { get; internal set; }
    }
}
