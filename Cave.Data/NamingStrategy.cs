namespace Cave.Data
{
    /// <summary>Provides strategies to create names from text.</summary>
    public enum NamingStrategy
    {
        /// <summary>Keep exact name.</summary>
        Exact,

        /// <summary>Build a camel case name.</summary>
        CamelCase,

        /// <summary>Build a snake case name.</summary>
        SnakeCase
    }
}
