using System;
namespace PostgreToMongo.Models
{
    /// <summary>
    /// The description of a column, in the postgreSQL Data Table.
    /// IsPrimaryKey and DataTypeName, will mostelikely not be used, but are important for debugging.
    /// </summary>
    public class ColumnDescription
    {
        /// <summary>
        /// name of the column.
        /// </summary>
        public string Name { get; set; }
        /// <summary>
        /// Will be true, if the specified Column is a primary key, of the table.
        /// </summary>
        public bool IsPrimaryKey { get; set; }
        /// <summary>
        /// The data type of the column.
        /// </summary>
        public string DataTypeName { get; set; }
    }
}

