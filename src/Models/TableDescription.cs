using System;
namespace PostgreToMongo.Models
{
    /// <summary>
    /// Descripe a single Data Table in the PostgreSQL Databse, which also includes the data.
    /// The infos are needed to migrate the data the right way into mongo DB.
    /// </summary>
    public class TableDescription
    {
        /// <summary>
        /// The name of the table
        /// Which will later be the name of the collection, in mongo.
        /// </summary>
        public string TableName { get; set; }
        /// <summary>
        /// A list of columndescriptions, containing important infos about the columns. (IsPk or not...)
        /// </summary>
        public List<ColumnDescription> ColumnDescriptions { get; set; } = new List<ColumnDescription>();
        /// <summary>
        /// All rows int the data table, one row consists of dictionary.
        /// The key is the column name, and the value is the value for the specific row and column.
        /// </summary>
        public List<Dictionary<string, object>> Table { get; set; } = new List<Dictionary<string, object>>();
    }
}

