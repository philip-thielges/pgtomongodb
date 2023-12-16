using System;
namespace PostgreToMongo.Options
{
    public class PostgreSettings
    {
        /// <summary>
        /// The connection string for thw PostgreSQL databse.
        /// Is defined in the appsettings.json
        /// </summary>
        public string ConnectionString { get; set; }
    }
}

