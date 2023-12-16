using System;
namespace PostgreToMongo.Options
{
    public class MongoSettings
    {
        public string ConnectionString { get; set; } = null!;

        public string DatabaseName { get; set; } = null!;
    }
}

