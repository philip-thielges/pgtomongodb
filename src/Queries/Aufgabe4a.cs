using System;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using PostgreToMongo.Options;

namespace PostgreToMongo.Queries
{
    public class Aufgabe4a:Query
    {
        private static readonly string call = @"var collection = Database.GetCollection<BsonDocument>(""inventory"");

            PipelineDefinition<BsonDocument, BsonDocument> pipeline = new BsonDocument[]
            {
                new BsonDocument(""$group"", new BsonDocument()
                        .Add(""_id"", new BsonDocument())
                        .Add(""COUNT(*)"", new BsonDocument()
                                .Add(""$sum"", 1)
                        )),
                new BsonDocument(""$project"", new BsonDocument()
                        .Add(""COUNT(*)"", ""$COUNT(*)"")
                        .Add(""_id"", 0))
            };";
        public Aufgabe4a(ILogger<Aufgabe4a> logger, IOptions<MongoSettings> mongoSettings)
            :base(call, "Aufgabe 4a", logger, mongoSettings)
        {
        }

        public override Task RunAsync()
        {
            var collection = Database.GetCollection<BsonDocument>("inventory");

            PipelineDefinition<BsonDocument, BsonDocument> pipeline = new BsonDocument[]
            {
                new BsonDocument("$group", new BsonDocument()
                        .Add("_id", new BsonDocument())
                        .Add("COUNT(*)", new BsonDocument()
                                .Add("$sum", 1)
                        )),
                new BsonDocument("$project", new BsonDocument()
                        .Add("COUNT(*)", "$COUNT(*)")
                        .Add("_id", 0))
            };


            return AggregateAsync(collection, pipeline);
        }
    }
}

