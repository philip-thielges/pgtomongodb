using System;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using PostgreToMongo.Options;

namespace PostgreToMongo.Queries
{
    public class Aufgabe4c:Query
    {
        private static readonly string call = @"var collection = Database.GetCollection<BsonDocument>(""film_actor"");

            PipelineDefinition<BsonDocument, BsonDocument> pipeline = new BsonDocument[]
            {
                new BsonDocument(""$group"", new BsonDocument()
                        .Add(""_id"", new BsonDocument()
                                .Add(""actor_id"", ""$actor_id"")
                        )
                        .Add(""COUNT(film_id)"", new BsonDocument()
                                .Add(""$sum"", 1)
                        )),
                new BsonDocument(""$project"", new BsonDocument()
                        .Add(""actor_id"", ""$_id.actor_id"")
                        .Add(""COUNT(film_id)"", ""$COUNT(film_id)"")
                        .Add(""_id"", 0)),
                new BsonDocument(""$sort"", new BsonDocument()
                        .Add(""COUNT(film_id)"", -1)),
                new BsonDocument(""$limit"", 10)
            };";

        public Aufgabe4c(ILogger<Query> logger,
            IOptions<MongoSettings> mongoSettings)
            :base(call, "Aufgabe 4C", logger, mongoSettings)
        {
        }

        public override Task RunAsync()
        {
            var collection = Database.GetCollection<BsonDocument>("film_actor");

            PipelineDefinition<BsonDocument, BsonDocument> pipeline = new BsonDocument[]
            {
                new BsonDocument("$group", new BsonDocument()
                        .Add("_id", new BsonDocument()
                                .Add("actor_id", "$actor_id")
                        )
                        .Add("COUNT(film_id)", new BsonDocument()
                                .Add("$sum", 1)
                        )),
                new BsonDocument("$project", new BsonDocument()
                        .Add("actor_id", "$_id.actor_id")
                        .Add("COUNT(film_id)", "$COUNT(film_id)")
                        .Add("_id", 0)),
                new BsonDocument("$sort", new BsonDocument()
                        .Add("COUNT(film_id)", -1)),
                new BsonDocument("$limit", 10)
            };


            return AggregateAsync(collection, pipeline);
        }
    }
}

