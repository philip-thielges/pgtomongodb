using System;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using PostgreToMongo.Options;

namespace PostgreToMongo.Queries;

public class Aufgabe4b : Query
{
    private static readonly string call = @"var collection = Database.GetCollection<BsonDocument>(""inventory"");

            PipelineDefinition<BsonDocument, BsonDocument> pipeline = new BsonDocument[]
            {
                new BsonDocument(""$group"", new BsonDocument()
                        .Add(""_id"", new BsonDocument()
                                .Add(""store_id"", ""$store_id"")
                        )
                        .Add(""unique_count"", new BsonDocument()
                                .Add(""$addToSet"", ""$film_id"")
                        )), 
                new BsonDocument(""$project"", new BsonDocument()
                        .Add(""store_id"", 1.0)
                        .Add(""count"", new BsonDocument()
                                .Add(""$size"", ""$unique_count"")
                        ))
            };";

    public Aufgabe4b(ILogger<Query> logger,
        IOptions<MongoSettings> mongoSettings)
        : base(call, "Aufgabe 4b", logger, mongoSettings)
    {
    }

    public override Task RunAsync()
    {
        var collection = Database.GetCollection<BsonDocument>("inventory");

        PipelineDefinition<BsonDocument, BsonDocument> pipeline = new BsonDocument[]
            {
                new BsonDocument("$group", new BsonDocument()
                        .Add("_id", new BsonDocument()
                                .Add("store_id", "$store_id")
                        )
                        .Add("unique_count", new BsonDocument()
                                .Add("$addToSet", "$film_id")
                        )),
                new BsonDocument("$project", new BsonDocument()
                        .Add("store_id", 1.0)
                        .Add("count", new BsonDocument()
                                .Add("$size", "$unique_count")
                        ))
            };

        return AggregateAsync(collection, pipeline);
    }
}

