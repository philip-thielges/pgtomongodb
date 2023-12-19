using System;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using PostgreToMongo.Options;

namespace PostgreToMongo.Queries;

public class Aufgabe4b : QueryBuilder
{
    private static readonly string customCall = @"var inventoryCollection = Database.GetCollection<BsonDocument>(""inventory"");

        PipelineDefinition<BsonDocument, BsonDocument> filter = new BsonDocument[]
            {
                new BsonDocument(""$group"", new BsonDocument()
                        .Add(""_id"", new BsonDocument()
                                .Add(""Standort_Store"", ""$store_id"")
                        )
                        .Add(""count_per_store"", new BsonDocument()
                                .Add(""$addToSet"", ""$film_id"")
                        )),
                new BsonDocument(""$project"", new BsonDocument()
                        .Add(""_id"", 0) // Exclude the _id field
                        .Add(""store_id"", ""$_id.Standort_Store"")
                        .Add(""SUMME"", new BsonDocument()
                                .Add(""$size"", ""$count_per_store"")
                        ))
            };";

    public Aufgabe4b(ILogger<QueryBuilder> logger,
        IOptions<MongoSettings> mongoSettings)
        : base(customCall, "Aufgabe 4b): Anzahl der unterschiedlichen Filme je Standort", logger, mongoSettings)
    {
    }

    public override Task ExecuteAsync()
    {
        //Use GetCollection Method to get the inventory Collection
        var inventoryCollection = Database.GetCollection<BsonDocument>("inventory");

        PipelineDefinition<BsonDocument, BsonDocument> filter = new BsonDocument[]
            {
                new BsonDocument("$group", new BsonDocument()
                        .Add("_id", new BsonDocument()
                                .Add("Standort_Store", "$store_id")
                        )
                        .Add("count_per_store", new BsonDocument()
                                .Add("$addToSet", "$film_id")
                        )),
                new BsonDocument("$project", new BsonDocument()
                        .Add("_id", 0) // Exclude the _id field
                        .Add("store_id", "$_id.Standort_Store")
                        .Add("SUMME", new BsonDocument()
                                .Add("$size", "$count_per_store")
                        ))
            };

        return PerformAggregationAsync(inventoryCollection, filter);
    }
}

