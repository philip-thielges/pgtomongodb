﻿using System;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using PostgreToMongo.Options;

namespace PostgreToMongo.Queries
{
    public class Aufgabe4a:QueryBuilder
    {
        private static readonly string customCall = @"var inventoryCollection = Database.GetCollection<BsonDocument>(""inventory"");

            PipelineDefinition<BsonDocument, BsonDocument> filter = new BsonDocument[]
            {
                new BsonDocument(""$group"", new BsonDocument()
                        .Add(""_id"", new BsonDocument())
                        .Add(""SUM"", new BsonDocument()
                                .Add(""$sum"", 1)
                        )),
                new BsonDocument(""$project"", new BsonDocument()
                        .Add(""SUMME"", ""$SUM"")
                        .Add(""_id"", 0))

            };";
        public Aufgabe4a(ILogger<Aufgabe4a> logger, IOptions<MongoSettings> mongoSettings)
            :base(customCall, "Aufgabe 4a): Gesamtanzahl der verfügbaren Filme", logger, mongoSettings)
        {
        }

        public override Task ExecuteAsync()
        {
            //Use GetCollection Method to get the inventory Collection
            var inventoryCollection = Database.GetCollection<BsonDocument>("inventory");

            PipelineDefinition<BsonDocument, BsonDocument> filter = new BsonDocument[]
            {
                new BsonDocument("$group", new BsonDocument() // $group is the aggregation operator
                        .Add("_id", new BsonDocument())
                        .Add("SUM", new BsonDocument()
                                .Add("$sum", 1) // $sum is the aggregation operator
                        )),
                new BsonDocument("$project", new BsonDocument() // $project is the aggregation operator
                        .Add("SUMME", "$SUM") // SUMME is the name of the new field
                        .Add("_id", 0)) // _id is always added by default, so we have to remove it

            };

            return PerformAggregationAsync(inventoryCollection, filter);
        }
    }
}
