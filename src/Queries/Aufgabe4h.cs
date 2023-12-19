using System;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using PostgreToMongo.Options;

namespace PostgreToMongo.Queries
{
    public class Aufgabe4h : QueryBuilder
    {
        private static readonly string customCall = @"var categoryCollection = Database.GetCollection<BsonDocument>(""category"");

            PipelineDefinition<BsonDocument, BsonDocument> filter = new BsonDocument[]
            {
                new BsonDocument(""$project"", new BsonDocument()
                        .Add(""_id"", 0)
                        .Add(""category"", ""$$ROOT"")),
                new BsonDocument(""$lookup"", new BsonDocument()
                        .Add(""localField"", ""category.category_id"")
                        .Add(""from"", ""film_category"")
                        .Add(""foreignField"", ""category_id"")
                        .Add(""as"", ""film_category"")),
                new BsonDocument(""$unwind"", new BsonDocument()
                        .Add(""path"", ""$film_category"")
                        .Add(""preserveNullAndEmptyArrays"", new BsonBoolean(false))),
                new BsonDocument(""$lookup"", new BsonDocument()
                        .Add(""localField"", ""film_category.film_id"")
                        .Add(""from"", ""inventory"")
                        .Add(""foreignField"", ""film_id"")
                        .Add(""as"", ""inventory"")),
                new BsonDocument(""$unwind"", new BsonDocument()
                        .Add(""path"", ""$inventory"")
                        .Add(""preserveNullAndEmptyArrays"", new BsonBoolean(false))),
                new BsonDocument(""$lookup"", new BsonDocument()
                        .Add(""localField"", ""inventory.inventory_id"")
                        .Add(""from"", ""rental"")
                        .Add(""foreignField"", ""inventory_id"")
                        .Add(""as"", ""rental"")),
                new BsonDocument(""$unwind"", new BsonDocument()
                        .Add(""path"", ""$rental"")
                        .Add(""preserveNullAndEmptyArrays"", new BsonBoolean(false))),
                new BsonDocument(""$group"", new BsonDocument()
                        .Add(""_id"", new BsonDocument()
                                .Add(""category\u1390name"", ""$category.name"")
                        )
                        .Add(""count"", new BsonDocument()
                                .Add(""$sum"", 1)
                        )),
                new BsonDocument(""$project"", new BsonDocument()
                        .Add(""category.name"", ""$_id.category\u1390name"")
                        .Add(""count"", ""$count"")
                        .Add(""_id"", 0)),
                new BsonDocument(""$sort"", new BsonDocument()
                        .Add(""count"", -1)),
                new BsonDocument(""$limit"", 3)
            };";
        public Aufgabe4h(ILogger<QueryBuilder> logger,
            IOptions<MongoSettings> mongoSettings)
            : base(customCall, "Aufgabe 4h): Die 3 meistgesehenen Filmkategorien", logger, mongoSettings)
        {
        }

        public override Task ExecuteAsync()
        {
                //Use GetCollection Method to get the category Collection
            var categoryCollection = Database.GetCollection<BsonDocument>("category");

            PipelineDefinition<BsonDocument, BsonDocument> filter = new BsonDocument[]
            {
                new BsonDocument("$project", new BsonDocument()
                        .Add("_id", 0)
                        .Add("category", "$$ROOT")),
                new BsonDocument("$lookup", new BsonDocument()
                        .Add("localField", "category.category_id")
                        .Add("from", "film_category")
                        .Add("foreignField", "category_id")
                        .Add("as", "film_category")),
                new BsonDocument("$unwind", new BsonDocument()
                        .Add("path", "$film_category")
                        .Add("preserveNullAndEmptyArrays", new BsonBoolean(false))),
                new BsonDocument("$lookup", new BsonDocument()
                        .Add("localField", "film_category.film_id")
                        .Add("from", "inventory")
                        .Add("foreignField", "film_id")
                        .Add("as", "inventory")),
                new BsonDocument("$unwind", new BsonDocument()
                        .Add("path", "$inventory")
                        .Add("preserveNullAndEmptyArrays", new BsonBoolean(false))),
                new BsonDocument("$lookup", new BsonDocument()
                        .Add("localField", "inventory.inventory_id")
                        .Add("from", "rental")
                        .Add("foreignField", "inventory_id")
                        .Add("as", "rental")),
                new BsonDocument("$unwind", new BsonDocument()
                        .Add("path", "$rental")
                        .Add("preserveNullAndEmptyArrays", new BsonBoolean(false))),
                new BsonDocument("$group", new BsonDocument()
                        .Add("_id", new BsonDocument()
                                .Add("category\u1390name", "$category.name")
                        )
                        .Add("count", new BsonDocument()
                                .Add("$sum", 1)
                        )),
                new BsonDocument("$project", new BsonDocument()
                        .Add("category.name", "$_id.category\u1390name")
                        .Add("count", "$count")
                        .Add("_id", 0)),
                new BsonDocument("$sort", new BsonDocument()
                        .Add("count", -1)),
                new BsonDocument("$limit", 3)
            };


            return PerformAggregationAsync(categoryCollection, filter);
        }
    }
}

