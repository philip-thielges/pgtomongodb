using System;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using PostgreToMongo.Options;

namespace PostgreToMongo.Queries
{
    public class Aufgabe4h : Query
    {
        private static readonly string call = @"var collection = Database.GetCollection<BsonDocument>(""category"");

            PipelineDefinition<BsonDocument, BsonDocument> pipeline = new BsonDocument[]
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
                        .Add(""COUNT(*)"", new BsonDocument()
                                .Add(""$sum"", 1)
                        )),
                new BsonDocument(""$project"", new BsonDocument()
                        .Add(""category.name"", ""$_id.category\u1390name"")
                        .Add(""COUNT(*)"", ""$COUNT(*)"")
                        .Add(""_id"", 0)),
                new BsonDocument(""$sort"", new BsonDocument()
                        .Add(""COUNT(*)"", -1)),
                new BsonDocument(""$limit"", 3)
            };";
        public Aufgabe4h(ILogger<Query> logger,
            IOptions<MongoSettings> mongoSettings)
            : base(call, "Aufgabe 4h", logger, mongoSettings)
        {
        }

        public override Task RunAsync()
        {
            var collection = Database.GetCollection<BsonDocument>("category");

            PipelineDefinition<BsonDocument, BsonDocument> pipeline = new BsonDocument[]
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
                        .Add("COUNT(*)", new BsonDocument()
                                .Add("$sum", 1)
                        )),
                new BsonDocument("$project", new BsonDocument()
                        .Add("category.name", "$_id.category\u1390name")
                        .Add("COUNT(*)", "$COUNT(*)")
                        .Add("_id", 0)),
                new BsonDocument("$sort", new BsonDocument()
                        .Add("COUNT(*)", -1)),
                new BsonDocument("$limit", 3)
            };


            return AggregateAsync(collection, pipeline);
        }
    }
}

