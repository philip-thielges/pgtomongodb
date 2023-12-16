using System;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using PostgreToMongo.Options;

namespace PostgreToMongo.Queries
{
    public class Aufgabe4g : Query
    {
        private static readonly string call = @"var collection = Database.GetCollection<BsonDocument>(""film"");

            PipelineDefinition<BsonDocument, BsonDocument> pipeline = new BsonDocument[]
            {
                new BsonDocument(""$project"", new BsonDocument()
                        .Add(""_id"", 0)
                        .Add(""film"", ""$$ROOT"")),
                new BsonDocument(""$lookup"", new BsonDocument()
                        .Add(""localField"", ""film.film_id"")
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
                                .Add(""film\u1390title"", ""$film.title"")
                        )
                        .Add(""COUNT(*)"", new BsonDocument()
                                .Add(""$sum"", 1)
                        )),
                new BsonDocument(""$project"", new BsonDocument()
                        .Add(""film.title"", ""$_id.film\u1390title"")
                        .Add(""COUNT(*)"", ""$COUNT(*)"")
                        .Add(""_id"", 0)),
                new BsonDocument(""$sort"", new BsonDocument()
                        .Add(""COUNT(*)"", -1)),
                new BsonDocument(""$limit"", 10)
            };";
        public Aufgabe4g(ILogger<Query> logger,
            IOptions<MongoSettings> mongoSettings)
            : base(call, "Aufgabe 4g", logger, mongoSettings)
        {
        }

        public override Task RunAsync()
        {
            var collection = Database.GetCollection<BsonDocument>("film");

            PipelineDefinition<BsonDocument, BsonDocument> pipeline = new BsonDocument[]
            {
                new BsonDocument("$project", new BsonDocument()
                        .Add("_id", 0)
                        .Add("film", "$$ROOT")),
                new BsonDocument("$lookup", new BsonDocument()
                        .Add("localField", "film.film_id")
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
                                .Add("film\u1390title", "$film.title")
                        )
                        .Add("COUNT(*)", new BsonDocument()
                                .Add("$sum", 1)
                        )),
                new BsonDocument("$project", new BsonDocument()
                        .Add("film.title", "$_id.film\u1390title")
                        .Add("COUNT(*)", "$COUNT(*)")
                        .Add("_id", 0)),
                new BsonDocument("$sort", new BsonDocument()
                        .Add("COUNT(*)", -1)),
                new BsonDocument("$limit", 10)
            };

            return AggregateAsync(collection, pipeline);
        }
    }
}

