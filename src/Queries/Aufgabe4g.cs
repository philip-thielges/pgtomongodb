using System;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using PostgreToMongo.Options;

namespace PostgreToMongo.Queries
{
    public class Aufgabe4g : QueryBuilder
    {
        private static readonly string customCall = @"var filmCollection = Database.GetCollection<BsonDocument>(""film"");

            PipelineDefinition<BsonDocument, BsonDocument> filter = new BsonDocument[]
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
                        .Add(""count"", new BsonDocument()
                                .Add(""$sum"", 1)
                        )),
                new BsonDocument(""$project"", new BsonDocument()
                        .Add(""film.title"", ""$_id.film\u1390title"")
                        .Add(""count"", ""$count"")
                        .Add(""_id"", 0)),
                new BsonDocument(""$sort"", new BsonDocument()
                        .Add(""count"", -1)),
                new BsonDocument(""$limit"", 10)
            };";
        public Aufgabe4g(ILogger<QueryBuilder> logger,
            IOptions<MongoSettings> mongoSettings)
            : base(customCall, "Aufgabe 4g): Die 10 meistgesehenen Filme unter Angabe des Titels, absteigend sortiert", logger, mongoSettings)
        {
        }

        public override Task ExecuteAsync()
        {
                //Use GetCollection Method to get the film Collection
            var filmCollection = Database.GetCollection<BsonDocument>("film");

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
                        .Add("count", new BsonDocument()
                                .Add("$sum", 1)
                        )),
                new BsonDocument("$project", new BsonDocument()
                        .Add("film.title", "$_id.film\u1390title")
                        .Add("count", "$count")
                        .Add("_id", 0)),
                new BsonDocument("$sort", new BsonDocument()
                        .Add("count", -1)),
                new BsonDocument("$limit", 10)
            };

            return PerformAggregationAsync(filmCollection, pipeline);
        }
    }
}

