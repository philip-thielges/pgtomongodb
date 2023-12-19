using System;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using PostgreToMongo.Options;

namespace PostgreToMongo.Queries
{
    public class Aufgabe4c:QueryBuilder
    {
        private static readonly string customCall = @"var film_actor_Collection = Database.GetCollection<BsonDocument>(""film_actor"");

            PipelineDefinition<BsonDocument, BsonDocument> filter = new BsonDocument[]
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

        public Aufgabe4c(ILogger<QueryBuilder> logger,
            IOptions<MongoSettings> mongoSettings)
            :base(customCall, "Aufgabe 4c): Die Vor- und Nachnamen der 10 Schauspieler mit den meisten Filmen,absteigend sortiert", logger, mongoSettings)
        {
        }

        public override Task ExecuteAsync()
        {
                //Use GetCollection Method to get the film_actor Collection
            var film_actor_Collection = Database.GetCollection<BsonDocument>("film_actor");

            PipelineDefinition<BsonDocument, BsonDocument> filter = new BsonDocument[]
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


            return PerformAggregationAsync(film_actor_Collection, filter);
        }
    }
}

