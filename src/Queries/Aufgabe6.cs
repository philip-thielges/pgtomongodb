﻿using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using PostgreToMongo.Options;

namespace PostgreToMongo.Queries;

public class Aufgabe6 : QueryBuilder
{
    private static readonly string customCall = @"// Es werden die Filme unter 60 min aus  der “film” Tabelle gelöscht, zudem werden die Entleihungen für diese Filme gelöscht. Aus dem Inventar oder an anderer Stelle werden die Filme nicht gelöscht da nicht explizit gefordert.
            // Aufgabe b
            var pipeline = new BsonDocument[]
            {
                new BsonDocument(""$project"", new BsonDocument()
                        .Add(""_id"", 0)
                        .Add(""rental"", ""$$ROOT"")),
                new BsonDocument(""$lookup"", new BsonDocument()
                        .Add(""localField"", ""rental.inventory_id"")
                        .Add(""from"", ""inventory"")
                        .Add(""foreignField"", ""inventory_id"")
                        .Add(""as"", ""inventory"")),
                new BsonDocument(""$unwind"", new BsonDocument()
                        .Add(""path"", ""$inventory"")
                        .Add(""preserveNullAndEmptyArrays"", new BsonBoolean(false))),
                new BsonDocument(""$lookup"", new BsonDocument()
                        .Add(""localField"", ""inventory.film_id"")
                        .Add(""from"", ""film"")
                        .Add(""foreignField"", ""film_id"")
                        .Add(""as"", ""film"")),
                new BsonDocument(""$unwind"", new BsonDocument()
                        .Add(""path"", ""$film"")
                        .Add(""preserveNullAndEmptyArrays"", new BsonBoolean(false))),
                new BsonDocument(""$match"", new BsonDocument()
                        .Add(""film.length"", new BsonDocument()
                                .Add(""$lt"", new BsonInt64(60L))
                        )),
                new BsonDocument(""$project"", new BsonDocument()
                        .Add(""rental_id"", ""$rental.rental_id"")
                        .Add(""_id"", 0))
            };

            var rentalCollection = Database.GetCollection<BsonDocument>(""rental"");

            var options = new AggregateOptions()
            {
                AllowDiskUse = true
            };

            BsonArray ids = new BsonArray();
            using (var cursor = await rentalCollection.AggregateAsync<BsonDocument>(pipeline, options))
            {
                while (await cursor.MoveNextAsync())
                {
                    var batch = cursor.Current;
                    foreach (var item in batch)
                    {
                        rentalCollection.DeleteOne(item);
                    }

                }
            }

            // Aufgabe a
            var collection = Database.GetCollection<BsonDocument>(""film"");
            await collection.DeleteManyAsync(""{ length: { $lt: 60 } }"");";

    public Aufgabe6(ILogger<QueryBuilder> logger,
        IOptions<MongoSettings> mongoSettings)
        : base(customCall, "Aufgabe 6): Löscht die folgende Daten: a. Alle Filme, die weniger als 60 Minuten Spielzeit haben b. Alle damit zusammenhängenden Entleihungen", logger, mongoSettings)
    {
    }

    public async override Task ExecuteAsync()
    {
        // Aufgabe b
        var filter = new BsonDocument[]
        {
            new BsonDocument("$project", new BsonDocument()
                    .Add("_id", 0)
                    .Add("rental", "$$ROOT")),
            new BsonDocument("$lookup", new BsonDocument()
                    .Add("localField", "rental.inventory_id")
                    .Add("from", "inventory")
                    .Add("foreignField", "inventory_id")
                    .Add("as", "inventory")),
            new BsonDocument("$unwind", new BsonDocument()
                    .Add("path", "$inventory")
                    .Add("preserveNullAndEmptyArrays", new BsonBoolean(false))),
            new BsonDocument("$lookup", new BsonDocument()
                    .Add("localField", "inventory.film_id")
                    .Add("from", "film")
                    .Add("foreignField", "film_id")
                    .Add("as", "film")),
            new BsonDocument("$unwind", new BsonDocument()
                    .Add("path", "$film")
                    .Add("preserveNullAndEmptyArrays", new BsonBoolean(false))),
            new BsonDocument("$match", new BsonDocument()
                    .Add("film.length", new BsonDocument()
                            .Add("$lt", new BsonInt64(60L))
                    )),
            new BsonDocument("$project", new BsonDocument()
                    .Add("rental_id", "$rental.rental_id")
                    .Add("_id", 0))
        };
        //Use GetCollection Method to get the rental Collection
        var rentalCollection = Database.GetCollection<BsonDocument>("rental");

        var options = new AggregateOptions()
        {
            AllowDiskUse = true
        };

        BsonArray ids = new BsonArray();
        using (var cursor = await rentalCollection.AggregateAsync<BsonDocument>(filter, options))
        {
            while (await cursor.MoveNextAsync())
            {
                var batch = cursor.Current;
                foreach (var item in batch)
                {
                    rentalCollection.DeleteOne(item);
                }

            }
        }

        // Aufgabe a
        //Use GetCollection Method to get the film Collection
        var filmCollection = Database.GetCollection<BsonDocument>("film");
        await filmCollection.DeleteManyAsync("{ length: { $lt: 60 } }");

    }
}

