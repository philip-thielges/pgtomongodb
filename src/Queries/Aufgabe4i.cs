using System;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using PostgreToMongo.Options;

namespace PostgreToMongo.Queries
{
    public class Aufgabe4i : QueryBuilder
    {
        private static readonly string customCall = @"PipelineDefinition<BsonDocument, BsonDocument> filter = new BsonDocument[]
            {
                new BsonDocument(""$project"", new BsonDocument()
                        .Add(""_id"", 0.0)
                        .Add(""customer"", ""$$ROOT"")),
                new BsonDocument(""$lookup"", new BsonDocument()
                        .Add(""localField"", ""customer.address_id"")
                        .Add(""from"", ""address"")
                        .Add(""foreignField"", ""address_id"")
                        .Add(""as"", ""address"")),
                new BsonDocument(""$unwind"", new BsonDocument()
                        .Add(""path"", ""$address"")
                        .Add(""preserveNullAndEmptyArrays"", new BsonBoolean(false))),
                new BsonDocument(""$lookup"", new BsonDocument()
                        .Add(""localField"", ""address.city_id"")
                        .Add(""from"", ""city"")
                        .Add(""foreignField"", ""city_id"")
                        .Add(""as"", ""city"")),
                new BsonDocument(""$unwind"", new BsonDocument()
                        .Add(""path"", ""$city"")
                        .Add(""preserveNullAndEmptyArrays"", new BsonBoolean(false))),
                new BsonDocument(""$lookup"", new BsonDocument()
                        .Add(""localField"", ""city.country_id"")
                        .Add(""from"", ""country"")
                        .Add(""foreignField"", ""country_id"")
                        .Add(""as"", ""country"")),
                new BsonDocument(""$unwind"", new BsonDocument()
                        .Add(""path"", ""$country"")
                        .Add(""preserveNullAndEmptyArrays"", new BsonBoolean(false))),
                new BsonDocument(""$project"", new BsonDocument()
                        .Add(""id"", ""$customer.customer_id"")
                        .Add(""name"", new BsonDocument()
                                .Add(""$concat"", new BsonArray()
                                        .Add(""$customer.first_name"")
                                        .Add("" "")
                                        .Add(""$customer.last_name"")
                                )
                        )
                        .Add(""address"", ""$address.address"")
                        .Add(""zip code"", ""$address.postal_code"")
                        .Add(""phone"", ""$address.phone"")
                        .Add(""city"", ""$city.city"")
                        .Add(""country"", ""$country.country"")
                        .Add(""notes"", new BsonDocument()
                                .Add(""$cond"", new BsonDocument()
                                        .Add(""if"", ""$customer.activebool"")
                                        .Add(""then"", ""active"")
                                        .Add(""else"", """")
                                )
                        )
                        .Add(""sid"", ""$customer.store_id""))
            };
            Database.DropCollection(""customer_list"");
            await Database.CreateViewAsync(""customer_list"", ""customer"", pipeline);

            IMongoCollection<BsonDocument> collection = Database.GetCollection<BsonDocument>(""customer_list"");
            var options = new FindOptions<BsonDocument>()
            {
                Limit = 10
            };

            using (var cursor = await collection.FindAsync(new BsonDocument(), options))
            {
                while (await cursor.MoveNextAsync())
                {
                    var batch = cursor.Current;
                    Results = batch.Select(x => x.ToJson()).ToList();

                }
            }";

        public Aufgabe4i(ILogger<QueryBuilder> logger,
            IOptions<MongoSettings> mongoSettings)
            : base(customCall, "Aufgabe 4i): Eine Sicht auf die Kunden mit allen relevanten Informationen wie im View „customer_list“ der vorhandenen Postgres-Datenbank", logger, mongoSettings)
        {
        }

        public async override Task ExecuteAsync()
        {
            PipelineDefinition<BsonDocument, BsonDocument> filter = new BsonDocument[]
            {
                new BsonDocument("$project", new BsonDocument()
                        .Add("_id", 0.0)
                        .Add("customer", "$$ROOT")),
                new BsonDocument("$lookup", new BsonDocument()
                        .Add("localField", "customer.address_id")
                        .Add("from", "address")
                        .Add("foreignField", "address_id")
                        .Add("as", "address")),
                new BsonDocument("$unwind", new BsonDocument()
                        .Add("path", "$address")
                        .Add("preserveNullAndEmptyArrays", new BsonBoolean(false))),
                new BsonDocument("$lookup", new BsonDocument()
                        .Add("localField", "address.city_id")
                        .Add("from", "city")
                        .Add("foreignField", "city_id")
                        .Add("as", "city")),
                new BsonDocument("$unwind", new BsonDocument()
                        .Add("path", "$city")
                        .Add("preserveNullAndEmptyArrays", new BsonBoolean(false))),
                new BsonDocument("$lookup", new BsonDocument()
                        .Add("localField", "city.country_id")
                        .Add("from", "country")
                        .Add("foreignField", "country_id")
                        .Add("as", "country")),
                new BsonDocument("$unwind", new BsonDocument()
                        .Add("path", "$country")
                        .Add("preserveNullAndEmptyArrays", new BsonBoolean(false))),
                new BsonDocument("$project", new BsonDocument()
                        .Add("id", "$customer.customer_id")
                        .Add("name", new BsonDocument()
                                .Add("$concat", new BsonArray()
                                        .Add("$customer.first_name")
                                        .Add(" ")
                                        .Add("$customer.last_name")
                                )
                        )
                        .Add("address", "$address.address")
                        .Add("zip code", "$address.postal_code")
                        .Add("phone", "$address.phone")
                        .Add("city", "$city.city")
                        .Add("country", "$country.country")
                        .Add("notes", new BsonDocument()
                                .Add("$cond", new BsonDocument()
                                        .Add("if", "$customer.activebool")
                                        .Add("then", "active")
                                        .Add("else", "")
                                )
                        )
                        .Add("sid", "$customer.store_id"))
            };
            Database.DropCollection("customer_list");
            await Database.CreateViewAsync("customer_list", "customer", filter);

                //Use GetCollection Method to get the customer_list Collection
            IMongoCollection<BsonDocument> customer_list_Collection = Database.GetCollection<BsonDocument>("customer_list");
            var options = new FindOptions<BsonDocument>()
            {
                Limit = 10
            };

            using (var cursor = await customer_list_Collection.FindAsync(new BsonDocument(), options))
            {
                while (await cursor.MoveNextAsync())
                {
                    var batch = cursor.Current;
                    QueryResults = batch.Select(x => x.ToJson()).ToList();

                }
            }
        }
    }
}

