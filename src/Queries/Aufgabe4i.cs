using System;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using PostgreToMongo.Options;

namespace PostgreToMongo.Queries
{
    public class Aufgabe4i : Query
    {
        private static readonly string call = @"PipelineDefinition<BsonDocument, BsonDocument> pipeline = new BsonDocument[]
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

        public Aufgabe4i(ILogger<Query> logger,
            IOptions<MongoSettings> mongoSettings)
            : base(call, "Aufgabe 4i", logger, mongoSettings)
        {
        }

        public async override Task RunAsync()
        {
            PipelineDefinition<BsonDocument, BsonDocument> pipeline = new BsonDocument[]
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
            await Database.CreateViewAsync("customer_list", "customer", pipeline);


            IMongoCollection<BsonDocument> collection = Database.GetCollection<BsonDocument>("customer_list");
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
            }
        }
    }
}

