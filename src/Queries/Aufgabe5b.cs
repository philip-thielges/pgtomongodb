using System;
using System.Net.NetworkInformation;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using PostgreToMongo.Options;

namespace PostgreToMongo.Queries
{
    public class Aufgabe5b : Query
    {
        private static readonly string call = @"// Der neuer Store mit der ID 3 erhält eine eindeutige Adress ID und das gesamte Inventar wird auf Store 3 aktualisiert. Die Entleihungen und Zuordnung der Kunden zu den Stores werden vorerst nicht angepasst, da die Entleihungen ja in den alten Stores stattgefunden haben und eine Aktualisierung hier irreführend wäre. Die neue Store ID soll dort (bei Kunden und Entleihungen) dann erst aktualisiert werden, wenn der Kunden das erste mal etwas im neuen Store 3 ausleiht.

            var storeCollection = Database.GetCollection<BsonDocument>(""store"");
            var addressCollection = Database.GetCollection<BsonDocument>(""address"");
            var inventoryCollection = Database.GetCollection<BsonDocument>(""inventory"");


            var getMaxAddressIdPipeline = new BsonDocument[]
            {
                new BsonDocument(""$group"", new BsonDocument()
                        .Add(""_id"", new BsonDocument())
                        .Add(""MAX(address_id)"", new BsonDocument()
                                .Add(""$max"", ""$address_id"")
                        )),
                new BsonDocument(""$project"", new BsonDocument()
                        .Add(""max_id"", ""$MAX(address_id)"")
                        .Add(""_id"", 0))
            };

            var getMaxStoreIdPipeline = new BsonDocument[]
            {
                new BsonDocument(""$group"", new BsonDocument()
                        .Add(""_id"", new BsonDocument())
                        .Add(""MAX(store_id)"", new BsonDocument()
                                .Add(""$max"", ""$store_id"")
                        )),
                new BsonDocument(""$project"", new BsonDocument()
                        .Add(""max_id"", ""$MAX(store_id)"")
                        .Add(""_id"", 0))
            };


            int maxAddressId = await GetMaxIdAsync(getMaxAddressIdPipeline, addressCollection);
            int maxStoreId = await GetMaxIdAsync(getMaxStoreIdPipeline, storeCollection);


            var newStore = new BsonDocument
            {
                {""store_id"", ++maxStoreId },
                {""manager_staff_id"", 1 },
                {""address_id"", ++maxAddressId },
                {""last_update"", DateTime.UtcNow },

            };

            var newAddress = new BsonDocument
            {
                {""address_id"", maxAddressId },
                {""address"", ""1 Was eine Straße"" },
                //{""address2"", null },
                {""district"", ""St. Pauli"" },
                {""city_id"", 1 },
                {""postal_code"", ""20359"" },
                {""phone"", ""0123456789"" },
                {""last_update"", DateTime.UtcNow },

            };


            await storeCollection.InsertOneAsync(newStore);
            await addressCollection.InsertOneAsync(newStore);

            await inventoryCollection.UpdateManyAsync(""{store_id : {$gt: 0}}"", ""{$set: { \""store_id\"" : "" + maxStoreId + ""}}"");";

        public Aufgabe5b(ILogger<Query> logger,
            IOptions<MongoSettings> mongoSettings)
            : base(call, "Aufgabe 5b", logger, mongoSettings)
        {
        }

        public async override Task RunAsync()
        {
            // Der neuer Store mit der ID 3 erhält eine eindeutige Adress ID und das gesamte Inventar wird auf Store 3 aktualisiert. Die Entleihungen und Zuordnung der Kunden zu den Stores werden vorerst nicht angepasst, da die Entleihungen ja in den alten Stores stattgefunden haben und eine Aktualisierung hier irreführend wäre. Die neue Store ID soll dort (bei Kunden und Entleihungen) dann erst aktualisiert werden, wenn der Kunden das erste mal etwas im neuen Store 3 ausleiht.
            var storeCollection = Database.GetCollection<BsonDocument>("store");
            var addressCollection = Database.GetCollection<BsonDocument>("address");
            var inventoryCollection = Database.GetCollection<BsonDocument>("inventory");


            var getMaxAddressIdPipeline = new BsonDocument[]
            {
                new BsonDocument("$group", new BsonDocument()
                        .Add("_id", new BsonDocument())
                        .Add("MAX(address_id)", new BsonDocument()
                                .Add("$max", "$address_id")
                        )),
                new BsonDocument("$project", new BsonDocument()
                        .Add("max_id", "$MAX(address_id)")
                        .Add("_id", 0))
            };

            var getMaxStoreIdPipeline = new BsonDocument[]
            {
                new BsonDocument("$group", new BsonDocument()
                        .Add("_id", new BsonDocument())
                        .Add("MAX(store_id)", new BsonDocument()
                                .Add("$max", "$store_id")
                        )),
                new BsonDocument("$project", new BsonDocument()
                        .Add("max_id", "$MAX(store_id)")
                        .Add("_id", 0))
            };


            int maxAddressId = await GetMaxIdAsync(getMaxAddressIdPipeline, addressCollection);
            int maxStoreId = await GetMaxIdAsync(getMaxStoreIdPipeline, storeCollection);


            var newStore = new BsonDocument
            {
                {"store_id", ++maxStoreId },
                {"manager_staff_id", 1 },
                {"address_id", ++maxAddressId },
                {"last_update", DateTime.UtcNow },

            };

            var newAddress = new BsonDocument
            {
                {"address_id", maxAddressId },
                {"address", "1 Was eine Straße" },
                //{"address2", null },
                {"district", "St. Pauli" },
                {"city_id", 1 },
                {"postal_code", "20359" },
                {"phone", "0123456789" },
                {"last_update", DateTime.UtcNow },

            };


            await storeCollection.InsertOneAsync(newStore);
            await addressCollection.InsertOneAsync(newStore);

            await inventoryCollection.UpdateManyAsync("{store_id : {$gt: 0}}", "{$set: { \"store_id\" : " + maxStoreId + "}}");

        }

        private async Task<int> GetMaxIdAsync(PipelineDefinition<BsonDocument, BsonDocument> pipeline, IMongoCollection<BsonDocument> collection)
        {
            var options = new AggregateOptions()
            {
                AllowDiskUse = true
            };

            using (var cursor = await collection.AggregateAsync<BsonDocument>(pipeline, options))
            {
                while (await cursor.MoveNextAsync())
                {
                    var batch = cursor.Current;
                    foreach (BsonDocument document in batch)
                    {
                        return (int)document.GetElement("max_id").Value;
                    }
                }
            }

            return -1;
        }
    }
}

