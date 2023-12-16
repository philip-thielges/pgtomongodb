using System;
using System.Xml.Linq;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using PostgreToMongo.Models;
using PostgreToMongo.Options;

namespace PostgreToMongo.Services
{
    /// <summary>
    /// This service handles evrything Mongo DB related, in terms of migration.
    /// </summary>
    public class MongoService : IMongoService
    {
        private readonly IMongoDatabase db;
        private readonly ILogger<MongoService> _logger;

        // get related Services via DI
        public MongoService(IOptions<MongoSettings> mongoSettings,
            ILogger<MongoService> logger)
        {
            // create mongo DB client to access the mongo db server
            var mongoClient = new MongoClient(
                // connection string set up can be find in the appsettings.json file.
                // should be mongodb://root:1234@mongo
                mongoSettings.Value.ConnectionString);
            // get access to the rigth database on the mongo server
            db = mongoClient.GetDatabase(
            // should be DVD
                mongoSettings.Value.DatabaseName);
            this._logger = logger;
        }

        public async Task AddTableAsync(TableDescription tableDescription)
        {
            try
            {
                // drop collection, so the migration can be run several times.
                await db.DropCollectionAsync(tableDescription.TableName);
                // re create the collection
                await db.CreateCollectionAsync(tableDescription.TableName);
                // get the collection, which just has been created, to add data.
                var collection = db.GetCollection<BsonDocument>(tableDescription.TableName);
                // create a new List of BSON documents, one BSOn document is one entry in the mongo collection, or a row in a database.
                var documents = new List<BsonDocument>();

                // for each row in the table, which we obtained from the postgreSQL DB, we will now insert into the collection.
                foreach (var row in tableDescription.Table)
                {
                    // create new document
                    // "row" is a dictionary, that means the structure is similar to a plain json or bson object, so it ca be give via constructor to create the BSON Document from
                    // one bson element consist of a key and a value, same is true for the dictionary. (key: val)
                    var document = new BsonDocument(row);
                    // add to the list of documents.
                    documents.Add(document);
                }

                // now add all documents at once to the collection. And one table has been migrated.
                await collection.InsertManyAsync(documents);
                _logger.LogInformation($"Added: {tableDescription.TableName}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
            }
        }
    }
}

