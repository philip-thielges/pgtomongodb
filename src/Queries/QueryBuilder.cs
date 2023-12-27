using System;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using PostgreToMongo.Options;

namespace PostgreToMongo.Queries
{
    /// <summary>
    /// This class has to be implmented for all excersices.
    /// And all the excersices will than be injected to the Dependency Injection.
    /// </summary>
    public abstract class QueryBuilder : IQueryBuilder
    {
        private readonly ILogger customLogger;
        private readonly string queryName, customCall;

        public QueryBuilder(string call,
            string name,
            ILogger logger,
            IOptions<MongoSettings> mongoSettings)
        {
            
            this.customCall = call;
            this.customLogger = logger;
            this.queryName = name;

            // create the mongo db client to access the DB server
            var mongoClient = new MongoClient(
                mongoSettings.Value.ConnectionString);
            // get the right databse to performe actions on
            Database = mongoClient.GetDatabase(
                mongoSettings.Value.DatabaseName);
        }

        public void PerformLogging()
        {
            customLogger.LogInformation(queryName);
            LogResults();
            LogQueryCall();
        }

        /// <summary>
        /// This Mehtod has to be implemented by each child class.
        /// A child class has to be created for each Excersice.
        /// </summary>
        /// <returns></returns>
        public abstract Task ExecuteAsync();

        /// <summary>
        /// Contains all the results for select statements only
        /// </summary>
        protected List<string> QueryResults { get; set; }
        /// <summary>
        /// Contains the DB specified in the appsettings.json
        /// Should be DVD.
        /// </summary>
        protected IMongoDatabase Database { get; set; }

        private void LogQueryCall() =>
            customLogger.LogInformation($"####################################\nVerwendeter Befehl:\n{customCall}");

        private void LogResults() =>
            customLogger.LogInformation($"####################################\nErgebnis:\n{string.Join("\n", QueryResults ?? Array.Empty<string>().ToList())}");

        /// <summary>
        /// Helper Method to execute and aggregation call.
        /// </summary>
        /// <param name="collection"></param>
        /// <param name="pipeline"></param>
        /// <param name="allowDiskUse"></param>
        /// <returns></returns>
        protected async Task PerformAggregationAsync(IMongoCollection<BsonDocument> collection, PipelineDefinition<BsonDocument, BsonDocument> filter, bool allowDiskUse = true)
        {
            var options = new AggregateOptions()
            {
                AllowDiskUse = allowDiskUse
            };

            using (var cursor = await collection.AggregateAsync(filter, options))
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

