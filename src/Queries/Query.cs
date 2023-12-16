using System;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using PostgreToMongo.Options;

namespace PostgreToMongo.Queries
{
    /// <summary>
    /// This class has to be implmented for all excersices.
    /// And all the excersices will than be injected to the DI.
    /// </summary>
    public abstract class Query : IQuery
    {
        private readonly ILogger logger;
        private readonly string name, call;

        public Query(string call,
            string name,
            ILogger logger,
            IOptions<MongoSettings> mongoSettings)
        {
            // the call as a string
            this.call = call;
            this.logger = logger;
            // name of the excersice as string
            this.name = name;

            // create the mongo db client to access the DB server
            var mongoClient = new MongoClient(
                mongoSettings.Value.ConnectionString);
            // get the right databse to performe actions on
            Database = mongoClient.GetDatabase(
                mongoSettings.Value.DatabaseName);
        }

        /// <summary>
        /// helper Metho to log the results, ercersice name and the call
        /// </summary>
        public void Log()
        {
            logger.LogInformation(name);
            logResults();
            logCall();
        }

        /// <summary>
        /// This Mehtod has to be implemented by each child class.
        /// A child class has to be created for each Excersice.
        /// </summary>
        /// <returns></returns>
        public abstract Task RunAsync();

        /// <summary>
        /// Contains all the results for select statements only
        /// </summary>
        protected List<string> Results { get; set; }
        /// <summary>
        /// Contains the DB specified in the appsettings.json
        /// Should be DVD.
        /// </summary>
        protected IMongoDatabase Database { get; set; }

        private void logCall() =>
            logger.LogInformation($"Used by making the following call:\n{call}");

        private void logResults() =>
            logger.LogInformation($"Ergebnisse:\n{string.Join("\n", Results ?? Array.Empty<string>().ToList())}");

        /// <summary>
        /// Helper Method to execute and aggregation call.
        /// </summary>
        /// <param name="collection"></param>
        /// <param name="pipeline"></param>
        /// <param name="allowDiskUse"></param>
        /// <returns></returns>
        protected async Task AggregateAsync(IMongoCollection<BsonDocument> collection, PipelineDefinition<BsonDocument, BsonDocument> pipeline, bool allowDiskUse = true)
        {
            var options = new AggregateOptions()
            {
                AllowDiskUse = allowDiskUse
            };

            using (var cursor = await collection.AggregateAsync(pipeline, options))
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

