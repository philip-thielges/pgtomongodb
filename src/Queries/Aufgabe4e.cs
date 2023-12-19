using System;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using PostgreToMongo.Options;

namespace PostgreToMongo.Queries
{
    public class Aufgabe4e : QueryBuilder
    {
        private static readonly string customCall = @"var customerCollection = Database.GetCollection<BsonDocument>(""customer"");

            PipelineDefinition<BsonDocument, BsonDocument> pipeline = new BsonDocument[]
            {
                new BsonDocument(""$project"", new BsonDocument()
                        .Add(""_id"", 0)
                        .Add(""customer"", ""$$ROOT"")),
                new BsonDocument(""$lookup"", new BsonDocument()
                        .Add(""localField"", ""customer.customer_id"")
                        .Add(""from"", ""rental"")
                        .Add(""foreignField"", ""customer_id"")
                        .Add(""as"", ""rental"")),
                new BsonDocument(""$unwind"", new BsonDocument()
                        .Add(""path"", ""$rental"")
                        .Add(""preserveNullAndEmptyArrays"", new BsonBoolean(false))),
                new BsonDocument(""$group"", new BsonDocument()
                        .Add(""_id"", new BsonDocument()
                                .Add(""customer\u1390customer_id"", ""$customer.customer_id"")
                        )
                        .Add(""COUNT(*)"", new BsonDocument()
                                .Add(""$sum"", 1)
                        )),
                new BsonDocument(""$project"", new BsonDocument()
                        .Add(""customer.customer_id"", ""$_id.customer\u1390customer_id"")
                        .Add(""COUNT(*)"", ""$COUNT(*)"")
                        .Add(""_id"", 0)),
                new BsonDocument(""$sort"", new BsonDocument()
                        .Add(""COUNT(*)"", -1)),
                new BsonDocument(""$limit"", 10)
            };";

        public Aufgabe4e(ILogger<QueryBuilder> logger,
            IOptions<MongoSettings> mongoSettings)
            : base(customCall, "Aufgabe 4e): Die IDs der 10 Kunden mit den meisten Entleihungen", logger, mongoSettings)
        {
        }

        public override Task ExecuteAsync()
        {
                //Use GetCollection Method to get the customer Collection
            var customerCollection = Database.GetCollection<BsonDocument>("customer");

            PipelineDefinition<BsonDocument, BsonDocument> filter = new BsonDocument[]
            {
                new BsonDocument("$project", new BsonDocument()
                        .Add("_id", 0)
                        .Add("customer", "$$ROOT")),
                new BsonDocument("$lookup", new BsonDocument()
                        .Add("localField", "customer.customer_id")
                        .Add("from", "rental")
                        .Add("foreignField", "customer_id")
                        .Add("as", "rental")),
                new BsonDocument("$unwind", new BsonDocument()
                        .Add("path", "$rental")
                        .Add("preserveNullAndEmptyArrays", new BsonBoolean(false))),
                new BsonDocument("$group", new BsonDocument()
                        .Add("_id", new BsonDocument()
                                .Add("customer\u1390customer_id", "$customer.customer_id")
                        )
                        .Add("count", new BsonDocument()
                                .Add("$sum", 1)
                        )),
                new BsonDocument("$project", new BsonDocument()
                        .Add("customer.customer_id", "$_id.customer\u1390customer_id")
                        .Add("count", "$count")
                        .Add("_id", 0)),
                new BsonDocument("$sort", new BsonDocument()
                        .Add("count", -1)),
                new BsonDocument("$limit", 10)
            };

            return PerformAggregationAsync(customerCollection, filter);
        }
    }
}

