using System;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using PostgreToMongo.Options;

namespace PostgreToMongo.Queries
{
    public class Aufgabe4f : QueryBuilder
    {
        private static readonly string customCall = @"var customerCollection = Database.GetCollection<BsonDocument>(""customer"");

            PipelineDefinition<BsonDocument, BsonDocument> filter = new BsonDocument[]
            {
                new BsonDocument(""$project"", new BsonDocument()
                        .Add(""_id"", 0)
                        .Add(""customer"", ""$$ROOT"")), 
                new BsonDocument(""$lookup"", new BsonDocument()
                        .Add(""localField"", ""customer.customer_id"")
                        .Add(""from"", ""payment"")
                        .Add(""foreignField"", ""customer_id"")
                        .Add(""as"", ""payment"")), 
                new BsonDocument(""$unwind"", new BsonDocument()
                        .Add(""path"", ""$payment"")
                        .Add(""preserveNullAndEmptyArrays"", new BsonBoolean(false))), 
                new BsonDocument(""$group"", new BsonDocument()
                        .Add(""_id"", new BsonDocument()
                                .Add(""customer\u1390customer_id"", ""$customer.customer_id"")
                                .Add(""customer\u1390last_name"", ""$customer.last_name"")
                                .Add(""customer\u1390first_name"", ""$customer.first_name"")
                                .Add(""customer\u1390store_id"", ""$customer.store_id"")
                        )
                        .Add(""SUM(payment\u1390amount)"", new BsonDocument()
                                .Add(""$sum"", ""$payment.amount"")
                        )), 
                new BsonDocument(""$project"", new BsonDocument()
                        .Add(""customer.last_name"", ""$_id.customer\u1390last_name"")
                        .Add(""customer.first_name"", ""$_id.customer\u1390first_name"")
                        .Add(""customer.store_id"", ""$_id.customer\u1390store_id"")
                        .Add(""_id"", 0)), 
                new BsonDocument(""$sort"", new BsonDocument()
                        .Add(""SUM(payment.amount)"", -1)), 
                new BsonDocument(""$project"", new BsonDocument()
                        .Add(""_id"", 0)
                        .Add(""customer.last_name"", ""$customer.last_name"")
                        .Add(""customer.first_name"", ""$customer.first_name"")
                        .Add(""customer.store_id"", ""$customer.store_id"")), 
                new BsonDocument(""$limit"", 10)
            };";
        public Aufgabe4f(ILogger<QueryBuilder> logger,
            IOptions<MongoSettings> mongoSettings)
            : base(customCall, "Aufgabe 4f): Die Vor- und Nachnamen sowie die Niederlassung der 10 Kunden, die das meiste Geld ausgegeben haben", logger, mongoSettings)
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
                        .Add("from", "payment")
                        .Add("foreignField", "customer_id")
                        .Add("as", "payment")),
                new BsonDocument("$unwind", new BsonDocument()
                        .Add("path", "$payment")
                        .Add("preserveNullAndEmptyArrays", new BsonBoolean(false))),
                new BsonDocument("$group", new BsonDocument()
                        .Add("_id", new BsonDocument()
                                .Add("customer\u1390customer_id", "$customer.customer_id")
                                .Add("customer\u1390last_name", "$customer.last_name")
                                .Add("customer\u1390first_name", "$customer.first_name")
                                .Add("customer\u1390store_id", "$customer.store_id")
                        )
                        .Add("SUM(payment\u1390amount)", new BsonDocument()
                                .Add("$sum", "$payment.amount")
                        )), new BsonDocument("$project", new BsonDocument()
                        .Add("customer.last_name", "$_id.customer\u1390last_name")
                        .Add("customer.first_name", "$_id.customer\u1390first_name")
                        .Add("customer.store_id", "$_id.customer\u1390store_id")
                        .Add("_id", 0)),
                new BsonDocument("$sort", new BsonDocument()
                        .Add("SUM(payment.amount)", -1)),
                new BsonDocument("$project", new BsonDocument()
                        .Add("_id", 0)
                        .Add("customer.last_name", "$customer.last_name")
                        .Add("customer.first_name", "$customer.first_name")
                        .Add("customer.store_id", "$customer.store_id")),
                new BsonDocument("$limit", 10)
            };

            return PerformAggregationAsync(customerCollection, filter);
        }
    }
}

