using System;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using PostgreToMongo.Options;

namespace PostgreToMongo.Queries
{
    public class Aufgabe4d:Query
    {
        private static readonly string call = @"var collection = Database.GetCollection<BsonDocument>(""staff"");

            PipelineDefinition<BsonDocument, BsonDocument> pipeline = new BsonDocument[]
            {
                new BsonDocument(""$project"", new BsonDocument()
                        .Add(""_id"", 0)
                        .Add(""staff"", ""$$ROOT"")),
                new BsonDocument(""$lookup"", new BsonDocument()
                        .Add(""localField"", ""staff.staff_id"")
                        .Add(""from"", ""payment"")
                        .Add(""foreignField"", ""staff_id"")
                        .Add(""as"", ""payment"")),
                new BsonDocument(""$unwind"", new BsonDocument()
                        .Add(""path"", ""$payment"")
                        .Add(""preserveNullAndEmptyArrays"", new BsonBoolean(false))),
                new BsonDocument(""$group"", new BsonDocument()
                        .Add(""_id"", new BsonDocument()
                                .Add(""staff\u1390staff_id"", ""$staff.staff_id"")
                                .Add(""staff\u1390last_name"", ""$staff.last_name"")
                                .Add(""staff\u1390first_name"", ""$staff.first_name"")
                        )
                        .Add(""SUM(payment\u1390amount)"", new BsonDocument()
                                .Add(""$sum"", ""$payment.amount"")
                        )),
                new BsonDocument(""$project"", new BsonDocument()
                        .Add(""staff.staff_id"", ""$_id.staff\u1390staff_id"")
                        .Add(""staff.last_name"", ""$_id.staff\u1390last_name"")
                        .Add(""staff.first_name"", ""$_id.staff\u1390first_name"")
                        .Add(""SUM(payment\u1390amount)"", ""$SUM(payment\u1390amount)"")
                        .Add(""_id"", 0))
            };";
        public Aufgabe4d(ILogger<Query> logger,
            IOptions<MongoSettings> mongoSettings)
            : base(call, "Aufgabe 4d", logger, mongoSettings)
        {
        }

        public override Task RunAsync()
        {
            var collection = Database.GetCollection<BsonDocument>("staff");

            PipelineDefinition<BsonDocument, BsonDocument> pipeline = new BsonDocument[]
            {
                new BsonDocument("$project", new BsonDocument()
                        .Add("_id", 0)
                        .Add("staff", "$$ROOT")),
                new BsonDocument("$lookup", new BsonDocument()
                        .Add("localField", "staff.staff_id")
                        .Add("from", "payment")
                        .Add("foreignField", "staff_id")
                        .Add("as", "payment")),
                new BsonDocument("$unwind", new BsonDocument()
                        .Add("path", "$payment")
                        .Add("preserveNullAndEmptyArrays", new BsonBoolean(false))),
                new BsonDocument("$group", new BsonDocument()
                        .Add("_id", new BsonDocument()
                                .Add("staff\u1390staff_id", "$staff.staff_id")
                                .Add("staff\u1390last_name", "$staff.last_name")
                                .Add("staff\u1390first_name", "$staff.first_name")
                        )
                        .Add("SUM(payment\u1390amount)", new BsonDocument()
                                .Add("$sum", "$payment.amount")
                        )),
                new BsonDocument("$project", new BsonDocument()
                        .Add("staff.staff_id", "$_id.staff\u1390staff_id")
                        .Add("staff.last_name", "$_id.staff\u1390last_name")
                        .Add("staff.first_name", "$_id.staff\u1390first_name")
                        .Add("SUM(payment\u1390amount)", "$SUM(payment\u1390amount)")
                        .Add("_id", 0))
            };

            return AggregateAsync(collection, pipeline);
        }
    }
}

