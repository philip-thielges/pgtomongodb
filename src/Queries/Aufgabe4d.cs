using System;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using PostgreToMongo.Options;

namespace PostgreToMongo.Queries
{
    public class Aufgabe4d:QueryBuilder
    {
        private static readonly string customCall = @"var staffCollection = Database.GetCollection<BsonDocument>(""staff"");

            PipelineDefinition<BsonDocument, BsonDocument> filter = new BsonDocument[]
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
                        )
                        .Add(""SUM(payment\u1390amount)"", new BsonDocument()
                                .Add(""$sum"", ""$payment.amount"")
                        )),
                new BsonDocument(""$project"", new BsonDocument()
                        .Add(""staff.staff_id"", ""$_id.staff\u1390staff_id"")
                        .Add(""sum"", ""$SUM(payment\u1390amount)"")
                        .Add(""_id"", 0))
            };";
        public Aufgabe4d(ILogger<QueryBuilder> logger,
            IOptions<MongoSettings> mongoSettings)
            : base(customCall, "Aufgabe 4d): Die Erlöse je Mitarbeiter", logger, mongoSettings)
        {
        }

        public override Task ExecuteAsync()
        {
                //Use GetCollection Method to get the staff Collection
            var staffCollection = Database.GetCollection<BsonDocument>("staff");

            PipelineDefinition<BsonDocument, BsonDocument> filter = new BsonDocument[]
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
                        )
                        .Add("SUM(payment\u1390amount)", new BsonDocument()
                                .Add("$sum", "$payment.amount")
                        )),
                new BsonDocument("$project", new BsonDocument()
                        .Add("staff.staff_id", "$_id.staff\u1390staff_id")
                        .Add("sum", "$SUM(payment\u1390amount)")
                        .Add("_id", 0))
            };

            return PerformAggregationAsync(staffCollection, filter);
        }
    }
}

