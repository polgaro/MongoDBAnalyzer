using MongoDB.Bson;
using System;
using System.Collections.Generic;
using System.Text;

namespace MongoDBAnalyzer.Core
{
    public class ProfileData
    {
        public string op;
        public string ns;
        public BsonDocument command;
        public long cursorid;
        public int keysExamined;
        public int docsExamined;
        public int numYield;
        public int nreturned;

        public int nMatched;
        public int nModified;
        public int keysInserted;
        public int keysDeleted;

        public string queryHash;
        public string planCacheKey;
        public BsonDocument locks;
        public BsonDocument storage;
        public int responseLength;
        public string protocol;
        public int millis;
        public string planSummary;
        public BsonDocument execStats;
        public DateTime? ts;
        public string client;
        public string appName;
        public string[] allUsers;
        public string user;
        public bool hasSortStage;
        public bool cursorExhausted;

        public override string ToString()
        {
            return $"Plan: {planSummary} {command.ToString()}";
        }
    }
}
