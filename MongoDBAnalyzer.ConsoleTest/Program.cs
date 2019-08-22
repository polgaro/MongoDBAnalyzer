using MongoDB.Bson;
using MongoDBAnalyzer.Core;
using System;
using System.Linq;

namespace MongoDBAnalyzer.ConsoleTest
{
    class Program
    {
        static void Main(string[] args)
        {
            using (var c = new MongoDBProfiler(QueryExecuted))
            {
                Console.ReadLine();
            }
        }

        private static void QueryExecuted(ProfileData queryInfo)
        {
            Console.WriteLine(queryInfo.ToString());
        }
    }
}
