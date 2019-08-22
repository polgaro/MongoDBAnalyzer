using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Driver;
using MongoDB.Driver.Core.Events;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MongoDBAnalyzer.Core
{
    public class MongoDBProfiler: IDisposable
    {
        #region Declarations
        enum ProfilingLevels
        {
            None = 0,
            Max = 1,
            All = 2
        }

        public delegate void QueryExecutedDelegate(ProfileData queryInfo);

        public class MongoDBProfilerConfiguration
        {
            public QueryExecutedDelegate Callback { get; set; }
            public string ConnStr { get; set; }
            public string DatabaseName { get; set; }
            public int TimeBetweenTries { get; set; } = 100;
            public int MaximumRecordsPerTrip { get; set; } = 500;
            public int MinTime { get; set; } = 100;
        }
        #endregion Declarations

        #region Fields
        static object objStartLock = new object();
        private IMongoDatabase mongoDatabase;
        private MongoClient client;
        private DateTime? lastRecordDT = null;
        private MongoDBProfilerConfiguration configuration;

        #region fields for threading
        private Thread receiverThread;
        private Thread delegateExecuterThread;
        private Thread mainThread;
        private ConcurrentQueue<ProfileData> dataQueue = new ConcurrentQueue<ProfileData>();
        private object objLock = new object();
        #endregion fields for threading


        #endregion Fields

        #region Constructors
        /// <summary>
        /// This constructor will use the connection string and database from within the app.config
        /// Looking for "ConnStr". Default is mongodb://localhost:27017
        /// Looking for "DbName". Default is MDSLogging
        /// </summary>
        public MongoDBProfiler(QueryExecutedDelegate queryExecuted) : 
            this(new MongoDBProfilerConfiguration { Callback = queryExecuted })
        {
            
        }

        /// <summary>
        /// Constructor to create a connector with a given connection string.
        /// </summary>
        /// <param name="connStr">Connection string to mongodb</param>
        public MongoDBProfiler(string connStr, string databaseName, QueryExecutedDelegate queryExecuted) : 
            this(new MongoDBProfilerConfiguration { ConnStr = connStr, DatabaseName = databaseName, Callback = queryExecuted })
        {
        }

        /// <summary>
        /// Constructor to create a connector with a given connection string.
        /// </summary>
        /// <param name="configuration">Parameters to connect and profile mongoDB </param>
        public MongoDBProfiler(MongoDBProfilerConfiguration configuration)
        {
            this.configuration = configuration;

            if (string.IsNullOrEmpty(configuration.ConnStr))
                configuration.ConnStr = GetConnStr();

            if (string.IsNullOrEmpty(configuration.DatabaseName))
                configuration.DatabaseName = GetDatabaseName();

            //create settings from connStr
            MongoClientSettings settings = MongoClientSettings.FromConnectionString(configuration.ConnStr);


            //create client
            client = new MongoClient(settings);

            //select database
            mongoDatabase = client.GetDatabase(configuration.DatabaseName);

            //initialize indexes if they don't exist
            InitializeIndexes();

            //drop and re-create the profiling tables
            InitializeProfiling();

            mainThread = Thread.CurrentThread;

            receiverThread = new Thread(TryReceive);
            delegateExecuterThread = new Thread(TryDequeue);

            receiverThread.Start();
            delegateExecuterThread.Start();
        }
        #endregion

        #region Initialization
        private void InitializeIndexes()
        {
            //if (!ProfileCollection.Indexes.List().Any())
            //{
            //    //TODO: LOCK LOCK LOCK
            //    ProfileCollection.Indexes.CreateOne(
            //            Builders<ProfileData>.IndexKeys.Ascending(x => x.ts)
            //        );
            //}
        }
        #endregion Initialization

        #region Queue and Dequeue
        private void TryReceive()
        {
            var baseQuery = ProfileCollectionQ;
            baseQuery = baseQuery.Where(x => x.ns != configuration.DatabaseName + ".system.profile").OrderBy(x => x.ts);

            while (true)
            {
                var query = baseQuery;
                if (lastRecordDT.HasValue)
                    query = query.Where(x => x.ts > lastRecordDT.Value);

                var profiles = query.Take(configuration.MaximumRecordsPerTrip);

                foreach (ProfileData profile in profiles)
                {

                    if (profile != null)
                    {
                        lastRecordDT = profile.ts;
                        dataQueue.Enqueue(profile);

                        lock (objLock)
                        {
                            Monitor.Pulse(objLock);
                        }
                    }
                }
                Thread.Sleep(configuration.TimeBetweenTries);
            }
        }

        private void TryDequeue()
        {
            while (true)
            {
                ProfileData profile;
                if (dataQueue.TryDequeue(out profile))
                {
                    configuration.Callback?.Invoke(profile);
                }
                else
                {
                    lock (objLock)
                    {
                        Monitor.Wait(objLock);
                    }
                }
            }
        }
        #endregion Queue and Dequeue

        #region Collections
        public IMongoCollection<ProfileData> ProfileCollection { get { return mongoDatabase.GetCollection<ProfileData>("system.profile"); } }
        public IQueryable<ProfileData> ProfileCollectionQ { get { return ProfileCollection.AsQueryable(); } }
        #endregion

        #region Profiling
        private void SetProfilingLevel(ProfilingLevels level, int minTime)
        {
            var command = new BsonDocumentCommand<BsonDocument>(
                    new BsonDocument
                        {
                            { "profile", (int)level },
                            { "slowms", minTime}
                        }
                    
                );
            var ret = mongoDatabase.RunCommand(command);
            int i = 0;
        }

        private int GetProfilingLevel()
        {

            BsonDocumentCommand<BsonDocument> command = new BsonDocumentCommand<BsonDocument>(new BsonDocument("profile", -1));
            BsonDocument response = mongoDatabase.RunCommand(command);
            return response.GetValue("was").AsInt32;

        }

        private void InitializeProfiling()
        {
            SetProfilingLevel(ProfilingLevels.None, configuration.MinTime);
            mongoDatabase.DropCollection("system.profile");
            mongoDatabase.CreateCollection("system.profile", new CreateCollectionOptions { Capped = true, MaxSize = 10000000 });
            SetProfilingLevel(ProfilingLevels.All, configuration.MinTime);
        }
        #endregion Profiling

        #region Private connStr methods
        private static string GetConnStr()
        {
            return ConfigurationManager.AppSettings["ConnStr"] ?? "mongodb://localhost:27017";
        }

        private static string GetDatabaseName()
        {
            return ConfigurationManager.AppSettings["DbName"] ?? "MDSLogging";
        }
        #endregion

        #region Cleanup
        public void Dispose()
        {

            receiverThread.Abort();
            delegateExecuterThread.Abort();

            SetProfilingLevel(ProfilingLevels.None, 100);

        }
        #endregion

    }
}
