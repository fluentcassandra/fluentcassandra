using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FluentCassandra.Connections;
using System.Threading;
using System.Globalization;
using Xunit;
using System.Diagnostics;

namespace FluentCassandra.StressTest
{
    /// <summary>
    /// The performance test on rapid insert/select/delete operations from mutlithreaded environment
    /// </summary>
    class StressTest2
    {
        public class CassandraJob
        {
            string command;
            string logic;
            Guid id;
            int n = 0;
            Guid ackId;

            public CassandraJob(FluentCassandra.Linq.ICqlRow job, Guid ackId)
            {
                command = job["command"];
                logic = job["logic"];
                id = job["id"];
                this.ackId = ackId;
            }

            public string GetCommand()
            {
                return command;
            }

            public string GetLogic()
            {
                return logic;
            }

            public Guid GetID()
            {
                return id;
            }

            public Guid GetAckID()
            {
                return ackId;
            }
        }
        private static string KeyspaceName = "stresstest2";
        private static readonly Server Server = new Server("localhost");

        [Fact]
        public static void Test()
        {
            int TestTimeInMinutes = 10;

            int ThreadCount = 50;
            int ThreadCount2 = 10;
            int TimeOut_Sec = 5;
            bool usePooling = true;

            object alive_monitor = new object();
            bool alive_condition = true;
            List<Thread> threads = new List<Thread>();

            int thrStarted_cnt = 0;
            object thrStarted_monitor = new object();

            //initialize
            CassandraContext main_db = new CassandraContext(new ConnectionBuilder(keyspace: KeyspaceName, server: Server, cqlVersion: CqlVersion.Cql3, pooling: usePooling));
            {
                if (main_db.KeyspaceExists(KeyspaceName))
                    main_db.DropKeyspace(KeyspaceName);

                var keyspace = new CassandraKeyspace(new CassandraKeyspaceSchema
                {
                    Name = KeyspaceName,
                }, main_db);

                keyspace.TryCreateSelf();

                CreateTablesIfNotExists(main_db);
            }

            for (int tI = 0; tI < ThreadCount; tI++)
            {
                int thrNo = tI;
                var thr = new Thread(() =>
                {
                    
                    Console.Write("(*" + thrNo + ")");
                    try
                    {
                        CassandraContext db = new CassandraContext(new ConnectionBuilder(keyspace: KeyspaceName, server: Server, cqlVersion: CqlVersion.Cql3, pooling: usePooling));
                        lock (thrStarted_monitor)
                        {
                            thrStarted_cnt++;
                            Monitor.PulseAll(thrStarted_monitor);
                        }

                        while (true)
                        {
                            var job = GetJob(db, 10);
                            if (job != null)
                            {
                                Console.Write("-");
                                if (job.GetLogic() == null || job.GetCommand() == null)
                                    Console.WriteLine("Error");
                                DeleteJob(db, job);
                            }
                            else
                            {
                                lock (alive_monitor)
                                {
                                    if (!alive_condition)
                                        return;
                                    else
                                        Monitor.Wait(alive_monitor, TimeOut_Sec * 1000);
                                }
                            }
                        }
                    }
                    finally
                    {
                        Console.Write("(~" + thrNo + ")");
                    }
                });
                threads.Add(thr);
                thr.Start();
            }

            for (int tI = 0; tI < ThreadCount2; tI++)
            {
                int thrNo = tI;
                var thr = new Thread(() =>
                {
                    Console.Write("<*" + thrNo + ">");
                    try
                    {
                        CassandraContext db = new CassandraContext(new ConnectionBuilder(keyspace: KeyspaceName, server: Server, cqlVersion: CqlVersion.Cql3, pooling: usePooling));
                        lock (thrStarted_monitor)
                        {
                            thrStarted_cnt++;
                            Monitor.PulseAll(thrStarted_monitor);
                        }

                        while (true)
                        {
                            StringBuilder sb = new StringBuilder();
                            for (int i = 0; i < 100; i++)
                                sb.AppendLine(Guid.NewGuid().ToString());
                            AddJob(db, sb.ToString(), Guid.NewGuid().ToString());
                            Console.Write("+");
                            Thread.Sleep(100);
                            lock (alive_monitor)
                            {
                                if (!alive_condition)
                                    return;
                            }
                        }
                    }
                    finally
                    {
                        Console.Write("<~" + thrNo + ">");
                    }
                });
                threads.Add(thr);
                thr.Start();
            }

            while (true)
            {
                lock (thrStarted_monitor)
                {
                    Monitor.Wait(thrStarted_monitor);
                    if (thrStarted_cnt == ThreadCount+ThreadCount2)
                        break;
                }
            }

            //wait for ten minutes
            Thread.Sleep(TestTimeInMinutes * 60 * 1000);

            lock (alive_monitor)
            {
                alive_condition = false;
                Monitor.PulseAll(alive_monitor);
            }
            foreach (var thr in threads)
            {
                thr.Join();
            }

            var j = main_db.ExecuteQuery(SelectCQL_Main(1)).ToArray();
            var c = main_db.ExecuteQuery(SelectCQL_Trans(1)).ToArray();
            if (j.Count() > 0 || c.Count() > 0)
                Console.WriteLine("Error");

            Console.WriteLine("Finished");
        }

           static ThreadLocal<Random> rnd = new ThreadLocal<Random>((() => new Random(Guid.NewGuid().GetHashCode())));

           public static void DeleteJob(CassandraContext db, CassandraJob job)
           {
               db.ExecuteNonQuery(DeleteRowCQL_Main((job as CassandraJob).GetID()));
               db.ExecuteNonQuery(DeleteRowCQL_Trans((job as CassandraJob).GetID()));
           }

           public static void AddJob(CassandraContext db, string logic, string command)
           {
               var id = Guid.NewGuid();
               db.ExecuteNonQuery(InsertRowCQL(id, logic, command));
           }
           
        public static CassandraJob GetJob(CassandraContext db, int hide_time_sec)
        {
        loop:
            dynamic job = null;
            for (int i = 0; i < 1000; i++)
            {
                var jobs = db.ExecuteQuery(SelectCQL_Main(1000)).ToArray();
                var mx = jobs.Length < 1000 ? jobs.Length - 1 : 1000;
                if (mx == -1)
                    break;
                job = jobs[rnd.Value.Next(mx)];
                if (job["hidden_till"] < ToUnixTime(DateTimeOffset.UtcNow))
                    break;
            }
            if (job != null)
            {
                var myID = Guid.NewGuid();
                var ret = new CassandraJob(job, myID);
                db.ExecuteNonQuery(HideRowCQL_Trans(ret.GetID(), ret.GetAckID()));
                var cnt = db.ExecuteQuery(SelectCQL_Trans(ret.GetID(), 1000)).ToArray();
                var counter = cnt.First()["count"];
                if (counter > 1)
                {
                    db.ExecuteNonQuery(DeleteRowCQL_Trans(ret.GetID()));
                    goto loop;
                }
                if (counter == 0)
                    goto loop;

                db.ExecuteNonQuery(HideRowCQL_Main2(ret.GetID(), hide_time_sec,ret.GetLogic(),ret.GetCommand()));

                return ret;
            }
            else
                return null;
        }

        public static void CreateTablesIfNotExists(CassandraContext db)
        {
            try
            {
                db.ExecuteNonQuery(string.Format(@"
CREATE TABLE {0} (
 id     uuid,
 logic text,
 command text,
 hidden_till timestamp,
 PRIMARY KEY (id));
", TableName("main")));
            }
            catch (CassandraException) { }
            try
            {

                db.ExecuteNonQuery(string.Format(@"
CREATE TABLE {0} (
 id     uuid,
 oid    uuid,
 progress double,
 last_access timestamp,
 PRIMARY KEY (id,oid));
", TableName("trans")));
            }
            catch (CassandraException) { }
            try
            {
                db.ExecuteNonQuery(string.Format(@"
CREATE TABLE {0} (
 id     uuid,
 oid    uuid,
 what   int,
 when   timestamp,
 info   text,
 PRIMARY KEY (id,oid,what,when));
", TableName("out")));
            }
            catch (CassandraException) { }


        }
        protected static string Encode(string str)
        {
            return '\'' + str.Replace("\'", "\'\'") + '\'';
        }

        protected void DeleteTables()
        {
            throw new NotImplementedException();
        }

        protected static string TableName(string pfx)
        {
            return "scheduler_" + pfx;
        }


        protected static readonly DateTimeOffset UnixStart = new DateTimeOffset(1970, 1, 1, 0, 0, 0, TimeSpan.Zero);

        protected static long ToUnixTime(DateTimeOffset dt)
        {
            // this was changed from .NET Ticks to the Unix Epoch to be compatible with other cassandra libraries
            return Convert.ToInt64(Math.Floor((dt - UnixStart).TotalMilliseconds));
        }


        protected static string InsertRowCQL(Guid id, string logic, string command)
        {
            return string.Format(@"
INSERT INTO {0} 
(id,logic,command, hidden_till) 
VALUES ({1},{2},{3},{4})",
            TableName("main"),
            id.ToString(),
            Encode(logic),
            Encode(command),
            0
            );
        }

        protected static string HideRowCQL_Main(Guid id, int hide_time_sec)
        {
            return string.Format(@"
UPDATE {0} 
SET hidden_till={1}
WHERE id={2}",
            TableName("main"),
            ToUnixTime(DateTimeOffset.UtcNow.AddSeconds(hide_time_sec)),
            id.ToString()
            );
        }

        protected static string HideRowCQL_Main2(Guid id, int hide_time_sec, string logic, string command)
        {
            return string.Format(@"
UPDATE {0} 
SET hidden_till={1}, logic={2}, command={3}
WHERE id={4}",
            TableName("main"),
            ToUnixTime(DateTimeOffset.UtcNow.AddSeconds(hide_time_sec)),
            Encode(logic),
            Encode(command),
            id.ToString()
            );
        }

        protected static string HideRowCQL_Trans(Guid id, Guid oid)
        {
            return string.Format(@"
INSERT INTO {0} 
(id,oid,progress,last_access) 
VALUES ({1},{2},0,0)",
            TableName("trans"),
            id.ToString(),
            oid.ToString()
            );
        }

        protected static string DeleteRowCQL_Main(Guid id)
        {
            return string.Format(@"
DELETE FROM {0} 
WHERE id={1}",
                TableName("main"),
            id.ToString()
            );
        }

        protected static string DeleteRowCQL_Trans(Guid id)
        {
            return string.Format(@"
DELETE FROM {0} 
WHERE id={1}",
                TableName("trans"),
            id.ToString()
            );
        }

        protected static string SelectCQL_Main(int limit)
        {
            return string.Format(@"
SELECT *
FROM {0} LIMIT {1}",
            TableName("main"), limit.ToString()
            );
        }
        protected static string SelectCQL_Trans(int limit)
        {
            return string.Format(@"
SELECT *
FROM {0} LIMIT {1}",
            TableName("trans"), limit.ToString()
            );
        }
        protected static string SelectCQL_Trans(Guid id, int limit)
        {
            return string.Format(@"
SELECT count(1)
FROM {0} WHERE id={1} LIMIT {2}",
            TableName("trans"), id.ToString(), limit.ToString()
            );
        }

        protected static string SetProgressRowCQL_Trans(Guid id, Guid oid, double percentDone)
        {
            return string.Format(@"
UPDATE {0} 
SET progress={1}
WHERE id={2} and oid = {3}",
            TableName("trans"),
            percentDone.ToString(CultureInfo.InvariantCulture),
            id.ToString(),
            oid.ToString()
            );
        }

        protected static string AddInfoRowCQL_Out(Guid id, Guid oid, int what, string info)
        {
            return string.Format(@"
INSERT INTO {0} 
 (id,oid,what,when,info)
VALUES ({1},{2},{3},{4},{5});
", TableName("out"), id.ToString(), oid.ToString(), what.ToString(), ToUnixTime(DateTimeOffset.UtcNow), Encode(info));
        }
    }
}
