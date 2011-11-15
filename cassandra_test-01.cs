using System;
using System.Diagnostics;
using System.Threading.Tasks;
using FluentCassandra;
using FluentCassandra.Types;

namespace CassandraTest1
{
    class Program
    {
        private static int count = 10000;
        private static int dataLength = 1024;
        private static int threadCount = 8;

        static void Main(string[] args)
        {
            Task[] tasks = new Task[threadCount];

            Stopwatch watch = new Stopwatch();
            watch.Start();
            
            for (int i = 0; i < threadCount; i++)
            {
                tasks[i] = Task.Factory.StartNew(DoWork);
            }

            Task.WaitAll(tasks);
            watch.Stop();

            double rate = (count * threadCount) / watch.Elapsed.TotalSeconds;
            double throughput = rate * dataLength;
            Console.WriteLine("Total Completed: " + watch.Elapsed + "\tRate: " + rate + "\tThroughput: " + throughput);
            Console.ReadKey();
        }

        private static void DoWork()
        {
            using (var db = new CassandraContext("Blog", "localhost"))
            {
                Stopwatch watch = new Stopwatch();
                watch.Start();

                Random random = new Random();
                byte[] data = new byte[dataLength];
                random.NextBytes(data);
                int errors = 0;

                for (int i = 0; i < count; i++)
                {
                    // Insert
                    Guid postId = Guid.NewGuid();
                    string titleName = i.ToString();

                    var family = db.GetColumnFamily<UTF8Type, UTF8Type>("Posts");

                    var post = family.CreateRecord(postId);
                    dynamic details = post.CreateSuperColumn();

                    details.Body = data;

                    db.Attach(post);
                    db.SaveChanges();
                }

                double rate = count / watch.Elapsed.TotalSeconds;
                double throughput = rate * data.Length;
                Console.WriteLine("Completed: " + watch.Elapsed + "\tRate: " + rate + "\tThroughput: " + throughput + "\tErrors:" + errors);
            }
        }
    }
}