using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using FluentCassandra.Types;
using FluentCassandra.Connections;
using Apache.Cassandra;

namespace FluentCassandra.StressTest
{
	internal class Program
	{
		private static int count = 10000;
		private static int dataLength = 1024;
		private static int threadCount = 8;
		private static string keyspaceName = "Blog";
		private static Server server = new Server("localhost");

		private static void SendDebugToConsole()
		{
			// Disable Debug traces
			Trace.Listeners.Clear();

			// Disable Debug assert message boxes
			using (DefaultTraceListener listener = new DefaultTraceListener())
			{
				listener.AssertUiEnabled = false;
				Trace.Listeners.Add(listener);
			}

			// Restore Debug traces to NUnit's Console.Out tab.
			Trace.Listeners.Add(new TextWriterTraceListener(Console.Out));
		}

		private static void SetupKeyspace()
		{
			if (!CassandraSession.KeyspaceExists(server, keyspaceName))
				CassandraSession.AddKeyspace(server, new KsDef {
					Name = keyspaceName,
					Replication_factor = 1,
					Strategy_class = "org.apache.cassandra.locator.SimpleStrategy",
					Cf_defs = new List<CfDef>()
				});

			var keyspace = new CassandraKeyspace(keyspaceName);

			if (!keyspace.ColumnFamilyExists(server, "Posts"))
				keyspace.AddColumnFamily(server, new CfDef {
					Name = "Posts",
					Keyspace = keyspaceName,
					Column_type = "Super",
					Comparator_type = "UTF8Type",
					Subcomparator_type = "UTF8Type",
					Comment = "Used for blog posts."
				});
		}

		private static void Main(string[] args)
		{
			SendDebugToConsole();
			SetupKeyspace();

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
			using (var db = new CassandraContext(keyspace: keyspaceName, server: server))
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

					post[DateTime.Now] = details;
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