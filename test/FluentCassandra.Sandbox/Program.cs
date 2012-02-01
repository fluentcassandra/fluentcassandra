using System;
using System.Linq;
using System.Threading;
using FluentCassandra.Connections;
using FluentCassandra.Types;

namespace FluentCassandra.Sandbox
{
	internal class Program
	{
		private const string KeyspaceName = "Blog";
		private static readonly Server Server = new Server("localhost");

		#region Setup

		private static void SetupKeyspace()
		{
			using (var db = new CassandraContext(keyspace: KeyspaceName, server: Server))
			{

				if (db.KeyspaceExists(KeyspaceName))
					db.DropKeyspace(KeyspaceName);

				var keyspace = db.Keyspace;
				keyspace.TryCreateSelf();
				keyspace.TryCreateColumnFamily<UTF8Type>("Posts");
				keyspace.TryCreateColumnFamily<LongType>("Tags");
				keyspace.TryCreateColumnFamily<TimeUUIDType, UTF8Type>("Comments");
			}
		}

		#endregion

		#region Console Helpers

		private static void ConsoleHeader(string header)
		{
			Console.WriteLine(@"
************************************************
** " + header + @"
************************************************");
		}

		#endregion

		#region Create Post

		private static void CreatePost()
		{
			using (var db = new CassandraContext(keyspace: KeyspaceName, server: Server))
			{
				var key = "first-blog-post";

				var postFamily = db.GetColumnFamily<UTF8Type>("Posts");
				var tagsFamily = db.GetColumnFamily<LongType>("Tags");

				// create post
				ConsoleHeader("create post");
				dynamic post = postFamily.CreateRecord(key: key);
				post.Title = "My First Cassandra Post";
				post.Body = "Blah. Blah. Blah. about my first post on how great Cassandra is to work with.";
				post.Author = "Nick Berardi";
				post.PostedOn = DateTimeOffset.Now;

				// create tags
				ConsoleHeader("create post tags");
				dynamic tags = tagsFamily.CreateRecord(key: key);
				tags[0] = "Cassandra";
				tags[1] = ".NET";
				tags[2] = "Database";
				tags[3] = "NoSQL";

				// attach the post to the database
				ConsoleHeader("attaching record");
				db.Attach(post);
				db.Attach(tags);

				// save the changes
				ConsoleHeader("saving changes");
				db.SaveChanges();
			}
		}

		#endregion

		#region Read Post

		private static void ReadPost()
		{
			using (var db = new CassandraContext(keyspace: KeyspaceName, server: Server))
			{
				var key = "first-blog-post";

				var postFamily = db.GetColumnFamily<UTF8Type>("Posts");
				var tagsFamily = db.GetColumnFamily<LongType>("Tags");

				// get the post back from the database
				ConsoleHeader("getting 'first-blog-post'");
				dynamic post = postFamily.Get(key).FirstOrDefault();
				dynamic tags = tagsFamily.Get(key).FirstOrDefault();

				// show details
				ConsoleHeader("showing post");
				Console.WriteLine(
					String.Format("=={0} by {1}==\n{2}",
						post.Title,
						post.Author,
						post.Body
					));

				// show tags
				ConsoleHeader("showing tags");
				foreach (var tag in tags)
					Console.WriteLine(String.Format("{0}:{1},", (long)tag.ColumnName, tag.ColumnValue));
			}
		}

		#endregion

		#region Update Post
		private static void UpdatePost()
		{
			using (var db = new CassandraContext(keyspace: KeyspaceName, server: Server))
			{
				var key = "first-blog-post";

				var postFamily = db.GetColumnFamily<UTF8Type>("Posts");
				// get the post back from the database
				ConsoleHeader("getting 'first-blog-post' for update");
				dynamic post = postFamily.Get(key).FirstOrDefault();

				post.Title = post.Title + "(updated)";
				post.Body = post.Body + "(updated)";
				post.Author = post.Author + "(updated)";
				post.PostedOn = DateTimeOffset.Now;

				// attach the post to the database
				ConsoleHeader("attaching record");
				db.Attach(post);

				// save the changes
				ConsoleHeader("saving changes");
				db.SaveChanges();
			}
		}
		#endregion

		#region Create Comments

		private static void CreateComments()
		{
			using (var db = new CassandraContext(keyspace: KeyspaceName, server: Server))
			{
				var key = "first-blog-post";

				// get the comments family
				var commentsFamily = db.GetColumnFamily<TimeUUIDType, UTF8Type>("Comments");

				ConsoleHeader("create comments");
				dynamic postComments = commentsFamily.CreateRecord(key: key);

				// lets attach it to the database before we add the comments
				db.Attach(postComments);

				// add 5 comments
				for (int i = 0; i < 5; i++)
				{
					dynamic comment = postComments.CreateSuperColumn();
					comment.Name = "Nick Berardi";
					comment.Email = "nick@coderjournal.com";
					comment.Website = "www.coderjournal.com";
					comment.Comment = "Wow fluent cassandra is really great and easy to use.";

					var commentPostedOn = DateTime.Now;
					postComments[commentPostedOn] = comment;

					Console.WriteLine("Comment " + (i + 1) + " Posted On " + commentPostedOn.ToLongTimeString());
					Thread.Sleep(TimeSpan.FromSeconds(2));
				}

				// save the comments
				db.SaveChanges();
			}
		}

		#endregion

		#region Read Comments

		private static void ReadComments()
		{
			using (var db = new CassandraContext(keyspace: KeyspaceName, server: Server))
			{
				var key = "first-blog-post";
				var lastDate = DateTime.Now;

				// get the comments family
				var commentsFamily = db.GetColumnFamily<TimeUUIDType, UTF8Type>("Comments");

				for (int page = 0; page < 2; page++)
				{
					// lets back the date off by a millisecond so we don't get paging overlaps
					lastDate = lastDate.AddMilliseconds(-1D);

					ConsoleHeader("showing page " + page + " of comments starting at " + lastDate.ToLocalTime());

					var comments = commentsFamily.Get(key)
						.Reverse()
						.Fetch(lastDate)
						.Take(3)
						.FirstOrDefault();

					foreach (dynamic comment in comments)
					{
						var dateTime = (DateTime)comment.ColumnName;

						Console.WriteLine(String.Format("{0:T} : {1} ({2} - {3})",
							dateTime.ToLocalTime(),
							comment.Name,
							comment.Email,
							comment.Website
						));

						lastDate = dateTime;
					}
				}
			}
		}

		#endregion

		private static void Main(string[] args)
		{
			SetupKeyspace();

			CreatePost();

			ReadPost();

			UpdatePost();

			ReadPost();

			CreateComments();

			ReadComments();

			Console.Read();
		}
	}
}
