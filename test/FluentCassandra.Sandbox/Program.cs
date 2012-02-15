using System;
using System.Linq;
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

				var keyspace = new CassandraKeyspace(new CassandraKeyspaceSchema { 
					Name = KeyspaceName, 
					Strategy = CassandraKeyspaceSchema.ReplicaPlacementStrategySimple, 
					ReplicationFactor = 1 }, db);

				keyspace.TryCreateSelf();
				db.ExecuteNonQuery(@"
CREATE COLUMNFAMILY Posts (
	KEY ascii PRIMARY KEY,
	Title text,
	Body text,
	Author text,
	PostedOn timestamp
);");
				keyspace.TryCreateColumnFamily(new CassandraColumnFamilySchema {
					FamilyName = "Tags",
					KeyType = CassandraType.AsciiType,
					ColumnNameType = CassandraType.Int32Type,
					DefaultColumnValueType = CassandraType.UTF8Type
				});
				keyspace.TryCreateColumnFamily(new CassandraColumnFamilySchema {
					FamilyName = "Comments",
					FamilyType = ColumnType.Super,
					KeyType = CassandraType.AsciiType,
					SuperColumnNameType = CassandraType.TimeUUIDType,
					ColumnNameType = CassandraType.UTF8Type,
					DefaultColumnValueType = CassandraType.UTF8Type
				});
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

		private static void CreateFirstPost()
		{
			using (var db = new CassandraContext(keyspace: KeyspaceName, server: Server))
			{
				var key = "first-blog-post";

				var postFamily = db.GetColumnFamily("Posts");
				var tagsFamily = db.GetColumnFamily("Tags");

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

		private static void CreateSecondPost()
		{
			using (var db = new CassandraContext(keyspace: KeyspaceName, server: Server))
			{
				var key = "second-blog-post";

				var postFamily = db.GetColumnFamily("Posts");
				var tagsFamily = db.GetColumnFamily("Tags");

				// create post
				ConsoleHeader("create post");
				dynamic post = postFamily.CreateRecord(key: key);
				post.Title = "My Second Cassandra Post";
				post.Body = "Blah. Blah. Blah. about my second post on how great Cassandra is to work with.";
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

		private static void ReadFirstPost()
		{
			using (var db = new CassandraContext(keyspace: KeyspaceName, server: Server))
			{
				var key = "first-blog-post";

				var postFamily = db.GetColumnFamily("Posts");
				var tagsFamily = db.GetColumnFamily("Tags");

				// get the post back from the database
				ConsoleHeader("getting 'first-blog-post'");
				dynamic post = postFamily.Get(key).FirstOrDefault();
				dynamic tags = (
					from t in tagsFamily
					where t.Key == key
					select t).FirstOrDefault();

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
					Console.WriteLine(String.Format("{0}:{1}", tag.ColumnName, tag.ColumnValue));
			}
		}

		private static void ReadAllPosts()
		{
			using (var db = new CassandraContext(keyspace: KeyspaceName, server: Server))
			{
				var key = "first-blog-post";

				var tagsFamily = db.GetColumnFamily("Tags");

				// get the post back from the database
				ConsoleHeader("getting 'first-blog-post'");
				var posts = db.ExecuteQuery("SELECT * FROM Posts LIMIT 25");
				dynamic tags = tagsFamily.Get(key).FirstOrDefault();

				// show details
				ConsoleHeader("showing post");
				foreach (dynamic post in posts)
				{
					Console.WriteLine(
						String.Format("=={0} by {1}==\n{2}",
							post.Title,
							post.Author,
							post.Body
						));
				}

				// show tags
				ConsoleHeader("showing tags");
				foreach (var tag in tags)
					Console.WriteLine(String.Format("{0}:{1}", tag.ColumnName, tag.ColumnValue));
			}
		}

		#endregion

		#region Update Post
		
		private static void UpdateFirstPost()
		{
			using (var db = new CassandraContext(keyspace: KeyspaceName, server: Server))
			{
				var key = "first-blog-post";

				var postFamily = db.GetColumnFamily("Posts");

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
				var commentsFamily = db.GetSuperColumnFamily("Comments");

				ConsoleHeader("create comments");
				dynamic postComments = commentsFamily.CreateRecord(key: key);

				// lets attach it to the database before we add the comments
				db.Attach(postComments);

				var dt = new DateTime(2010, 11, 29, 5, 03, 00, DateTimeKind.Local);

				// add 5 comments
				for (int i = 0; i < 5; i++)
				{
					dynamic comment = postComments.CreateSuperColumn();
					comment.Name = "Nick Berardi";
					comment.Email = "nick@coderjournal.com";
					comment.Website = "www.coderjournal.com";
					comment.Comment = "Wow fluent cassandra is really great and easy to use.";

					var commentPostedOn = dt;
					postComments[commentPostedOn] = comment;

					Console.WriteLine("Comment " + (i + 1) + " Posted On " + commentPostedOn.ToLongTimeString());
					dt = dt.AddMinutes(2);
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
				var commentsFamily = db.GetSuperColumnFamily("Comments");

				for (int page = 0; page < 2; page++)
				{
					// lets back the date off by a millisecond so we don't get paging overlaps
					lastDate = lastDate.AddMilliseconds(-1D);

					ConsoleHeader("showing page " + page + " of comments starting at " + lastDate.ToLocalTime());

					var comments = commentsFamily.Get(key)
						.ReverseColumns()
						.StartWithColumn(lastDate)
						.TakeColumns(3)
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

			CreateFirstPost();

			CreateSecondPost();

			ReadFirstPost();

			ReadAllPosts();

			UpdateFirstPost();

			ReadFirstPost();

			CreateComments();

			ReadComments();

			Console.Read();
		}
	}
}
