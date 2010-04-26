using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Thrift.Transport;
using Thrift.Protocol;

using FluentCassandra.Configuration;

namespace FluentCassandra.Sandbox
{
	internal class Program
	{
		private static void Main(string[] args)
		{
			using (var db = new CassandraContext("Blog", "localhost"))
			{
				dynamic postDetails = new FluentSuperColumn();
				postDetails.Title = "My First Cassandra Post";
				postDetails.Body = "Blah. Blah. Blah. about my first post on how great Cassandra is to work with.";
				postDetails.Author = "Nick Berardi";
				postDetails.PostedOn = DateTimeOffset.Now;
			
				dynamic tags = new FluentSuperColumn();
				tags[0] = "Cassandra";
				tags[1] = ".NET";
				tags[2] = "Database";
				tags[3] = "NoSQL";

				dynamic post = new FluentSuperColumnFamily(
					key: "first-post",
					columnFamily: "Posts"
				);

				post.Post = postDetails;
				post.Tags = tags;

				Console.WriteLine("attaching record");
				db.Attach(post);

				Console.WriteLine("saving changes");
				db.SaveChanges();

				var postsTable = db.GetColumnFamily("Posts");

				dynamic samePost = postsTable.GetSingle("first-post", new[] { "Post", "Tags" });

				samePost.Post.SetHint("PostedOn", typeof(DateTimeOffset));

				Console.WriteLine("display post");
				foreach (var col in samePost.Post)
					Console.WriteLine("{0}: {1}", col.Name, samePost.Post[col.Name]);

				Console.WriteLine("display tags");
				foreach (var tag in samePost.Tags)
					Console.WriteLine("{0}: {1}", tag.Name, samePost.Tags[tag.Name]);
			}

			Console.WriteLine("Done");
			Console.Read();
		}
	}
}
