using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Thrift.Transport;
using Thrift.Protocol;

using FluentCassandra.Configuration;
using FluentCassandra.Types;

namespace FluentCassandra.Sandbox
{
	internal class Program
	{
		private static void Main(string[] args)
		{
			using (var db = new CassandraContext("Blog", "localhost"))
			{
				// create post
				dynamic post = new FluentSuperColumnFamily<AsciiType, AsciiType>(
					key: "first-post",
					columnFamily: "Posts"
				);

				// create post details
				dynamic postDetails = post.CreateSuperColumn();
				postDetails.Title = "My First Cassandra Post";
				postDetails.Body = "Blah. Blah. Blah. about my first post on how great Cassandra is to work with.";
				postDetails.Author = "Nick Berardi";
				postDetails.PostedOn = DateTimeOffset.Now;

				// create post tags
				dynamic tags = post.CreateSuperColumn();
				tags[0] = "Cassandra";
				tags[1] = ".NET";
				tags[2] = "Database";
				tags[3] = "NoSQL";

				// add properties to post
				post.Post = postDetails;
				post.Tags = tags;

				// attach the post to the database
				Console.WriteLine("attaching record");
				db.Attach(post);

				// save the changes
				Console.WriteLine("saving changes");
				db.SaveChanges();

				// get the table/column family to pull back the post
				var postsTable = db.GetColumnFamily<AsciiType, AsciiType>("Posts");

				// we want to pull back the post we just saved, and we want the Post and Tags columns that we added
				dynamic samePost = postsTable.GetSingle("first-post", new AsciiType[] { "Post", "Tags" });

				// display all the fields in the Post property
				Console.WriteLine("display post");
				foreach (var col in samePost.Post)
					Console.WriteLine("{0}: {1}", col.Name, samePost.Post[col.Name]);

				// display all the fields in the Tags property
				Console.WriteLine("display tags");
				foreach (var tag in samePost.Tags)
					Console.WriteLine("{0}: {1}", tag.Name, samePost.Tags[tag.Name]);

				dynamic comments = new FluentSuperColumnFamily<TimeUUIDType, AsciiType>(
					key: "first-post",
					columnFamily: "Comments"
				);

				// now lets add some comments
				dynamic comment = comments.CreateSuperColumn();
				comment.Author = "Nick Berardi";
				comment.CommentedOn = DateTime.Now;
				comment.Text = "Wow this is great.";

				dynamic comment2 = comments.CreateSuperColumn();
				comment2.Author = "Joe Somebody";
				comment2.CommentedOn = DateTime.Now;
				comment2.Text = "I agree with you Nick. -- Joe";

				// the comments are stored by time based Guid under the same key
				comments[GuidGenerator.GenerateTimeBasedGuid()] = comment;
				comments[GuidGenerator.GenerateTimeBasedGuid()] = comment2;

				db.Attach(comments);

				db.SaveChanges();
			}

			Console.WriteLine("Done");
			Console.Read();
		}
	}
}
