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
			using (var db = new CassandraContext(keyspace: "Blog", host: "localhost"))
			{
				var family = db.GetColumnFamily<UTF8Type, UTF8Type>("Posts");

				// create post
				dynamic post = family.CreateRecord(key: "first-blog-post");

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
				post.Details = postDetails;
				post.Tags = tags;

				// attach the post to the database
				Console.WriteLine("attaching record");
				db.Attach(post);

				// save the changes
				Console.WriteLine("saving changes");
				db.SaveChanges();

				// get the post back from the database
				Console.WriteLine("getting 'first-blog-post'");
				dynamic getPost = family.Get("first-blog-post").FirstOrDefault();

				// show details
				dynamic getPostDetails = getPost.Details;
				Console.WriteLine(
					String.Format("=={0} by {1}==\n{2}", 
						getPostDetails.Title, 
						getPostDetails.Author, 
						getPostDetails.Body
					));
				
				// show tags
				Console.Write("tags:");
				foreach (var tag in getPost.Tags)
					Console.Write(String.Format("{0}:{1},", tag.Name, tag.Value));
			}

			Console.Read();
		}
	}
}
