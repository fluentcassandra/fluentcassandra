using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Thrift.Transport;
using Thrift.Protocol;

using FluentCassandra.Configuration;
using FluentCassandra.Types;
using System.Threading;

namespace FluentCassandra.Sandbox
{
	internal class Program
	{
		private static void Main(string[] args)
		{
			using (var db = new CassandraContext(keyspace: "Blog", host: "192.168.1.155"))
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
					Console.Write(String.Format("{0}:{1},", tag.ColumnName, tag.ColumnValue));
				Console.WriteLine();

				// get the comments family
				var commentsFamily = db.GetColumnFamily<TimeUUIDType, UTF8Type>("Comments");

				dynamic postComments = commentsFamily.CreateRecord(key: "first-blog-post");

				// lets attach it to the database before we add the comments
				db.Attach(postComments);

				// add 5 comments
				for (int i = 0; i < 5; i++)
				{
					dynamic comment = postComments.CreateSuperColumn();
					comment.Name = i + " Nick Berardi";
					comment.Email = i + " nick@coderjournal.com";
					comment.Website = i + " www.coderjournal.com";
					comment.Comment = i + " Wow fluent cassandra is really great and easy to use.";

					postComments[GuidGenerator.GenerateTimeBasedGuid()] = comment;

					Console.WriteLine("Comment " + i + " Done");
					Thread.Sleep(TimeSpan.FromSeconds(5));
				}

				// save the comments
				db.SaveChanges();

				DateTime lastDate = DateTime.Now;

				for (int page = 0; page < 2; page++)
				{
					// lets back the date off by a millisecond so we don't get paging overlaps
					lastDate = lastDate.AddMilliseconds(-1D);

					Console.WriteLine("Showing page " + page + " starting at " + lastDate.ToLocalTime());

					var comments = commentsFamily.Get("first-blog-post")
						.Reverse()
						.Fetch(lastDate)
						.Take(3)
						.FirstOrDefault();

					foreach (dynamic comment in comments)
					{
						var dateTime = GuidGenerator.GetDateTime((Guid)comment.ColumnName);

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

			Console.Read();
		}
	}
}
