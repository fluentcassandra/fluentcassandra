using System;
using System.Linq;
using FluentCassandra.Connections;
using FluentCassandra.Types;
using FluentCassandra.Linq;
using System.Collections.Generic;
using System.Configuration;


namespace FluentCassandra.Sandbox
{
    internal class Program
    {
        public static readonly string KeyspaceName = ConfigurationManager.AppSettings["TestKeySpace"];
        public static readonly Server Server = new Server(ConfigurationManager.AppSettings["TestServer"]);
		

        #region Setup

        private static void SetupKeyspace()
        {
            using (var db = new CassandraContext(keyspace: KeyspaceName, server: Server))
            {
                if (db.KeyspaceExists(KeyspaceName))
                    db.DropKeyspace(KeyspaceName);

                var keyspace = new CassandraKeyspace(new CassandraKeyspaceSchema
                {
                    Name = KeyspaceName,
                }, db);

                keyspace.TryCreateSelf();

                // create column family using CQL
                db.ExecuteNonQuery(@"
                CREATE COLUMNFAMILY Posts (
	            KEY ascii PRIMARY KEY,
	            Title text,
	            Body text,
	            Author text,
	            PostedOn timestamp
                );");

                // create column family using API
                keyspace.TryCreateColumnFamily(new CassandraColumnFamilySchema
                {
                    FamilyName = "Tags",
                    KeyValueType = CassandraType.AsciiType,
                    ColumnNameType = CassandraType.Int32Type,
                    DefaultColumnValueType = CassandraType.UTF8Type
                });

                // create super column family using API
                keyspace.TryCreateColumnFamily(new CassandraColumnFamilySchema
                {
                    FamilyName = "Comments",
                    FamilyType = ColumnType.Super,
                    KeyValueType = CassandraType.AsciiType,
                    SuperColumnNameType = CassandraType.DateType,
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

                // query using API
                dynamic post = postFamily.Get(key).FirstOrDefault();

                // query using CQL-LINQ
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

                // query using CQL
                var posts = db.ExecuteQuery("SELECT * FROM Posts LIMIT 25");

                // query using API
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

                // query using API
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
				var postComments = commentsFamily.CreateRecord(key: key);
				
                // lets attach it to the database before we add the comments
                db.Attach(postComments);

                var dt = new DateTime(2010, 11, 29, 5, 03, 00, DateTimeKind.Local);

                // add 5 comments
                for (int i = 0; i < 5; i++)
                {
					var comment = postComments.CreateSuperColumn();
					comment["Name"] = "Nick Berardi";
					comment["Email"] = "nick@coderjournal.com";

					// you can also use it as a dynamic object
					dynamic dcomment = comment;
					dcomment.Website = "www.coderjournal.com";
					dcomment.Comment = "Wow fluent cassandra is really great and easy to use.";

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

                    // query using API
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

        #region CreateColumnFamilyWithUUIDOperator
        private static void CreateColumnFamilyWithUUIDOperator()
        {

            using (var db = new CassandraContext(keyspace: KeyspaceName, server: Server))
            {

                db.ExecuteNonQuery("CREATE TABLE TestCF (KEY text PRIMARY KEY) WITH comparator=uuid AND default_validation=text;");

                Guid columnName1 = GuidGenerator.GenerateTimeBasedGuid(DateTime.Now);

                Guid columnName2 = GuidGenerator.GenerateTimeBasedGuid(DateTime.Now);

                db.ExecuteNonQuery(string.Format("INSERT INTO TestCF (KEY, {0}, {1}) VALUES ('Key1', 'Value1', 'Value2')", columnName1, columnName2));

                List<ICqlRow> rows = db.ExecuteQuery("SELECT * FROM TestCF WHERE KEY = 'Key1'").ToList();

                Guid value = rows[0].Columns[1].ColumnName.GetValue<Guid>();

                ConsoleHeader("Returned data from db");
                ConsoleHeader(value.ToString());
            }
        }
        #endregion

        #region CreateColumnFamilyWithTimestampOperator
        private static void CreateColumnFamilyWithTimestampOperator()
        {
            using (var db = new CassandraContext(keyspace: KeyspaceName, server: Server))
            {

                db.ExecuteNonQuery("CREATE TABLE TestCF2 (KEY text PRIMARY KEY) WITH comparator=timestamp AND default_validation=text;");

                DateTimeOffset UnixStart = new DateTimeOffset(1970, 1, 1, 0, 0, 0, TimeSpan.Zero);
                DateTimeOffset TimeNow = DateTimeOffset.UtcNow;

                var columnName2 = Convert.ToInt64(Math.Floor((TimeNow - UnixStart).TotalMilliseconds));
                var columnName1 = columnName2 + 5000;

                db.ExecuteNonQuery(string.Format("INSERT INTO TestCF2 (KEY, {0}, {1}) VALUES ('Key1', 'Value1', 'Value2')", columnName1, columnName2));

                List<ICqlRow> rows = db.ExecuteQuery("SELECT * FROM TestCF2 WHERE KEY = 'Key1'").ToList();

                DateTime value1 = rows[0].Columns[1].ColumnName.GetValue<DateTime>();
                DateTime value2 = rows[0].Columns[2].ColumnName.GetValue<DateTime>();

                ConsoleHeader("Returned data from db for timestamp comparator");
                ConsoleHeader(value1.ToString());
                ConsoleHeader(value2.ToString());

            }
        }
        #endregion

        #region TombstoneTest
        private static void TombstoneTest()
        {
            using (var db = new CassandraContext(keyspace: KeyspaceName, server: Server))
            {


                db.ExecuteNonQuery("CREATE TABLE OfferReservation (KEY int PRIMARY KEY) WITH comparator = text AND default_validation = float");
                db.ExecuteNonQuery("INSERT INTO OfferReservation (KEY, '25:100') VALUES (5, 0.25)");
                db.ExecuteNonQuery("INSERT INTO OfferReservation (KEY, '25:101') VALUES (5, 0.25)");
                db.ExecuteNonQuery("DELETE '25:100' FROM OfferReservation WHERE KEY = 5");

                List<ICqlRow> rows = db.ExecuteQuery("SELECT '25:100' FROM OfferReservation WHERE KEY = 5").ToList();

            }
        }
        #endregion

        #region BigDecimalTest
        private static void BigDecimalTest()
        {
            using (var db = new CassandraContext(keyspace: KeyspaceName, server: Server))
            {

                // arrange
                db.ExecuteNonQuery("CREATE TABLE OfferReservation2 (KEY text PRIMARY KEY) WITH comparator = text AND default_validation = decimal");
                db.ExecuteNonQuery("INSERT INTO OfferReservation2 (KEY, 'MyColumn') VALUES ('Key0', 1000000000000000000)");
                db.ExecuteNonQuery("INSERT INTO OfferReservation2 (KEY, 'MyColumn') VALUES ('Key1', 0.25)");
                db.ExecuteNonQuery("INSERT INTO OfferReservation2 (KEY, 'MyColumn') VALUES ('Key2', 2000000000000.1234)");
                db.ExecuteNonQuery("INSERT INTO OfferReservation2 (KEY, 'MyColumn') VALUES ('Key3', -0.25)");
                db.ExecuteNonQuery("INSERT INTO OfferReservation2 (KEY, 'MyColumn') VALUES ('Key4', -0.25122333)");

                var actual = db.ExecuteQuery("SELECT * FROM OfferReservation2");

                var results = actual.ToList();


                var firstValue = (decimal)results.First(x => x.Key == "Key0")["MyColumn"];
                var secondValue = (decimal)results.First(x => x.Key == "Key1")["MyColumn"];
                var thirdValue = (decimal)results.First(x => x.Key == "Key2")["MyColumn"];
                var fourthValue = (decimal)results.First(x => x.Key == "Key3")["MyColumn"];
                var fifthValue = (decimal)results.First(x => x.Key == "Key4")["MyColumn"];

                ConsoleHeader("Returned data from Big Decimal Test");
                ConsoleHeader(firstValue.ToString());
                ConsoleHeader(secondValue.ToString());
                ConsoleHeader(thirdValue.ToString());
                ConsoleHeader(fourthValue.ToString());
                ConsoleHeader(fifthValue.ToString());
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

            CreateColumnFamilyWithUUIDOperator();

            CreateColumnFamilyWithTimestampOperator();

            ReadComments();

            TombstoneTest();

            BigDecimalTest();

            Console.Read();
        }
    }
}
