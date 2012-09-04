using System;
using System.Linq;
using FluentCassandra.Connections;
using FluentCassandra.Types;
using System.Collections.Generic;
using System.Configuration;

namespace FluentCassandra
{
    public class CassandraCql3DatabaseSetupFixture
    {
        private static bool DatabaseHasBeenCleaned = false;

        public CassandraCql3DatabaseSetup DatabaseSetup(bool? reset = null, bool toLower = false, bool toLower2 = false)
        {
            if (reset == null && !DatabaseHasBeenCleaned)
            {
                DatabaseHasBeenCleaned = true;

                // refresh the entire database
                return new CassandraCql3DatabaseSetup(reset: true);
            }

            return new CassandraCql3DatabaseSetup(reset: reset ?? false);
        }
    }

    public class CassandraCql3DatabaseSetup
    {
        public ConnectionBuilder ConnectionBuilder;
        public CassandraContext DB;

        public User[] Users = new[] {
					new User { Id = 1, Name = "Darren Gemmell", Email = "darren@somewhere.com", Age = 32 },
					new User { Id = 2, Name = "Fernando Laubscher", Email = "fernando@somewhere.com", Age = 23 },
					new User { Id = 3, Name = "Cody Millhouse", Email = "cody@somewhere.com", Age = 56 },
					new User { Id = 4, Name = "Emilia Thibert", Email = "emilia@somewhere.com", Age = 67 },
					new User { Id = 5, Name = "Allyson Schurr", Email = "allyson@somewhere.com", Age = 21 }
				};

        public class User
        {
            public int Id { get; set; }
            public string Name { get; set; }
            public string Email { get; set; }
            public int Age { get; set; }
        }

        public static readonly string Keyspace = ConfigurationManager.AppSettings["TestKeySpaceCql3"];
        public static readonly Server Server = new Server(ConfigurationManager.AppSettings["TestServer"]);

        public CassandraCql3DatabaseSetup(bool reset = false)
        {

            ConnectionBuilder = new ConnectionBuilder(keyspace: Keyspace, server: Server, cqlVersion: CqlVersion.Cql3);
            DB = new CassandraContext(ConnectionBuilder);

            if (DB.KeyspaceExists(Keyspace))
                DB.DropKeyspace(Keyspace);

            var keyspace = new CassandraKeyspace(new CassandraKeyspaceSchema
            {
                Name = Keyspace,
            }, DB);

            var exists = DB.KeyspaceExists(Keyspace);
            if (!exists)
                keyspace.TryCreateSelf();

            if (exists && !reset)
                return;

            ResetDatabase();
        }

        public void ResetDatabase()
        {
            using (var session = new CassandraSession(ConnectionBuilder))
            using (var db = new CassandraContext(session))
            {
                db.ThrowErrors = true;
                db.TryDropKeyspace(Keyspace);

                var keyspace = new CassandraKeyspace(new CassandraKeyspaceSchema
                {
                    Name = Keyspace
                }, db);
                db.Keyspace = keyspace;

                keyspace.TryCreateSelf();

                db.ExecuteNonQuery(@"
CREATE TABLE users (
	id int ,
	name ascii,
	email ascii,
	age int,
    PRIMARY KEY(id, email)
);");

                ResetUsersFamily(db);
            }
        }

        public static string Encode(string str)
        {
            return '\'' + str.Replace("\'", "\'\'") + '\'';
        }

        public void ResetUsersFamily(CassandraContext db)
        {
            db.ExecuteNonQuery(@"
TRUNCATE users
;
");

            foreach (var user in Users)
            {
                db.ExecuteNonQuery(string.Format(@"
INSERT INTO users(id, name, email, age) 
VALUES ({0},{1},{2},{3})
;
", user.Id.ToString(), Encode(user.Name), Encode(user.Email), user.Age.ToString()));
            }
        }

    }
}
