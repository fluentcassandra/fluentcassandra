using System;
using System.Linq;
using FluentCassandra.Connections;
using FluentCassandra.Types;
using System.Collections.Generic;
using System.Configuration;

namespace FluentCassandra
{
    public class GenericDatabaseSetupFixture
    {
        private static bool DatabaseHasBeenCleaned = false;

        public GenericDatabaseSetup DatabaseSetup(bool? reset = null)
        {
            if (reset == null && !DatabaseHasBeenCleaned)
            {
                DatabaseHasBeenCleaned = true;

                // refresh the entire database
                return new GenericDatabaseSetup(reset: true);
            }

            return new GenericDatabaseSetup(reset: reset ?? false);
        }
    }

    public class GenericDatabaseSetup
    {
        public ConnectionBuilder ConnectionBuilder;
        public CassandraContext DB;

        public static readonly string Keyspace = ConfigurationManager.AppSettings["TestKeySpaceCql3"];
        public static readonly Server Server = new Server(ConfigurationManager.AppSettings["TestServer"]);

        public GenericDatabaseSetup(bool reset = false)
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

            }
        }

        public static string Encode(string str)
        {
            return '\'' + str.Replace("\'", "\'\'") + '\'';
        }

    }
}
