using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Thrift.Transport;
using Thrift.Protocol;
using Apache.Cassandra;

namespace FluentCassandra
{
	public class CassandraKeyspace
	{
		private readonly string _keyspaceName;

		public CassandraKeyspace(KsDef definition)
		{
			if (definition == null)
				throw new ArgumentNullException("definition");

			_keyspaceName = definition.Name;
			_cachedKeyspaceDescription = definition;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="keyspaceName"></param>
		/// <param name="connecton"></param>
		public CassandraKeyspace(string keyspaceName)
		{
			_keyspaceName = keyspaceName;
		}

		/// <summary>
		/// 
		/// </summary>
		public string KeyspaceName
		{
			get { return _keyspaceName; }
		}

		private KsDef _cachedKeyspaceDescription;

		#region Cassandra System For Server

		public CfDef GetColumnFamily(Server server, string columnFamily)
		{
			return Describe(server).Cf_defs.FirstOrDefault(cf => cf.Name == columnFamily);
		}

		public string AddColumnFamily(Server server, CfDef definition)
		{
			_cachedKeyspaceDescription = null;

			using (var session = new CassandraSession(new ConnectionBuilder(KeyspaceName, server.Host, server.Port)))
				return session.GetClient().system_add_column_family(definition);
		}

		public string UpdateColumnFamily(Server server, CfDef definition)
		{
			_cachedKeyspaceDescription = null;

			using (var session = new CassandraSession(new ConnectionBuilder(KeyspaceName, server.Host, server.Port)))
				return session.GetClient().system_update_column_family(definition);
		}

		public string DropColumnFamily(Server server, string columnFamily)
		{
			_cachedKeyspaceDescription = null;

			using (var session = new CassandraSession(new ConnectionBuilder(KeyspaceName, server.Host, server.Port)))
				return session.GetClient().system_drop_column_family(columnFamily);
		}

		#endregion

		#region Cassandra Descriptions For Server

		public bool ColumnFamilyExists(Server server, string columnFamilyName)
		{
			return Describe(server).Cf_defs.Any(columnFamily => columnFamily.Name == columnFamilyName);
		}

		public IEnumerable<CassandraTokenRange> DescribeRing(Server server)
		{
			using (var session = new CassandraSession(new ConnectionBuilder(KeyspaceName, server.Host, server.Port)))
			{
				var tokenRanges = session.GetClient(setKeyspace: false).describe_ring(KeyspaceName);

				foreach (var tokenRange in tokenRanges)
					yield return new CassandraTokenRange(tokenRange.Start_token, tokenRange.End_token, tokenRange.Endpoints);
			}
		}

		public KsDef Describe(Server server)
		{
			if (_cachedKeyspaceDescription == null)
				_cachedKeyspaceDescription = CassandraSession.GetKeyspace(server, KeyspaceName);

			return _cachedKeyspaceDescription;
		}

		#endregion
	}
}
