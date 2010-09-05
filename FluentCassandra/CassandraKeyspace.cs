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

		private IDictionary<string, Dictionary<string, string>> _cachedKeyspaceDescription;

		#region Cassandra System For Server

		public string AddColumnFamily(Server server, CfDef definition)
		{
			_cachedKeyspaceDescription = null;

			using (var session = new CassandraSession(new ConnectionBuilder(KeyspaceName, server.Host, server.Port)))
			{
				return session.GetClient().system_add_column_family(definition);
			}
		}

		public string DropColumnFamily(Server server, string columnFamily)
		{
			_cachedKeyspaceDescription = null;

			using (var session = new CassandraSession(new ConnectionBuilder(KeyspaceName, server.Host, server.Port)))
			{
				return session.GetClient().system_drop_column_family(columnFamily);
			}
		}

		public string RenameColumnFamily(Server server, string oldColumnFamily, string newColumnFamily)
		{
			_cachedKeyspaceDescription = null;

			using (var session = new CassandraSession(new ConnectionBuilder(KeyspaceName, server.Host, server.Port)))
			{
				return session.GetClient().system_rename_column_family(oldColumnFamily, newColumnFamily);
			}
		}

		#endregion

		#region Cassandra Descriptions For Server

		public bool ColumnFamilyExists(Server server, string columnFamilyName)
		{
			return Describe(server).ContainsKey(columnFamilyName);
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

		public IDictionary<string, Dictionary<string, string>> Describe(Server server)
		{
			if (_cachedKeyspaceDescription == null)
			{
				using (var session = new CassandraSession(new ConnectionBuilder(KeyspaceName, server.Host, server.Port)))
				{
					var desc = session.GetClient(setKeyspace: false).describe_keyspace(KeyspaceName);
					_cachedKeyspaceDescription = desc;
				}
			}

			return _cachedKeyspaceDescription;
		}

		#endregion
	}
}
