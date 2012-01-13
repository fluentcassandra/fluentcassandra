using System;
using System.Collections.Generic;
using System.Linq;
using Apache.Cassandra;
using FluentCassandra.Connections;
using System.Diagnostics;
using FluentCassandra.Types;
using System.Text;

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

		public void TryCreateSelf(Server server)
		{
			if (CassandraSession.KeyspaceExists(server, KeyspaceName))
			{
				Debug.WriteLine(KeyspaceName + " already exists", "keyspace");
				return;
			}

			string result = CassandraSession.AddKeyspace(server, new KsDef {
				Name = KeyspaceName,
				Strategy_class = "org.apache.cassandra.locator.SimpleStrategy",
				Replication_factor = 1,
				Cf_defs = new List<CfDef>(0)
			});
			Debug.WriteLine(result, "keyspace setup");
		}

		public void TryCreateColumnFamily<CompareWith>(Server server, string columnFamilyName)
			where CompareWith : CassandraType
		{
			if (ColumnFamilyExists(server, columnFamilyName))
			{
				Debug.WriteLine(columnFamilyName + " already exists", "keyspace setup");
				return;
			}

			var comparatorType = GetCassandraComparatorType(typeof(CompareWith));

			string result = AddColumnFamily(server, new CfDef {
				Name = columnFamilyName,
				Keyspace = KeyspaceName,
				Comparator_type = comparatorType
			});
			Debug.WriteLine(result, "keyspace setup");
		}

		public void TryCreateColumnFamily<CompareWith, CompareSubcolumnWith>(Server server, string columnFamilyName)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			if (ColumnFamilyExists(server, columnFamilyName))
			{
				Debug.WriteLine(columnFamilyName + " already exists", "keyspace setup");
				return;
			}

			var comparatorType = GetCassandraComparatorType(typeof(CompareWith));
			var subComparatorType = GetCassandraComparatorType(typeof(CompareSubcolumnWith));

			string result = AddColumnFamily(server, new CfDef {
				Name = columnFamilyName,
				Keyspace = KeyspaceName,
				Column_type = "Super",
				Comparator_type = comparatorType,
				Subcomparator_type = subComparatorType
			});
			Debug.WriteLine(result, "keyspace setup");
		}

		private string GetCassandraComparatorType(Type comparatorType)
		{
			var comparatorTypeName = comparatorType.Name;

			// need to ignore generic dynamic composite types
			if (comparatorType.IsGenericType && comparatorTypeName.StartsWith("CompositeType"))
			{
				var compositeTypes = comparatorType.GetGenericArguments();
				var typeBuilder = new StringBuilder();

				typeBuilder.Append(comparatorTypeName.Substring(0, comparatorTypeName.IndexOf('`')));
				typeBuilder.Append("(");
				typeBuilder.Append(String.Join(",", compositeTypes.Select(t => t.Name)));
				typeBuilder.Append(")");

				comparatorTypeName = typeBuilder.ToString();
			}

			return comparatorTypeName;
		}

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
