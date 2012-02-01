using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Apache.Cassandra;
using FluentCassandra.Connections;
using FluentCassandra.Operations;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public class CassandraKeyspace
	{
		private readonly string _keyspaceName;
		private KsDef _cachedKeyspaceDescription;
		private CassandraContext _context;

		/// <summary>
		/// 
		/// </summary>
		/// <param name="definition"></param>
		public CassandraKeyspace(KsDef definition, CassandraContext context)
		{
			if (definition == null)
				throw new ArgumentNullException("definition");

			_keyspaceName = definition.Name;
			_cachedKeyspaceDescription = definition;
			_context = context;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="keyspaceName"></param>
		/// <param name="connecton"></param>
		public CassandraKeyspace(string keyspaceName, CassandraContext context)
		{
			if (keyspaceName == null)
				throw new ArgumentNullException("keyspaceName");

			_keyspaceName = keyspaceName;
			_context = context;
		}

		/// <summary>
		/// 
		/// </summary>
		public string KeyspaceName
		{
			get { return _keyspaceName; }
		}

		/// <summary>
		/// 
		/// </summary>
		/// <returns></returns>
		public override string ToString()
		{
			return KeyspaceName;
		}

		public void TryCreateSelf()
		{
			if (_context.KeyspaceExists(KeyspaceName))
			{
				Debug.WriteLine(KeyspaceName + " already exists", "keyspace");
				return;
			}

			string result = _context.AddKeyspace(new KsDef {
				Name = KeyspaceName,
				Strategy_class = "org.apache.cassandra.locator.SimpleStrategy",
				Replication_factor = 1,
				Cf_defs = new List<CfDef>(0)
			});
			Debug.WriteLine(result, "keyspace setup");
		}

		public void TryCreateColumnFamily<CompareWith>(string columnFamilyName)
			where CompareWith : CassandraType
		{
			try
			{
				var comparatorType = GetCassandraComparatorType(typeof(CompareWith));

				string result = _context.AddColumnFamily(new CfDef {
					Name = columnFamilyName,
					Keyspace = KeyspaceName,
					Comparator_type = comparatorType
				});
				Debug.WriteLine(result, "keyspace setup");
			}
			catch (Exception exc)
			{
				Debug.WriteLine(columnFamilyName + " already exists", "keyspace setup");
			}
		}

		public void TryCreateColumnFamily<CompareWith, CompareSubcolumnWith>(string columnFamilyName)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			try
			{
				var comparatorType = GetCassandraComparatorType(typeof(CompareWith));
				var subComparatorType = GetCassandraComparatorType(typeof(CompareSubcolumnWith));

				string result = _context.AddColumnFamily(new CfDef {
					Name = columnFamilyName,
					Keyspace = KeyspaceName,
					Column_type = "Super",
					Comparator_type = comparatorType,
					Subcomparator_type = subComparatorType
				});
				Debug.WriteLine(result, "keyspace setup");
			}
			catch (Exception exc)
			{
				Debug.WriteLine(columnFamilyName + " already exists", "keyspace setup");
			}
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

		public CfDef GetColumnFamilyDescription(string columnFamily)
		{
			return Describe().Cf_defs.FirstOrDefault(cf => cf.Name == columnFamily);
		}

		public bool ColumnFamilyExists(string columnFamilyName)
		{
			return Describe().Cf_defs.Any(columnFamily => columnFamily.Name == columnFamilyName);
		}

		public void ClearCachedKeyspaceDescription()
		{
			_cachedKeyspaceDescription = null;
		}

		#region Cassandra Keyspace Server Operations

		public KsDef Describe()
		{
			if (_cachedKeyspaceDescription == null)
				_cachedKeyspaceDescription = _context.ExecuteOperation(new SimpleOperation<Apache.Cassandra.KsDef>(ctx => {
					return ctx.Session.GetClient().describe_keyspace(KeyspaceName);
			}));

			return _cachedKeyspaceDescription;
		}

		public IEnumerable<CassandraTokenRange> DescribeRing()
		{
			return _context.ExecuteOperation(new SimpleOperation<IEnumerable<CassandraTokenRange>>(ctx => {
				var tokenRanges = ctx.Session.GetClient(setKeyspace: false).describe_ring(KeyspaceName);
				return tokenRanges.Select(tr => new CassandraTokenRange(tr.Start_token, tr.End_token, tr.Endpoints));
			}));
		}

		#endregion
	}
}
