using System;
using System.Collections.Generic;
using System.Linq;
using Apache.Cassandra;

namespace FluentCassandra
{
	public class CassandraKeyspaceSchema
	{
		public const string ReplicaPlacementStrategySimple = "org.apache.cassandra.locator.SimpleStrategy";
		public const string ReplicaPlacementStrategyLocal = "org.apache.cassandra.locator.LocalStrategy";
		public const string ReplicaPlacementStrategyNetworkTopology = "org.apache.cassandra.locator.NetworkTopologyStrategy";

		public CassandraKeyspaceSchema()
		{
			Strategy = ReplicaPlacementStrategySimple;
			StrategyOptions = new Dictionary<string, string>() { { "replication_factor", "1" } };
			ColumnFamilies = new List<CassandraColumnFamilySchema>();
			DurableWrites = true;
		}

		public CassandraKeyspaceSchema(KsDef def)
		{
			Name = def.Name;
			Strategy = def.Strategy_class;
			StrategyOptions = def.Strategy_options ?? new Dictionary<string, string>();
			ColumnFamilies = def.Cf_defs.Select(family => new CassandraColumnFamilySchema(family)).ToList();
			DurableWrites = def.Durable_writes;
		}

		public string Name { get; set; }
		public string Strategy { get; set; }
		public Dictionary<string,string> StrategyOptions { get; private set; }
		public bool DurableWrites { get; set; }

		public IList<CassandraColumnFamilySchema> ColumnFamilies { get; set; }

		public static implicit operator KsDef(CassandraKeyspaceSchema schema)
		{
			return new KsDef {
				Name = schema.Name,
				Strategy_class = schema.Strategy,
				Strategy_options = schema.StrategyOptions,
				Durable_writes = schema.DurableWrites,
				Cf_defs = new List<CfDef>(0)
			};
		}

		public static implicit operator CassandraKeyspaceSchema(KsDef def)
		{
			return new CassandraKeyspaceSchema(def);
		}
	}
}
