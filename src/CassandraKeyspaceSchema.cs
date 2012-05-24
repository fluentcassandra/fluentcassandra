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
			Strategy = ReplicaPlacementStrategyNetworkTopology;
			ReplicationFactor = 1;
			ColumnFamilies = new List<CassandraColumnFamilySchema>();
		}

		public CassandraKeyspaceSchema(KsDef def)
		{
			Name = def.Name;
			Strategy = def.Strategy_class;
			ReplicationFactor = def.Replication_factor;
			ColumnFamilies = def.Cf_defs.Select(family => new CassandraColumnFamilySchema(family)).ToList();
		}

		public string Name { get; set; }
		public string Strategy { get; set; }
		public int ReplicationFactor { get; set; }

		public IList<CassandraColumnFamilySchema> ColumnFamilies { get; set; }

		public static implicit operator KsDef(CassandraKeyspaceSchema schema)
		{
			return new KsDef {
				Name = schema.Name,
				Strategy_class = schema.Strategy,
				Replication_factor = schema.ReplicationFactor,
				Cf_defs = new List<CfDef>(0)
			};
		}

		public static implicit operator CassandraKeyspaceSchema(KsDef def)
		{
			return new CassandraKeyspaceSchema(def);
		}
	}
}
