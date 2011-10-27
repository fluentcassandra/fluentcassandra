using System;
using System.Collections.Generic;
using System.Linq;
using Apache.Cassandra;

namespace FluentCassandra.Operations
{
	public class BatchMutate : Operation<Void>
	{
		/*
		 * batch_mutate(keyspace, mutation_map, consistency_level)
		 */

		public IEnumerable<FluentMutation> Mutations { get; private set; }

		public override Void Execute()
		{
			var mutationMap = new Dictionary<byte[], Dictionary<string, List<Mutation>>>();

			foreach (var key in Mutations.GroupBy(x => x.Column.Family.Key))
			{
				var keyMutations = new Dictionary<string, List<Mutation>>();

				foreach (var columnFamily in key.GroupBy(x => x.Column.Family.FamilyName))
				{
					var columnFamilyMutations = columnFamily
						.Where(m => m.Type == MutationType.Added || m.Type == MutationType.Changed)
						.Select(m => Helper.CreateInsertedOrChangedMutation(m))
						.ToList();

					var superColumnsNeedingDeleted = columnFamily
						.Where(m => m.Type == MutationType.Removed && m.Column.GetParent().SuperColumn != null);

					foreach (var superColumn in superColumnsNeedingDeleted.GroupBy(x => x.Column.GetParent().SuperColumn.ColumnName))
						columnFamilyMutations.AddRange(Helper.CreateDeletedSuperColumnMutation(superColumn));

					var columnsNeedingDeleted = columnFamily
						.Where(m => m.Type == MutationType.Removed && m.Column.GetParent().SuperColumn == null);

					if (columnsNeedingDeleted.Count() > 0)
						columnFamilyMutations.AddRange(Helper.CreateDeletedColumnMutation(columnsNeedingDeleted));

					keyMutations.Add(columnFamily.Key, columnFamilyMutations);
				}

				mutationMap.Add(key.Key, keyMutations);
			}

			// Dictionary<string : key, Dicationary<string : columnFamily, List<Mutation>>>
			CassandraSession.Current.GetClient().batch_mutate(
				mutationMap,
				CassandraSession.Current.WriteConsistency
			);

			return new Void();
		}

		public BatchMutate(IEnumerable<FluentMutation> mutations)
		{
			Mutations = mutations;
		}
	}
}
