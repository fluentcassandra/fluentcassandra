using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;

namespace FluentCassandra.Operations
{
	public class BatchMutate : ContextOperation<Void>
	{
		/*
		 * batch_mutate(keyspace, mutation_map, consistency_level)
		 */

		public IEnumerable<FluentMutation> Mutations { get; private set; }

		public override Void Execute(CassandraContext context)
		{
			var mutationMap = new Dictionary<string, Dictionary<string, List<Mutation>>>();

			foreach (var key in Mutations.GroupBy(x => x.Column.Family.Key))
			{
				var keyMutations = new Dictionary<string, List<Mutation>>();

				foreach (var columnFamily in key.GroupBy(x => x.Column.Family.FamilyName))
				{
					var columnFamilyMutations = columnFamily
						.Where(m => m.Type == MutationType.Added || m.Type == MutationType.Changed)
						.Select(m => ObjectHelper.CreateInsertedOrChangedMutation(m))
						.ToList();

					var superColumnsNeedingDeleted = columnFamily
						.Where(m => m.Type == MutationType.Removed && m.Column.GetParent().SuperColumn != null);

					foreach (var superColumn in superColumnsNeedingDeleted.GroupBy(x => x.Column.GetParent().SuperColumn.ColumnName))
						columnFamilyMutations.Add(ObjectHelper.CreateDeletedSuperColumnMutation(superColumn));

					var columnsNeedingDeleted = columnFamily
						.Where(m => m.Type == MutationType.Removed && m.Column.GetParent().SuperColumn == null);

					if (columnsNeedingDeleted.Count() > 0)
						columnFamilyMutations.Add(ObjectHelper.CreateDeletedColumnMutation(columnsNeedingDeleted));

					keyMutations.Add(columnFamily.Key, columnFamilyMutations);
				}

				mutationMap.Add(key.Key, keyMutations);
			}

			// Dictionary<string : key, Dicationary<string : columnFamily, List<Mutation>>>
			context.GetClient().batch_mutate(
				context.Keyspace.KeyspaceName,
				mutationMap,
				context.WriteConsistency
			);

			return new Void();
		}

		public BatchMutate(IEnumerable<FluentMutation> mutations)
		{
			Mutations = mutations;
		}
	}
}
