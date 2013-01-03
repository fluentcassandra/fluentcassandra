using System;
using System.Collections.Generic;
using System.Linq;
using FluentCassandra.Apache.Cassandra;

namespace FluentCassandra.Operations
{
	public class BatchMutate : Operation<Void>
	{
		public IEnumerable<FluentMutation> Mutations { get; private set; }
		public bool Atomic { get; private set; }

		public override Void Execute()
		{
			if (Mutations.Count() == 0)
				return new Void();

			var mutationMap = new Dictionary<byte[], Dictionary<string, List<Mutation>>>();

			foreach (var key in Mutations.GroupBy(x => x.Column.Family.Key)) {
				var keyMutations = new Dictionary<string, List<Mutation>>();

				foreach (var columnFamily in key.GroupBy(x => x.Column.Family.FamilyName)) {
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

				mutationMap.Add(key.Key.TryToBigEndian(), keyMutations);
			}

			var client = Session.GetClient();

			if (Atomic && client.describe_version() >= RpcApiVersion.Cassandra120)
				client.atomic_batch_mutate(
					mutationMap,
					Session.WriteConsistency);
			else
				client.batch_mutate(
					mutationMap,
					Session.WriteConsistency);

			return new Void();
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="mutations"></param>
		/// <param name="atomic">in the database sense that if any part of the batch succeeds, all of it will. No other guarantees are implied; in particular, there is no isolation; other clients will be able to read the first updated rows from the batch, while others are in progress</param>
		/// <seealso href="http://www.datastax.com/dev/blog/atomic-batches-in-cassandra-1-2"/>
		public BatchMutate(IEnumerable<FluentMutation> mutations, bool atomic)
		{
			Mutations = mutations;
			Atomic = atomic;
		}
	}
}
