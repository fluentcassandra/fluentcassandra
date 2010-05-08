using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;
using FluentCassandra.Types;
using FluentCassandra.Operations.Helpers;

namespace FluentCassandra.Operations
{
	public class GetColumnFamilySlice<CompareWith> : ColumnFamilyOperation<FluentColumnFamily<CompareWith>>
		where CompareWith : CassandraType
	{
		/*
		 * list<ColumnOrSuperColumn> get_slice(keyspace, key, column_parent, predicate, consistency_level)
		 */

		public string Key { get; private set; }

		public CassandraSlicePredicate SlicePredicate { get; private set; }

		public override FluentColumnFamily<CompareWith> Execute(BaseCassandraColumnFamily columnFamily)
		{
			var result = new FluentColumnFamily<CompareWith>(Key, columnFamily.FamilyName, GetColumns(columnFamily));
			columnFamily.Context.Attach(result);
			result.MutationTracker.Clear();

			return result;
		}

		private IEnumerable<IFluentColumn<CompareWith>> GetColumns(BaseCassandraColumnFamily columnFamily)
		{
			var parent = new ColumnParent {
				Column_family = columnFamily.FamilyName
			};

			var output = columnFamily.GetClient().get_slice(
				columnFamily.Keyspace.KeyspaceName,
				Key,
				parent,
				SlicePredicate.CreateSlicePredicate(),
				ConsistencyLevel
			);

			foreach (var result in output)
			{
				var r = ObjectHelper.ConvertColumnToFluentColumn<CompareWith>(result.Column);
				yield return r;
			}
		}

		public GetColumnFamilySlice(string key, CassandraSlicePredicate columnSlicePredicate)
		{
			this.Key = key;
			this.SlicePredicate = columnSlicePredicate;
		}
	}
}