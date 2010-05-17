using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FluentCassandra.Types;
using Apache.Cassandra;

namespace FluentCassandra.Operations
{
	public class MultiGetSuperColumnFamilySlice<CompareWith, CompareSubcolumnWith> : QueryableColumnFamilyOperation<IFluentSuperColumnFamily<CompareWith, CompareSubcolumnWith>>
		where CompareWith : CassandraType
		where CompareSubcolumnWith : CassandraType
	{
		/*
		 * map<string,list<ColumnOrSuperColumn>> multiget_slice(keyspace, keys, column_parent, predicate, consistency_level)
		 */

		public List<string> Keys { get; private set; }

		public override IEnumerable<IFluentSuperColumnFamily<CompareWith, CompareSubcolumnWith>> Execute(BaseCassandraColumnFamily columnFamily)
		{
			return GetFamilies(columnFamily);
		}

		private IEnumerable<IFluentSuperColumnFamily<CompareWith, CompareSubcolumnWith>> GetFamilies(BaseCassandraColumnFamily columnFamily)
		{
			var parent = new ColumnParent {
				Column_family = columnFamily.FamilyName
			};

			var output = columnFamily.GetClient().multiget_slice(
				columnFamily.Keyspace.KeyspaceName,
				Keys,
				parent,
				SlicePredicate.CreateSlicePredicate(),
				ConsistencyLevel
			);

			foreach (var result in output)
			{
				var r = new FluentSuperColumnFamily<CompareWith, CompareSubcolumnWith>(result.Key, columnFamily.FamilyName, result.Value.Select(col => {
					var superCol = ObjectHelper.ConvertSuperColumnToFluentSuperColumn<CompareWith, CompareSubcolumnWith>(col.Super_column);
					columnFamily.Context.Attach(superCol);
					superCol.MutationTracker.Clear();

					return superCol;
				}));
				columnFamily.Context.Attach(r);
				r.MutationTracker.Clear();

				yield return r;
			}
		}

		public MultiGetSuperColumnFamilySlice(IEnumerable<string> keys, CassandraSlicePredicate columnSlicePredicate)
		{
			this.Keys = keys.ToList();
			this.SlicePredicate = columnSlicePredicate;
		}
	}
}
