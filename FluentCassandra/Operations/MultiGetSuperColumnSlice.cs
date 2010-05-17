using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class MultiGetSuperColumnSlice<CompareWith, CompareSubcolumnWith> : QueryableColumnFamilyOperation<IFluentSuperColumn<CompareWith, CompareSubcolumnWith>>
		where CompareWith : CassandraType
		where CompareSubcolumnWith : CassandraType
	{
		/*
		 * map<string,list<ColumnOrSuperColumn>> multiget_slice(keyspace, keys, column_parent, predicate, consistency_level)
		 */

		public List<string> Keys { get; private set; }

		public CassandraType SuperColumnName { get; private set; }

		public override IEnumerable<IFluentSuperColumn<CompareWith, CompareSubcolumnWith>> Execute(BaseCassandraColumnFamily columnFamily)
		{
			return GetFamilies(columnFamily);
		}

		private IEnumerable<IFluentSuperColumn<CompareWith, CompareSubcolumnWith>> GetFamilies(BaseCassandraColumnFamily columnFamily)
		{
			var parent = new ColumnParent {
				Column_family = columnFamily.FamilyName
			};

			if (SuperColumnName != null)
				parent.Super_column = SuperColumnName;

			var output = columnFamily.GetClient().multiget_slice(
				columnFamily.Keyspace.KeyspaceName,
				Keys,
				parent,
				SlicePredicate.CreateSlicePredicate(),
				ConsistencyLevel
			);

			foreach (var result in output)
			{
				var r = new FluentSuperColumn<CompareWith, CompareSubcolumnWith>(result.Value.Select(col => {
					return ObjectHelper.ConvertColumnToFluentColumn<CompareSubcolumnWith>(col.Column);
				}));
				columnFamily.Context.Attach(r);
				r.MutationTracker.Clear();

				yield return r;
			}
		}

		public MultiGetSuperColumnSlice(IEnumerable<string> keys, CassandraType superColumnName, CassandraSlicePredicate columnSlicePredicate)
		{
			this.Keys = keys.ToList();
			this.SuperColumnName = superColumnName;
			this.SlicePredicate = columnSlicePredicate;
		}
	}
}