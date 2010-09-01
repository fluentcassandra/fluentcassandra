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

		public List<BytesType> Keys { get; private set; }

		public CassandraType SuperColumnName { get; private set; }

		public override IEnumerable<IFluentSuperColumn<CompareWith, CompareSubcolumnWith>> Execute(BaseCassandraColumnFamily columnFamily)
		{
			CassandraSession _localSession = null;
			if (CassandraSession.Current == null)
				_localSession = new CassandraSession();

			try
			{
				var parent = new ColumnParent {
					Column_family = columnFamily.FamilyName
				};

				if (SuperColumnName != null)
					parent.Super_column = SuperColumnName;

				var output = CassandraSession.Current.GetClient().multiget_slice(
					Keys.ToByteArrayList(),
					parent,
					SlicePredicate.CreateSlicePredicate(),
					CassandraSession.Current.ReadConsistency
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
			finally
			{
				if (_localSession != null)
					_localSession.Dispose();
			}
		}

		public MultiGetSuperColumnSlice(IEnumerable<BytesType> keys, CassandraType superColumnName, CassandraSlicePredicate columnSlicePredicate)
		{
			Keys = keys.ToList();
			SuperColumnName = superColumnName;
			SlicePredicate = columnSlicePredicate;
		}
	}
}