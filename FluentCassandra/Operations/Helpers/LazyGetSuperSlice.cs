using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FluentCassandra.Types;
using Apache.Cassandra;

namespace FluentCassandra.Operations.Helpers
{
	internal class LazyGetSuperSlice<CompareWith, CompareSubcolumnWith> : IEnumerable<IFluentSuperColumn<CompareWith, CompareSubcolumnWith>>
		where CompareWith : CassandraType
		where CompareSubcolumnWith : CassandraType
	{
		public LazyGetSuperSlice(BaseCassandraColumnFamily columnFamily, GetSuperColumnFamilySlice<CompareWith, CompareSubcolumnWith> command)
		{
			this.ColumnFamily = columnFamily;
			this.Command = command;
		}

		public BaseCassandraColumnFamily ColumnFamily { get; private set; }

		public GetSuperColumnFamilySlice<CompareWith, CompareSubcolumnWith> Command { get; private set; }

		#region IEnumerable<IFluentSuperColumn<CompareWith, CompareSubcolumnWith>> Members

		public IEnumerator<IFluentSuperColumn<CompareWith, CompareSubcolumnWith>> GetEnumerator()
		{
			var parent = new ColumnParent {
				Column_family = ColumnFamily.FamilyName
			};

			var output = ColumnFamily.GetClient().get_slice(
				ColumnFamily.Keyspace.KeyspaceName,
				Command.Key,
				parent,
				Command.SlicePredicate.CreateSlicePredicate(),
				Command.ConsistencyLevel
			);

			foreach (var result in output)
			{
				var r = ObjectHelper.ConvertSuperColumnToFluentSuperColumn<CompareWith, CompareSubcolumnWith>(result.Super_column);
				ColumnFamily.Context.Attach(r);
				r.MutationTracker.Clear();

				yield return r;
			}
		}

		#endregion

		#region IEnumerable Members

		System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}

		#endregion
	}
}
