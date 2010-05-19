using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public abstract class CassandraSlicePredicate
	{
		internal abstract SlicePredicate CreateSlicePredicate();
	}

	public class ColumnSlicePredicate : CassandraSlicePredicate
	{
		public ColumnSlicePredicate(IEnumerable<CassandraType> columns)
		{
			Columns = columns;
		}

		public IEnumerable<CassandraType> Columns { get; private set; }

		internal override SlicePredicate CreateSlicePredicate()
		{
			return new SlicePredicate {
				Column_names = Columns.Select(x => (byte[])x).ToList()
			};
		}
	}

	public class RangeSlicePredicate : CassandraSlicePredicate
	{
		public RangeSlicePredicate(CassandraType start, CassandraType finish, bool reversed = false, int count = 100)
		{
			Start = start;
			Finish = finish;
			Reversed = reversed;
			Count = count;
		}

		public CassandraType Start { get; private set; }

		public CassandraType Finish { get; internal set; }

		public bool Reversed { get; internal set; }

		public int Count { get; internal set; }

		internal override SlicePredicate CreateSlicePredicate()
		{
			return new SlicePredicate {
				Slice_range = new SliceRange {
					Start = (Start == null ? new byte[0] : Start),
					Finish = (Finish == null ? new byte[0] : Finish),
					Reversed = Reversed,
					Count = Count
				}
			};
		}
	}
}
