using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;
using FluentCassandra.Types;

namespace FluentCassandra.Actions
{
	public abstract class CassandraSlicePredicate
	{
		internal abstract SlicePredicate CreateSlicePredicate();
	}

	public class ColumnSlicePredicate : CassandraSlicePredicate
	{
		public ColumnSlicePredicate(List<CassandraType> columns)
		{
			Columns = columns;
		}

		public List<CassandraType> Columns { get; private set; }

		internal override SlicePredicate CreateSlicePredicate()
		{
			return new SlicePredicate {
				Column_names = Columns.Cast<byte[]>().ToList()
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

		public CassandraType Finish { get; private set; }

		public bool Reversed { get; private set; }

		public int Count { get; private set; }

		internal override SlicePredicate CreateSlicePredicate()
		{
			return new SlicePredicate {
				Slice_range = new SliceRange {
					Start = Start,
					Finish = Finish,
					Reversed = Reversed,
					Count = Count
				}
			};
		}
	}
}
