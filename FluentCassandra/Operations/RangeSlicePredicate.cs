using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
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
					Start = (byte[])Start ?? new byte[0],
					Finish = (byte[])Finish ?? new byte[0],
					Reversed = Reversed,
					Count = Count
				}
			};
		}
	}
}
