using System;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class CassandraRangeSlicePredicate : CassandraSlicePredicate
	{
		public CassandraRangeSlicePredicate(CassandraType start, CassandraType finish, bool reversed = false, int count = 100)
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
	}
}
