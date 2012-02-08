using System;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class CassandraRangeSlicePredicate : CassandraSlicePredicate
	{
		public CassandraRangeSlicePredicate(CassandraObject start, CassandraObject finish, bool reversed = false, int count = 100)
		{
			Start = start;
			Finish = finish;
			Reversed = reversed;
			Count = count;
		}

		public CassandraObject Start { get; private set; }

		public CassandraObject Finish { get; internal set; }

		public bool Reversed { get; internal set; }

		public int Count { get; internal set; }
	}
}
