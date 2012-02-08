using System;
using System.Collections.Generic;
using System.Linq;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class CassandraQuerySetup
	{
		public CassandraObject Key
		{
			get
			{
				if (Keys != null)
					return Keys.FirstOrDefault();

				if (KeyRange != null)
					return KeyRange.StartKey;

				return new byte[0];
			}
		}

		public IEnumerable<CassandraObject> Keys
		{
			get;
			internal set;
		}

		public CassandraKeyRange KeyRange
		{
			get;
			internal set;
		}

		public CassandraObject SuperColumnName
		{
			get;
			internal set;
		}

		public CassandraIndexClause IndexClause
		{
			get;
			internal set;
		}
	}
	
	public class CassandraQuerySetup<TResult, CompareWith> : CassandraQuerySetup
		where CompareWith : CassandraObject
	{
		public Func<CassandraQuerySetup, CassandraSlicePredicate, QueryableColumnFamilyOperation<TResult>> CreateQueryOperation
		{
			get;
			internal set;
		}
	}
}