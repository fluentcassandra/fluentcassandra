using System;
using Apache.Cassandra;
using FluentCassandra.Types;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace FluentCassandra.Operations
{
	public class CassandraIndexClause<CompareWith>
		where CompareWith : CassandraType
	{
		public CassandraIndexClause(BytesType startKey, int count, Expression<Func<dynamic, bool>> expression)
		{
			StartKey = startKey;
			Count = count;
			Expression = expression;
		}

		public BytesType StartKey { get; private set; }
		public int Count { get; private set; }
		public Expression Expression { get; private set; }
		
		public List<Apache.Cassandra.IndexExpression> CompiledExpressions { get; private set; }

		internal IndexClause CreateIndexClause()
		{
			return new IndexClause {
				Start_key = StartKey,
				Count = Count,
				Expressions = CompiledExpressions
			};
		}
	}
}
