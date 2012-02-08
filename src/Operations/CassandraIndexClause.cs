using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class CassandraIndexClause
	{
		public CassandraIndexClause(CassandraObject startKey, int count, Expression<Func<IFluentRecordExpression, bool>> expression)
			: this(startKey, count, (Expression)expression) { }

		protected internal CassandraIndexClause(CassandraObject startKey, int count, Expression expression)
		{
			StartKey = startKey;
			Count = count;
			Expression = expression;
		}

		private List<Apache.Cassandra.IndexExpression> _compiledExpressions;

		public CassandraObject StartKey { get; private set; }
		public int Count { get; private set; }
		public Expression Expression { get; private set; }

		public List<Apache.Cassandra.IndexExpression> CompiledExpressions
		{
			get
			{
				if (_compiledExpressions == null)
					_compiledExpressions = CassandraIndexClauseBuilder.Evaluate(Expression);

				return _compiledExpressions;
			}
		}
	}
}
