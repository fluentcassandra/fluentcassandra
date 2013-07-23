using System;

namespace FluentCassandra.Operations
{
	public class SimpleOperation<TResult> : Operation<TResult>
	{
		private readonly Func<SimpleOperation<TResult>, TResult> _operation;

		public SimpleOperation(Func<SimpleOperation<TResult>, TResult> operation)
		{
			_operation = operation;
		}

		public override TResult Execute()
		{
			return _operation(this);
		}
	}
}
