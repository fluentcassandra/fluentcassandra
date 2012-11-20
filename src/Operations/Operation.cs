using System;
using System.Diagnostics;
using FluentCassandra.Apache.Cassandra;
using FluentCassandra.Thrift.Transport;

namespace FluentCassandra.Operations
{
	public abstract class Operation<TResult>
	{
		private int _executionCount;

		public Operation()
		{
			_executionCount = 0;

			HasError = false;
		}

		public CassandraContext Context { get; set; }
		public CassandraSession Session { get; set; }

		public bool HasError { get; protected set; }
		public CassandraOperationException Error { get; protected set; }

		public virtual bool TryExecute(out TResult result)
		{
			if (_executionCount > Session.ConnectionBuilder.MaxRetries)
			{
				result = default(TResult);
				return !HasError;
			}

			_executionCount++;

			try
			{
				result = Execute();
			}
			catch (AuthenticationException exc)
			{
				ExceptionOccurred(new CassandraOperationException(exc));
				result = default(TResult);
			}
			catch (AuthorizationException exc)
			{
				ExceptionOccurred(new CassandraOperationException(exc));
				result = default(TResult);
			}
			catch (InvalidRequestException exc)
			{
				ExceptionOccurred(new CassandraOperationException(exc));
				result = default(TResult);
			}
			catch (UnavailableException exc)
			{
				ExceptionOccuredRetryExecution(new CassandraOperationException(exc), out result);
			}
			catch (TimeoutException exc)
			{
				ExceptionOccuredRetryExecution(new CassandraOperationException(exc), out result);
			}
			catch (TimedOutException exc)
			{
				ExceptionOccuredRetryExecution(new CassandraOperationException(exc), out result);
			}
			catch (TTransportException exc)
			{
				ExceptionOccuredRetryExecution(new CassandraOperationException(exc), out result);
			}
			catch (Exception exc)
			{
				ExceptionOccurred(new CassandraOperationException(exc));
				result = default(TResult);
			}

			return !HasError;
		}

		private void ExceptionOccuredRetryExecution(CassandraOperationException exc, out TResult result)
		{
			ExceptionOccurred(exc);
			Session.MarkCurrentConnectionAsUnhealthy(exc);

			TryExecute(out result);
		}

		private void ExceptionOccurred(CassandraOperationException exc)
		{
			Debug.WriteLine(exc);

			HasError = true;
			Error = exc;
		}

		public abstract TResult Execute();
	}
}
