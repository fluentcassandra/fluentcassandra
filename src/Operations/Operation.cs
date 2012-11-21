using System;
using System.Diagnostics;
using System.IO;
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
                //We have to set HasError to false after execution.
                //In the event that we retry HasError is set to true from 
                //the initial exception. When we do retry and its good
                //We return false because HasError == true and the 
                //exception is thrown even though we may have
                //retried successfully. 
                HasError = false;
                Error = null;
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
                ExceptionOccuredRetryExecution(new CassandraOperationException(exc), true, out result);
            }
            catch (TimeoutException exc)
            {
                ExceptionOccuredRetryExecution(new CassandraOperationException(exc), false, out result);
            }
            catch (TimedOutException exc)
            {
                ExceptionOccuredRetryExecution(new CassandraOperationException(exc), false, out result);
            }
            catch (TTransportException exc)
            {
                ExceptionOccuredRetryExecution(new CassandraOperationException(exc), true, out result);
            }
            catch (IOException exc)
            {
                ExceptionOccuredRetryExecution(new CassandraOperationException(exc), true, out result);
            }
            catch (NotFoundException exc)
            {
                ExceptionOccuredRetryExecution(new CassandraOperationException(exc), true, out result);
            }
            catch (Exception exc)
            {
                ExceptionOccurred(new CassandraOperationException(exc));
                result = default(TResult);
            }

			return !HasError;
		}

		private void ExceptionOccuredRetryExecution(CassandraOperationException exc, bool markClientAsUnHealthy, out TResult result)
		{
			ExceptionOccurred(exc);

            if (markClientAsUnHealthy)
            { 
			    Session.MarkCurrentConnectionAsUnhealthy(exc);
            }

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
