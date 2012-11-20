using System;
using System.Linq;
using Apache.Cassandra;
using System.IO;
using Thrift.Transport;

namespace FluentCassandra.Operations
{
	public class CassandraOperationException : CassandraException
	{
        public bool IsClientHealthy { get; set; }
        public bool ShouldRetry { get; set; }

		public CassandraOperationException(AuthenticationException exc)
			: base(exc.Why, exc) 
        {
            IsClientHealthy = true;
            ShouldRetry = false;
        }

		public CassandraOperationException(InvalidRequestException exc)
			: base(exc.Why, exc) 
        {
            IsClientHealthy = true;
            ShouldRetry = false;
        }

		public CassandraOperationException(AuthorizationException exc)
			: base(exc.Why, exc) 
        {
            IsClientHealthy = true;
            ShouldRetry = false;
        }

		public CassandraOperationException(UnavailableException exc)
			: base("Cassandra server is unavailable.", exc) 
        {
            IsClientHealthy = false;
            ShouldRetry = true;
        }

		public CassandraOperationException(TimedOutException exc)
			: base("Connection to Cassandra has timed out.", exc)
        {
            IsClientHealthy = true;
            ShouldRetry = true;
        }
	
        public CassandraOperationException(IOException exc)
            : base(exc.Message, exc) 
        {
            IsClientHealthy = false;
            ShouldRetry = true;
        }

        public CassandraOperationException(NotFoundException exc)
            : base(exc.Message) 
        { 
            IsClientHealthy = false;
            ShouldRetry = true;
        }

        public CassandraOperationException(TTransportException exc)
            : base(exc.Message)
        {
            IsClientHealthy = true;
            ShouldRetry = false;
        }

        public CassandraOperationException(Exception exc)
            : base(exc.Message, exc)
        {
            //Unknown exception type, lets be safe and mark this node as down and not try to retry as well.
            IsClientHealthy = false;
            ShouldRetry = false;
        }

        public CassandraOperationException(string message, bool isClientHealthy, bool shouldRetry)
            : base(message)
        {
            IsClientHealthy = isClientHealthy;
            ShouldRetry = shouldRetry;
        }

        public CassandraOperationException(string message, Exception innerException, bool isClientHealthy, bool shouldRetry)
            : base(message, innerException)
        {
            IsClientHealthy = isClientHealthy;
            ShouldRetry = shouldRetry;
        }
    }
}
