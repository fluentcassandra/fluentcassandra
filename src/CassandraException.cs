using System;

namespace FluentCassandra
{
	public class CassandraException : Exception
	{
		public bool IsClientHealthy { get; set; }
		public bool ShouldRetry { get; set; }

		public CassandraException(string message)
			: base(message) { }

		public CassandraException(string message, Exception innerException)
			: base(message, innerException) { }

        public CassandraException(string message, bool isHealthy, bool shouldRetry)
            : base(message)
        {
            IsClientHealthy = isHealthy;
            ShouldRetry = shouldRetry;
        }

        public CassandraException(string message, Exception innerException, bool isHealthy, bool shouldRetry)
            : base(message, innerException)
        {
            IsClientHealthy = isHealthy;
            ShouldRetry = shouldRetry;
        }
	}
}
