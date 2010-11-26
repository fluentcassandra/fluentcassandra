using System;

namespace FluentCassandra
{
	public class CassandraException : Exception
	{
		public CassandraException(string message)
			: base(message) { }

		public CassandraException(string message, Exception innerException)
			: base(message, innerException) { }
	}
}
