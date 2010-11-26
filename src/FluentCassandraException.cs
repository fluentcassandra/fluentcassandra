using System;

namespace FluentCassandra
{
	public class FluentCassandraException : Exception
	{
		public FluentCassandraException(string message)
			: base(message) { }

		public FluentCassandraException(string message, Exception innerException)
			: base(message, innerException) { }
	}
}