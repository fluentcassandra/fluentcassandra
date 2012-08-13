using System;
using System.Linq;
using Xunit;
using System.Configuration;

namespace FluentCassandra.Connections
{
	
	public class ConnectionProviderTests
	{
		[Fact]
		public void NormalConnectionProvider()
		{
			// arrange
			var expected = typeof(NormalConnectionProvider);
            var connectionString = "Keyspace=" + ConfigurationManager.AppSettings["TestKeySpace"];

			// act
			var result = new ConnectionBuilder(connectionString);
			var actual = ConnectionProviderFactory.Get(result).GetType();

			// assert
			Assert.Equal(expected, actual);
		}

		[Fact]
		public void PooledConnectionProvider()
		{
			// arrange
			var expected = typeof(PooledConnectionProvider);
			var connectionString = "Keyspace=" + ConfigurationManager.AppSettings["TestKeySpace"] + ";Pooling=True";

			// act
			var result = new ConnectionBuilder(connectionString);
			var actual = ConnectionProviderFactory.Get(result).GetType();

			// assert
			Assert.Equal(expected, actual);
		}
	}
}
