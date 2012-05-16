using System;
using System.Linq;
using Xunit;

namespace FluentCassandra.Connections
{
	
	public class ConnectionProviderTests
	{
		[Fact]
		public void NormalConnectionProvider()
		{
			// arrange
			var expected = typeof(NormalConnectionProvider);
			var connectionString = "Keyspace=Testing";

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
			var connectionString = "Keyspace=Testing;Pooling=True";

			// act
			var result = new ConnectionBuilder(connectionString);
			var actual = ConnectionProviderFactory.Get(result).GetType();

			// assert
			Assert.Equal(expected, actual);
		}
	}
}
