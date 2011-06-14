using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using FluentCassandra.Connections;

namespace FluentCassandra.Tests.Connections
{
	[TestFixture]
	public class ConnectionProviderTest
	{
		[Test]
		public void NormalConnectionProvider()
		{
			// arrange
			var expected = typeof(NormalConnectionProvider);
			var connectionString = "Keyspace=Testing";

			// act
			var result = new ConnectionBuilder(connectionString);
			var actual = ConnectionProviderFactory.Get(result).GetType();

			// assert
			Assert.AreEqual(expected, actual);
		}

		[Test]
		public void PooledConnectionProvider()
		{
			// arrange
			var expected = typeof(PooledConnectionProvider);
			var connectionString = "Keyspace=Testing;Pooling=True";

			// act
			var result = new ConnectionBuilder(connectionString);
			var actual = ConnectionProviderFactory.Get(result).GetType();

			// assert
			Assert.AreEqual(expected, actual);
		}
	}
}
