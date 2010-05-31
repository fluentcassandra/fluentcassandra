using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace FluentCassandra.Connection.Test
{
	[TestClass]
	public class ConnectionProviderTest
	{
		[TestMethod]
		public void NormalConnectionProvider()
		{
			// arrange
			var expected = typeof(NormalConnectionProvider);
			var connectionString = "Keyspace=Testing;Provider=Normal";

			// act
			var result = new ConnectionBuilder(connectionString);
			var actual = result.Provider.GetType();

			// assert
			Assert.AreEqual(expected, actual);
		}

		[TestMethod]
		public void FailoverConnectionProvider()
		{
			// arrange
			var expected = typeof(FailoverConnectionProvider);
			var connectionString = "Keyspace=Testing;Provider=Failover;Timeout=1";

			// act
			var result = new ConnectionBuilder(connectionString);
			var actual = result.Provider.GetType();

			// assert
			Assert.AreEqual(expected, actual);
		}
	}
}
