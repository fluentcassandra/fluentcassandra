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
			var connectionString = "";

			// act
			var result = new ConnectionBuilder(connectionString);
			var actual = result.Provider.GetType();

			// assert
			Assert.AreEqual(expected, actual);
		}

		[TestMethod]
		public void PooledConnectionProvider()
		{
			// arrange
			var expected = typeof(PooledConnectionProvider);
			var connectionString = "Pooled=True";

			// act
			var result = new ConnectionBuilder(connectionString);
			var actual = result.Provider.GetType();

			// assert
			Assert.AreEqual(expected, actual);
		}
	}
}
