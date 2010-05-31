using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace FluentCassandra.Test.Connection
{
	[TestClass]
	public class FailoverConnectionProviderTest
	{
		private static string ConnectionString = "Keyspace=Testing;Provider=Failover;Timeout=1;Server=192.168.100.100,192.168.100.101,127.0.0.1";

		/// <summary>
		/// Needed to switch to testing ports since the network timeout was making the tests unbearably long.
		/// </summary>
		private static string FailoverConnectionString = "Keyspace=Testing;Provider=Failover;Timeout=1;Server=127.0.0.1:1234,127.0.0.1:4567,127.0.0.1";

		[TestMethod]
		public void First_Connection()
		{
			// arrange
			var expected = "192.168.100.100";

			// act
			var result = new ConnectionBuilder(ConnectionString);
			var provider = result.Provider;
			var conn = provider.CreateNewConnection();
			var actual = conn.Server.Host;

			// assert
			Assert.AreEqual(expected, actual);
		}

		[TestMethod]
		public void Second_Connection()
		{
			// arrange
			var expected = "192.168.100.101";

			// act
			var result = new ConnectionBuilder(ConnectionString);
			var provider = result.Provider;
			var conn = provider.CreateNewConnection();
			conn = provider.CreateNewConnection();
			var actual = conn.Server.Host;

			// assert
			Assert.AreEqual(expected, actual);
		}

		[TestMethod]
		public void Third_Connection()
		{
			// arrange
			var expected = "127.0.0.1";

			// act
			var result = new ConnectionBuilder(ConnectionString);
			var provider = result.Provider;
			var conn = provider.CreateNewConnection();
			conn = provider.CreateNewConnection();
			conn = provider.CreateNewConnection();
			var actual = conn.Server.Host;

			// assert
			Assert.AreEqual(expected, actual);
		}

		[TestMethod]
		public void Fails_Over()
		{
			// arrange
			var expectedHost = "127.0.0.1";
			var expectedPort = Server.DefaultPort;

			// act
			var result = new ConnectionBuilder(FailoverConnectionString);
			var provider = result.Provider;
			var conn = provider.Open();
			var actualHost = conn.Server.Host;
			var actualPort = conn.Server.Port;

			// assert
			Assert.AreEqual(expectedHost, actualHost);
			Assert.AreEqual(expectedPort, actualPort);
		}
	}
}
