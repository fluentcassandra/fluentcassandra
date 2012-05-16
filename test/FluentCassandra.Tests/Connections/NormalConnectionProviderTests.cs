using System;
using System.Linq;
using Xunit;

namespace FluentCassandra.Connections
{
	
	public class NormalConnectionProviderTests
	{
		/// <summary>
		/// Needed to switch to testing ports since the network timeout was making the tests unbearably long.
		/// </summary>
		private readonly string _failoverConnectionString = "Keyspace=Testing;Connection Timeout=1;Server=127.0.0.1:1234,127.0.0.1:4567,127.0.0.1";

		[Fact]
		public void Fails_Over()
		{
			// arrange
			var expectedHost = "127.0.0.1";
			var expectedPort = Server.DefaultPort;

			// act
			var result = new ConnectionBuilder(_failoverConnectionString);
			var provider = ConnectionProviderFactory.Get(result);
			var conn = provider.Open();
			var actualHost = conn.Server.Host;
			var actualPort = conn.Server.Port;

			// assert
			Assert.Equal(expectedHost, actualHost);
			Assert.Equal(expectedPort, actualPort);
		}
	}
}
