using System;
using System.Linq;
using Xunit;
using System.Configuration;

namespace FluentCassandra.Connections
{
	
	public class NormalConnectionProviderTests
	{
		/// <summary>
		/// Needed to switch to testing ports since the network timeout was making the tests unbearably long.
		/// </summary>
        private readonly string _failoverConnectionString = "Keyspace=" + ConfigurationManager.AppSettings["TestKeySpace"] + ";Connection Timeout=1;Server=" + ConfigurationManager.AppSettings["TestServer"] + ":" + ConfigurationManager.AppSettings["TestPort"] + "";

		[Fact]
		public void Fails_Over()
		{
			// arrange
			var expectedHost = ConfigurationManager.AppSettings["TestServer"];
			var expectedPort = Convert.ToInt16(ConfigurationManager.AppSettings["TestPort"]);

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
