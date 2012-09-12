using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;
using FluentCassandra.Connections;

namespace FluentCassandra.Bugs
{
    public class Issue65ServerTimeoutLost
    {
        [Fact]
        public void TestSingleServerWithHostAndPort()
        {
            Assert.Equal(5, new ConnectionBuilder("Server=host:123;Connection Timeout=5").Servers[0].Timeout);
        }

        [Fact]
        public void TestSingleServerWithHostAndDefaultPort()
        {
            Assert.Equal(5, new ConnectionBuilder("Server=host;Connection Timeout=5").Servers[0].Timeout);
        }

        [Fact]
        public void TestMultipleServersWithHostAndPort()
        {
            var servers = new ConnectionBuilder("Server=host:123,host2:456,host3:789;Connection Timeout=5").Servers;
            Assert.Equal(3, servers.Count);
            foreach (var server in servers)
            {
                Assert.Equal(5, server.Timeout);
            }
        }

        [Fact]
        public void TestMultipleServersWithHostAndDefaultPort()
        {
            var servers = new ConnectionBuilder("Server=host,host2,host3;Connection Timeout=5").Servers;
            Assert.Equal(3, servers.Count);
            foreach (var server in servers)
            {
                Assert.Equal(5, server.Timeout);
            }
        }

        [Fact]
        public void TestMultipleServersWithHostAndMixedPorts()
        {
            var servers = new ConnectionBuilder("Server=host:123,host2,host3:789;Connection Timeout=5").Servers;
            Assert.Equal(3, servers.Count);
            foreach (var server in servers)
            {
                Assert.Equal(5, server.Timeout);
            }
        }
    }
}
