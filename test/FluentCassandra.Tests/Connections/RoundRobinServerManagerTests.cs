using System;
using System.Collections.Generic;
using Xunit;

namespace FluentCassandra.Connections.Tests
{
    public class RoundRobinServerManagerTests
    {
        [Fact]
        public void CanBlackListAndCleanQueueTest()
        {
            RoundRobinServerManager target = new RoundRobinServerManager(new ConnectionBuilder("Server=unit-test-1,unit-test-2,unit-test-3"));

            Server srv = new Server("unit-test-4");
            target.Add(srv);

            bool gotServer4 = false;

            for (int i = 0; i < 4; i++)
            {
                Server server = target.Next();
                if (server.ToString().Equals(srv.ToString(), StringComparison.OrdinalIgnoreCase))
                {
                    gotServer4 = true;
                    break;
                }
            }

            Assert.True(gotServer4);

            target.BlackList(srv);

            gotServer4 = false;
            for (int i = 0; i < 4; i++)
            {
                Server server = target.Next();
                if (server.Equals(srv))
                {
                    gotServer4 = true;
                    break;
                }
            }

            Assert.False(gotServer4);
        }

        [Fact]
        public void HasNextWithMoreThanHalfBlacklistedTest()
        {
            RoundRobinServerManager target = new RoundRobinServerManager(new ConnectionBuilder("Server=unit-test-1"));

            Server srv1 = null;
            Server srv2 = new Server("unit-test-2");
            Server srv3 = new Server("unit-test-3");
            Server srv4 = new Server("unit-test-4");
            target.Add(srv2);
            target.Add(srv3);
            target.Add(srv4);
            List<Server> servers = new List<Server> { new Server("unit-test-1"), srv2, srv3, srv4 };

            for (int i = 0; i < 4; i++)
            {
                Server srv = target.Next();
                Assert.True(servers[i].ToString().Equals(srv.ToString(), StringComparison.OrdinalIgnoreCase));
                if(i == 0)
                {
                    srv1 = srv;
                }
            }

            target.BlackList(srv2);
            target.BlackList(srv3);
            Assert.True(target.HasNext);

            target.BlackList(srv1);
            Assert.True(target.HasNext);

            target.BlackList(srv4);
            Assert.False(target.HasNext);
        }
    }
}
