using System;
using System.Linq;
using NUnit.Framework;

namespace FluentCassandra
{
	[SetUpFixture]
	public class TestSetup
	{
		[SetUp]
		public void RunBeforeAnyTests()
		{
			// refresh the entire database
			new CassandraDatabaseSetup(volitile: true);
		}
	}
}