using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace FluentCassandra.Bugs
{
	public class Issue28GuidGeneratorInParallelContext
	{
		[Fact]
		public void Test()
		{
			// arrange
			var expected = 10;
			ConcurrentBag<Guid> guidList = new ConcurrentBag<Guid>();
			List<int> list = new List<int>();

			// act
			for (int i = 0; i < 10; i++)
				list.Add(i);

			list.AsParallel().ForAll(index => guidList.Add(GuidGenerator.GenerateTimeBasedGuid()));

			var actual = guidList.Count;

			// assert
			Assert.Equal(expected, actual);
		}
	}
}
