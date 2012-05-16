using System;
using Xunit;

namespace FluentCassandra.Types
{
	
	public class NullTypeTest
	{
		[Fact]
		public void Implicity_Cast_To_Int64()
		{
			// arranage
			long? expected = null;
			dynamic family = new FluentColumnFamily<AsciiType>("Test1", "Test1");

			// act
			long? actual = family.ShouldNotBeFound;

			// assert
			Assert.Equal(expected, actual);
		}

		[Fact]
		public void Implicity_Cast_To_FluentSuperColumn()
		{
			// arranage
			var expectedName = "ShouldNotBeFound";
			var expectedColumnCount = 0;
			dynamic family = new FluentSuperColumnFamily<AsciiType, AsciiType>("Test1", "SubTest1");

			// act
			FluentSuperColumn actual = family.ShouldNotBeFound;

			// assert
			Assert.Equal(expectedName, (string)actual.ColumnName);
			Assert.Equal(expectedColumnCount, actual.Columns.Count);
		}
	}
}