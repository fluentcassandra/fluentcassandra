using System;
using NUnit.Framework;

namespace FluentCassandra.Types
{
	[TestFixture]
	public class NullTypeTest
	{
		[Test]
		public void Implicity_Cast_To_Int64()
		{
			// arranage
			long? expected = null;
			dynamic family = new FluentColumnFamily<AsciiType>("Test1", "Test1");

			// act
			long? actual = family.ShouldNotBeFound;

			// assert
			Assert.AreEqual(expected, actual);
		}

		[Test]
		public void Implicity_Cast_To_FluentSuperColumn()
		{
			// arranage
			var expectedName = "ShouldNotBeFound";
			var expectedColumnCount = 0;
			dynamic family = new FluentSuperColumnFamily<AsciiType, AsciiType>("Test1", "SubTest1");

			// act
			FluentSuperColumn actual = family.ShouldNotBeFound;

			// assert
			Assert.AreEqual(expectedName, (string)actual.ColumnName);
			Assert.AreEqual(expectedColumnCount, actual.Columns.Count);
		}
	}
}