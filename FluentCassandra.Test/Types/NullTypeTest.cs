using FluentCassandra.Types;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.ComponentModel;
using System.Text;
using System.Linq;

namespace FluentCassandra.Test.Types
{
	[TestClass]
	public class NullTypeTest
	{
		[TestMethod]
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

		[TestMethod]
		public void Implicity_Cast_To_FluentSuperColumn()
		{
			// arranage
			var expectedName = "ShouldNotBeFound";
			var expectedColumnCount = 0;
			dynamic family = new FluentSuperColumnFamily<AsciiType, AsciiType>("Test1", "SubTest1");

			// act
			FluentSuperColumn<AsciiType, AsciiType> actual = family.ShouldNotBeFound;

			// assert
			Assert.AreEqual(expectedName, (string)actual.ColumnName);
			Assert.AreEqual(expectedColumnCount, actual.Columns.Count);
		}
	}
}