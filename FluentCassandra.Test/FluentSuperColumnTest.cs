using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using FluentCassandra.Types;

namespace FluentCassandra.Test
{
	[TestClass]
	public class FluentSuperColumnTest
	{
		[TestMethod]
		public void Constructor_Test()
		{
			// arrange
			var colName = "This is a test name";
			var col1 = new FluentColumn<AsciiType> { Name = "Test1", Value = 300M };
			var col2 = new FluentColumn<AsciiType> { Name = "Test2", Value = "Hello" };

			// act
			var actual = new FluentSuperColumn<AsciiType, AsciiType>();
			actual.Name = colName;
			actual.Columns.Add(col1);
			actual.Columns.Add(col2);

			// assert
			Assert.AreEqual(colName, (string)actual.Name);
			Assert.AreEqual(2, actual.Columns.Count);
		}

		[TestMethod]
		public void Constructor_Dynamic_Test()
		{
			// arrange
			var colName = "This is a test name";
			var col1 = "Test1";
			var colValue1 = 300M;
			var col2 = "Test2";
			var colValue2 = "Hello";

			// act
			dynamic actual = new FluentSuperColumn<AsciiType, AsciiType>();
			actual.Name = colName;
			actual.Test1 = colValue1;
			actual.Test2 = colValue2;

			// assert
			Assert.AreEqual(colName, (string)actual.Name);
			//Assert.AreEqual(2, actual.Columns.Count);
			Assert.AreEqual(colValue1, (decimal)actual.Test1);
			Assert.AreEqual(colValue1, (decimal)actual[col1]);
			Assert.AreEqual(colValue2, (string)actual.Test2);
			Assert.AreEqual(colValue2, (string)actual[col2]);
		}
	}
}
