using FluentCassandra.Types;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.ComponentModel;

namespace FluentCassandra.Test
{
	[TestClass]
	public class AsciiTypeTest
	{
		public TestContext TestContext { get; set; }

		#region Additional test attributes
		// 
		//You can use the following additional attributes as you write your tests:
		//
		//Use ClassInitialize to run code before running the first test in the class
		//[ClassInitialize()]
		//public static void MyClassInitialize(TestContext testContext)
		//{
		//}
		//
		//Use ClassCleanup to run code after all tests in a class have run
		//[ClassCleanup()]
		//public static void MyClassCleanup()
		//{
		//}
		//
		//Use TestInitialize to run code before running each test
		//[TestInitialize()]
		//public void MyTestInitialize()
		//{
		//}
		//
		//Use TestCleanup to run code after each test has run
		//[TestCleanup()]
		//public void MyTestCleanup()
		//{
		//}
		//
		#endregion

		[TestMethod]
		public void Implicit_Cast_From_String()
		{
			// arrange
			string expected = "I am casted!";

			// act
			AsciiType actual = expected;

			// assert
			Assert.AreEqual(expected, (string)actual);
		}

		[TestMethod]
		public void Implicit_Cast_To_String()
		{
			// arrange
			string expected = "I am casted!";
			AsciiType type = expected;

			// act
			string actual = type;

			// assert
			Assert.AreEqual(expected, actual);
		}
	}
}