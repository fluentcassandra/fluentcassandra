using FluentCassandra.Types;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.ComponentModel;
using System.Text;
using System.Linq;

namespace FluentCassandra.Test
{
	[TestClass]
	public class UTF8TypeTest
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
		public void CassandraType_Cast_String()
		{
			// arranage
			string expected = "I am casted!";
			UTF8Type type = expected;

			// act
			CassandraType actual = type;

			// assert
			Assert.AreEqual(expected, (string)actual);
		}

		[TestMethod]
		public void Implicit_Cast_From_String()
		{
			// arrange
			string expected = "I am casted!";

			// act
			UTF8Type actual = expected;

			// assert
			Assert.AreEqual(expected, (string)actual);
		}

		[TestMethod]
		public void Implicit_Cast_To_String()
		{
			// arrange
			string expected = "I am casted!";
			UTF8Type type = expected;

			// act
			string actual = type;

			// assert
			Assert.AreEqual(expected, actual);
		}

		[TestMethod]
		public void Implicity_Cast_To_ByteArray()
		{
			// arrange
			string value = "I am casted!";
			byte[] expected = Encoding.UTF8.GetBytes(value);
			UTF8Type type = value;

			// act
			byte[] actual = type;

			// assert
			Assert.IsTrue(actual.SequenceEqual(expected));
		}

		[TestMethod]
		public void Operator_EqualTo()
		{
			// arrange
			var value = "I am equal.";
			UTF8Type type = value;

			// act
			bool actual = type == value;

			// assert
			Assert.IsTrue(actual);
		}

		[TestMethod]
		public void Operator_NotEqualTo()
		{
			// arrange
			var value = "I am equal.";
			UTF8Type type = value;

			// act
			bool actual = type != value;

			// assert
			Assert.IsFalse(actual);
		}
	}
}