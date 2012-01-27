using System;
using System.Linq;
using FluentCassandra.Types;
using NUnit.Framework;

namespace FluentCassandra
{
	[TestFixture]
	public class ReportedIssuesTest
	{
		public const string FamilyName = "Standard";
		public const string TestKey = "Test1";
		private CassandraContext _db;

		[SetUp]
		public void TestInit()
		{
			var setup = new _CassandraSetup();
			_db = setup.DB;
		}

		[TearDown]
		public void TestCleanup()
		{
			_db.Dispose();
		}


		[Test]
		public void CreateRecord_Doesnt_Check_BytesType_Zero_Length()
		{
			// arrange
			var family = _db.GetColumnFamily<AsciiType>(FamilyName);
			
			// act
			// assert
			Assert.Throws<ArgumentException>(delegate {
				dynamic value = family.CreateRecord("");
			});
		}
	}
}
