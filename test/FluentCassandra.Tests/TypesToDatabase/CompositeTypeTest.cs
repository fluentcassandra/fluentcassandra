using System;
using System.Linq;
using System.Linq;
using NUnit.Framework;
using FluentCassandra.Types;

namespace FluentCassandra.TypesToDatabase
{
	[TestFixture]
	public class CompositeTypeTest
	{
		public const string FamilyName = "StandardCompositeType";
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
		public void Save_CompositeType()
		{
			// arrange
			var family = _db.GetColumnFamily<CompositeType<LongType, UTF8Type>>(FamilyName);
			var expected = new CompositeType<LongType, UTF8Type>(300L, "string1");

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			var value = family.Get(TestKey).Execute();
			var actual = value.FirstOrDefault().Columns.FirstOrDefault();

			// assert
			Assert.AreEqual(expected, (string)actual.ColumnName);
		}
	}
}