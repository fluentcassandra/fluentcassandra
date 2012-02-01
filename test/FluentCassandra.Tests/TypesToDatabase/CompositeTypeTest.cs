using System;
using System.Linq;
using FluentCassandra.Types;
using NUnit.Framework;

namespace FluentCassandra.TypesToDatabase
{
	[TestFixture]
	public class CompositeTypeTest
	{
		public const string FamilyName = "StandardCompositeType";
		public const string TestKey = "Test1";
		private CassandraContext _db;

		[TestFixtureSetUp]
		public void TestInit()
		{
			var setup = new CassandraDatabaseSetup();
			_db = setup.DB;
		}

		[TestFixtureTearDown]
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
			Assert.AreEqual((object)expected, (object)actual.ColumnName);
		}
	}
}