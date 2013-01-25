using System;
using Xunit;
using FluentCassandra.Types;

namespace FluentCassandra.TypesToDatabase
{
	public class TimeUUIDTypeTest : IUseFixture<CassandraDatabaseSetupFixture>, IDisposable
	{
		private CassandraContext _db;

		public void SetFixture(CassandraDatabaseSetupFixture data)
		{
			var setup = data.DatabaseSetup();
			_db = setup.DB;
		}

		public void Dispose()
		{
			_db.Dispose();
		}

		public const string FamilyName = "StandardTimeUUIDType";
		public const string TestKey = "Test1";

		[Fact]
		public void Save_Guid()
		{
			// arrange
			var family = _db.GetColumnFamily<TimeUUIDType>(FamilyName);
			Guid expected = new Guid("38400000-8cf0-11bd-b23e-10b96e4ef00d");

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			var actual = family.GetColumn(TestKey, expected);

			// assert
			Assert.Equal(expected, (Guid)actual.ColumnName);
		}

		[Fact]
		public void Save_DateTime_Local()
		{
			// arrange
			var family = _db.GetColumnFamily<TimeUUIDType>(FamilyName);
			var expected = new DateTime(2010, 11, 20, 0, 0, 0, DateTimeKind.Local);

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			var actualColumn = family.GetColumn(TestKey, expected);
			var actual = ((DateTime)actualColumn.ColumnName).ToLocalTime();

			// assert
			Assert.Equal(expected, actual);
		}

		[Fact]
		public void Save_DateTime_Universal()
		{
			// arrange
			var family = _db.GetColumnFamily<TimeUUIDType>(FamilyName);
			var expected = new DateTime(2010, 11, 21, 0, 0, 0, DateTimeKind.Utc);

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			var actual = family.GetColumn(TestKey, expected);

			// assert
			Assert.Equal(expected, (DateTime)actual.ColumnName);
		}

		[Fact]
		public void Save_DateTimeOffset()
		{
			// arrange
			var family = _db.GetColumnFamily<TimeUUIDType>(FamilyName);
			var expected = new DateTimeOffset(2010, 11, 22, 0, 0, 0, TimeSpan.Zero);

			// act
			family.InsertColumn(TestKey, expected, Math.PI);
			var actual = family.GetColumn(TestKey, expected);

			// assert
			Assert.Equal(expected, (DateTimeOffset)actual.ColumnName);
		}
	}
}
