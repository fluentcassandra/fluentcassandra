using System;
using FluentCassandra.Types;
using Xunit;

namespace FluentCassandra.Integration.Tests.Operations
{
	
	public class GetSliceTest : IUseFixture<CassandraDatabaseSetupFixture>, IDisposable
	{
		private CassandraContext _db;
		private CassandraColumnFamily<AsciiType> _family;
		private CassandraSuperColumnFamily<AsciiType, AsciiType> _superFamily;

		public void SetFixture(CassandraDatabaseSetupFixture data)
		{
			var setup = data.DatabaseSetup();
			_db = setup.DB;
			_family = setup.Family;
			_superFamily = setup.SuperFamily;
		}

		public void Dispose()
		{
			_db.Dispose();
		}

		private readonly string _testKey = "Test1";
		private readonly string _testName = "Test1";
		private readonly string _testSuperName = "SubTest1";

		[Fact]
		public void Standard_GetSlice_Columns()
		{
			// arrange
			int expectedCount = 2;

			// act
			var columns = _family.GetSingle(_testKey, new AsciiType[] { "Test1", "Test2" });

			// assert
			Assert.Equal(expectedCount, columns.Columns.Count);
		}

		[Fact]
		public void Super_GetSlice_Columns()
		{
			// arrange
			int expectedCount = 2;

			// act
			var columns = _superFamily.GetSingleSuperColumn(_testKey, _testSuperName, new AsciiType[] { "Test1", "Test2" });

			// assert
			Assert.Equal(expectedCount, columns.Columns.Count);
		}

		[Fact]
		public void Super_GetSuperSlice_Columns()
		{
			// arrange
			int expectedCount = 1;

			// act
			var columns = _superFamily.GetSingle(_testKey, new AsciiType[] { _testSuperName });

			// assert
			Assert.Equal(expectedCount, columns.Columns.Count);
		}

		[Fact]
		public void Standard_GetSlice_Range()
		{
			// arrange
			int expectedCount = 2;

			// act
			var columns = _family.GetSingle(_testKey, _testName, null, columnCount: 2);

			// assert
			Assert.Equal(expectedCount, columns.Columns.Count);
		}

		[Fact]
		public void Super_GetSlice_Range()
		{
			// arrange
			int expectedCount = 2;

			// act
			var columns = _superFamily.GetSingleSuperColumn(_testKey, _testSuperName, _testName, null, count: 2);

			// assert
			Assert.Equal(expectedCount, columns.Columns.Count);
		}

		[Fact]
		public void Super_GetSuperSlice_Range()
		{
			// arrange
			int expectedCount = 1;

			// act
			var columns = _superFamily.GetSingle(_testKey, _testSuperName, null, count: 1);

			// assert
			Assert.Equal(expectedCount, columns.Columns.Count);
		}
	}
}
