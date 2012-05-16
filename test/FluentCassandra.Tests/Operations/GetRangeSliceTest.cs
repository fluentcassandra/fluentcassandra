using System;
using System.Linq;
using Xunit;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	
	public class GetRangeSliceTest : IUseFixture<CassandraDatabaseSetupFixture>, IDisposable
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

		private const string _testKey = "Test1";
		private const string _testKey2 = "Test2";
		private const string _testName = "Test1";
		private const string _testSuperName = "SubTest1";

		[Fact]
		public void Standard_GetSlice_Columns()
		{
			// arrange
			int expectedCount = 2;

			// act
			var columns = _family
				.Get(_testKey, _testKey2)
				.FetchColumns(new AsciiType[] { "Test1", "Test2" })
				.Execute();

			// assert
			Assert.Equal(expectedCount, columns.Count());
		}

		[Fact]
		public void Super_GetSlice_Columns()
		{
			// arrange
			int expectedCount = 2;

			// act
			var columns = _superFamily
				.Get(_testKey, _testKey2)
				.ForSuperColumn(_testSuperName)
				.FetchColumns(new AsciiType[] { "Test1", "Test2" })
				.Execute();

			// assert
			Assert.Equal(expectedCount, columns.Count());
		}

		[Fact]
		public void Super_GetSuperSlice_Columns()
		{
			// arrange
			int expectedCount = 2;

			// act
			var columns = _superFamily
				.Get(_testKey, _testKey2)
				.FetchColumns(new AsciiType[] { _testSuperName })
				.Execute();

			// assert
			Assert.Equal(expectedCount, columns.Count());
		}

		[Fact]
		public void Standard_GetSlice_Range()
		{
			// arrange
			int expectedCount = 2;

			// act
			var columns = _family
				.Get(_testKey, _testKey2)
				.StartWithColumn(_testName)
				.TakeColumns(2)
				.Execute();

			// assert
			Assert.Equal(expectedCount, columns.Count());
		}

		[Fact]
		public void Super_GetSlice_Range()
		{
			// arrange
			int expectedCount = 2;

			// act
			var columns = _superFamily
				.Get(_testKey, _testKey2)
				.ForSuperColumn(_testSuperName)
				.StartWithColumn(_testName)
				.TakeColumns(2)
				.Execute();

			// assert
			Assert.Equal(expectedCount, columns.Count());
		}

		[Fact]
		public void Super_GetSuperSlice_Range()
		{
			// arrange
			int expectedCount = 2;

			// act
			var columns = _superFamily
				.Get(_testKey, _testKey2)
				.StartWithColumn(_testSuperName)
				.TakeColumns(1)
				.Execute();

			// assert
			Assert.Equal(expectedCount, columns.Count());
		}
	}
}
