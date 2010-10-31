using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FluentCassandra.Types;
using FluentCassandra.Operations;
using System.Linq.Expressions;

namespace FluentCassandra
{
	public static class CassandraColumnFamilyOperations
	{
		#region ColumnCount

		public static int ColumnCount<CompareWith>(this CassandraColumnFamily<CompareWith> family, BytesType key, IEnumerable<CompareWith> columnNames)
			where CompareWith : CassandraType
		{
			var op = new ColumnCount(key, new ColumnSlicePredicate(columnNames));
			return family.ExecuteOperation(op);
		}

		public static int ColumnCount<CompareWith>(this CassandraColumnFamily<CompareWith> family, BytesType key, CompareWith columnStart, CompareWith columnEnd, bool reversed = false, int count = 100)
			where CompareWith : CassandraType
		{
			var op = new ColumnCount(key, new RangeSlicePredicate(columnStart, columnEnd, reversed, count));
			return family.ExecuteOperation(op);
		}

		#endregion

		#region InsertColumn

		public static void InsertColumn<CompareWith>(this CassandraColumnFamily<CompareWith> family, BytesType key, IFluentColumn<CompareWith> column)
			where CompareWith : CassandraType
		{
			InsertColumn<CompareWith>(family, key, column.GetPath());
		}

		public static void InsertColumn<CompareWith>(this CassandraColumnFamily<CompareWith> family, BytesType key, FluentColumnPath path)
			where CompareWith : CassandraType
		{
			var columnName = path.Column.ColumnName;
			var columnValue = path.Column.ColumnValue;
			var timestamp = path.Column.ColumnTimestamp;
			var timeToLive = path.Column.ColumnTimeToLive;

			var op = new InsertColumn(key, columnName, columnValue, timestamp, timeToLive);
			family.ExecuteOperation(op);
		}

		public static void InsertColumn<CompareWith>(this CassandraColumnFamily<CompareWith> family, BytesType key, CompareWith columnName, BytesType columnValue)
			where CompareWith : CassandraType
		{
			InsertColumn<CompareWith>(family, key, columnName, columnValue, DateTimeOffset.UtcNow, 1);
		}

		public static void InsertColumn<CompareWith>(this CassandraColumnFamily<CompareWith> family, BytesType key, CompareWith columnName, BytesType columnValue, DateTimeOffset timestamp, int timeToLive)
			where CompareWith : CassandraType
		{
			var op = new InsertColumn(key, columnName, columnValue, timestamp, timeToLive);
			family.ExecuteOperation(op);
		}

		#endregion

		#region GetColumn

		public static IFluentColumn<CompareWith> GetColumn<CompareWith>(this CassandraColumnFamily<CompareWith> family, BytesType key, FluentColumnPath path)
			where CompareWith : CassandraType
		{
			var columnName = (CompareWith)path.Column.ColumnName;
			return GetColumn<CompareWith>(family, key, columnName);
		}

		public static IFluentColumn<CompareWith> GetColumn<CompareWith>(this CassandraColumnFamily<CompareWith> family, BytesType key, CompareWith columnName)
			where CompareWith : CassandraType
		{
			var op = new GetColumn<CompareWith>(key, columnName);
			return family.ExecuteOperation(op);
		}

		#endregion

		#region RemoveColumn

		public static void RemoveColumn<CompareWith>(this CassandraColumnFamily<CompareWith> family, BytesType key, FluentColumnPath path)
			where CompareWith : CassandraType
		{
			var columnName = (CompareWith)path.Column.ColumnName;
			RemoveColumn<CompareWith>(family, key, columnName);
		}

		public static void RemoveColumn<CompareWith>(this CassandraColumnFamily<CompareWith> family, BytesType key, CompareWith columnName)
			where CompareWith : CassandraType
		{
			var op = new RemoveColumn(key, columnName);
			family.ExecuteOperation(op);
		}

		#endregion

		#region RemoveKey

		public static void RemoveKey<CompareWith>(this CassandraColumnFamily<CompareWith> family, BytesType key)
			where CompareWith : CassandraType
		{
			var op = new RemoveKey(key);
			family.ExecuteOperation(op);
		}

		#endregion

		#region GetSingle

		public static IFluentColumnFamily<CompareWith> GetSingle<CompareWith>(this CassandraColumnFamily<CompareWith> family, BytesType key, IEnumerable<CompareWith> columnNames)
			where CompareWith : CassandraType
		{
			var op = new GetColumnFamilySlice<CompareWith>(key, new ColumnSlicePredicate(columnNames));
			return family.ExecuteOperation(op);
		}

		public static IFluentColumnFamily<CompareWith> GetSingle<CompareWith>(this CassandraColumnFamily<CompareWith> family, BytesType key, CompareWith columnStart, CompareWith columnEnd, bool columnsReversed = false, int columnCount = 100)
			where CompareWith : CassandraType
		{
			var op = new GetColumnFamilySlice<CompareWith>(key, new RangeSlicePredicate(columnStart, columnEnd, columnsReversed, columnCount));
			return family.ExecuteOperation(op);
		}

		#endregion

		#region Get

		// queryable

		public static ICassandraQueryable<IFluentColumnFamily<CompareWith>, CompareWith> Get<CompareWith>(this CassandraColumnFamily<CompareWith> family, params BytesType[] keys)
			where CompareWith : CassandraType
		{
			var setup = new CassandraQuerySetup<IFluentColumnFamily<CompareWith>, CompareWith> {
				Keys = keys,
				CreateQueryOperation = (s, slice) => new MultiGetColumnFamilySlice<CompareWith>(s.Keys, slice)
			};
			return ((ICassandraQueryProvider)family).CreateQuery(setup, null);
		}

		public static ICassandraQueryable<IFluentColumnFamily<CompareWith>, CompareWith> Get<CompareWith>(this CassandraColumnFamily<CompareWith> family, BytesType startKey, BytesType endKey, string startToken, string endToken, int keyCount)
			where CompareWith : CassandraType
		{
			var setup = new CassandraQuerySetup<IFluentColumnFamily<CompareWith>, CompareWith> {
				KeyRange = new CassandraKeyRange(startKey, endKey, startToken, endToken, keyCount),
				CreateQueryOperation = (s, slice) => new GetColumnFamilyRangeSlices<CompareWith>(s.KeyRange, slice)
			};
			return ((ICassandraQueryProvider)family).CreateQuery(setup, null);
		}

		public static ICassandraQueryable<IFluentColumnFamily<CompareWith>, CompareWith> Get<CompareWith>(this CassandraColumnFamily<CompareWith> family, BytesType startKey, int keyCount, Expression<Func<IFluentRecordHasFluentColumns<CompareWith>, bool>> expression)
			where CompareWith : CassandraType
		{
			var setup = new CassandraQuerySetup<IFluentColumnFamily<CompareWith>, CompareWith> {
				IndexClause = new CassandraIndexClause<CompareWith>(startKey, keyCount, expression),
				CreateQueryOperation = (s, slice) => new GetColumnFamilyIndexedSlices<CompareWith>(s.IndexClause, slice)
			};
			return ((ICassandraQueryProvider)family).CreateQuery(setup, null);
		}

		// multi_get_slice

		public static IEnumerable<IFluentColumnFamily<CompareWith>> Get<CompareWith>(this CassandraColumnFamily<CompareWith> family, IEnumerable<BytesType> keys, IEnumerable<CompareWith> columnNames)
			where CompareWith : CassandraType
		{
			var op = new MultiGetColumnFamilySlice<CompareWith>(keys, new ColumnSlicePredicate(columnNames));
			return family.ExecuteOperation(op);
		}

		public static IEnumerable<IFluentColumnFamily<CompareWith>> Get<CompareWith>(this CassandraColumnFamily<CompareWith> family, IEnumerable<BytesType> keys, CompareWith columnStart, CompareWith columnEnd, bool columnsReversed = false, int columnCount = 100)
			where CompareWith : CassandraType
		{
			var op = new MultiGetColumnFamilySlice<CompareWith>(keys, new RangeSlicePredicate(columnStart, columnEnd, columnsReversed, columnCount));
			return family.ExecuteOperation(op);
		}

		// get_range_slice

		public static IEnumerable<IFluentColumnFamily<CompareWith>> Get<CompareWith>(this CassandraColumnFamily<CompareWith> family, BytesType startKey, BytesType endKey, string startToken, string endToken, int keyCount, IEnumerable<CompareWith> columnNames)
			where CompareWith : CassandraType
		{
			var op = new GetColumnFamilyRangeSlices<CompareWith>(new CassandraKeyRange(startKey, endKey, startToken, endToken, keyCount), new ColumnSlicePredicate(columnNames));
			return family.ExecuteOperation(op);
		}

		public static IEnumerable<IFluentColumnFamily<CompareWith>> Get<CompareWith>(this CassandraColumnFamily<CompareWith> family, BytesType startKey, BytesType endKey, string startToken, string endToken, int keyCount, CompareWith columnStart, CompareWith columnEnd, bool columnsReversed = false, int columnCount = 100)
			where CompareWith : CassandraType
		{
			var op = new GetColumnFamilyRangeSlices<CompareWith>(new CassandraKeyRange(startKey, endKey, startToken, endToken, keyCount), new RangeSlicePredicate(columnStart, columnEnd, columnsReversed, columnCount));
			return family.ExecuteOperation(op);
		}

		// get_indexed_slices

		public static IEnumerable<IFluentColumnFamily<CompareWith>> Get<CompareWith>(this CassandraColumnFamily<CompareWith> family, BytesType startKey, int keyCount, Expression<Func<IFluentRecordHasFluentColumns<CompareWith>, bool>> expression, IEnumerable<CompareWith> columnNames)
			where CompareWith : CassandraType
		{
			var op = new GetColumnFamilyIndexedSlices<CompareWith>(new CassandraIndexClause<CompareWith>(startKey, keyCount, expression), new ColumnSlicePredicate(columnNames));
			return family.ExecuteOperation(op);
		}

		public static IEnumerable<IFluentColumnFamily<CompareWith>> Get<CompareWith>(this CassandraColumnFamily<CompareWith> family, BytesType startKey, int keyCount, Expression<Func<IFluentRecordHasFluentColumns<CompareWith>, bool>> expression, CompareWith columnStart, CompareWith columnEnd, bool columnsReversed = false, int columnCount = 100)
			where CompareWith : CassandraType
		{
			var op = new GetColumnFamilyIndexedSlices<CompareWith>(new CassandraIndexClause<CompareWith>(startKey, keyCount, expression), new RangeSlicePredicate(columnStart, columnEnd, columnsReversed, columnCount));
			return family.ExecuteOperation(op);
		}

		#endregion
	}
}