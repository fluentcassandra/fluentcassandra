using System;
using System.Collections.Generic;
using FluentCassandra.Types;
using FluentCassandra.Operations;
using System.Linq.Expressions;

namespace FluentCassandra
{
	public static class CassandraSuperColumnFamilyOperations
	{
		#region ColumnCount

		public static int ColumnCount(this CassandraSuperColumnFamily family, CassandraType key, IEnumerable<CassandraType> columnNames)
		{
			var op = new ColumnCount(key, new ColumnSlicePredicate(columnNames));
			return family.ExecuteOperation(op);
		}

		public static int ColumnCount(this CassandraSuperColumnFamily family, CassandraType key, CassandraType columnStart, CassandraType columnEnd, bool reversed = false, int count = 100)
		{
			var op = new ColumnCount(key, new RangeSlicePredicate(columnStart, columnEnd, reversed, count));
			return family.ExecuteOperation(op);
		}

		public static int SuperColumnCount(this CassandraSuperColumnFamily family, CassandraType key, CassandraType superColumnName, IEnumerable<CassandraType> columnNames)
		{
			var op = new ColumnCount(key, superColumnName, new ColumnSlicePredicate(columnNames));
			return family.ExecuteOperation(op);
		}

		public static int SuperColumnCount(this CassandraSuperColumnFamily family, CassandraType key, CassandraType superColumnName, CassandraType columnStart, CassandraType columnEnd, bool reversed = false, int count = 100)
		{
			var op = new ColumnCount(key, superColumnName, new RangeSlicePredicate(columnStart, columnEnd, reversed, count));
			return family.ExecuteOperation(op);
		}

		#endregion

		#region InsertColumn

		public static void InsertColumn(this CassandraSuperColumnFamily family, CassandraType key, FluentColumn column)
		{
			InsertColumn(family, key, column.GetPath());
		}

		public static void InsertColumn(this CassandraSuperColumnFamily family, CassandraType key, FluentColumnPath path)
		{
			var superColumnName = path.SuperColumn.ColumnName;
			var name = path.Column.ColumnName;
			var value = path.Column.ColumnValue;
			var timestamp = path.Column.ColumnTimestamp;
			var timeToLive = path.Column.ColumnSecondsUntilDeleted;

			var op = new InsertColumn(key, superColumnName, name, value, timestamp, timeToLive);
			family.ExecuteOperation(op);
		}

		public static void InsertColumn(this CassandraSuperColumnFamily family, CassandraType key, CassandraType superColumnName, CassandraType name, BytesType value)
		{
			InsertColumn(family, key, superColumnName, name, value, DateTimeOffset.UtcNow, null);
		}

		public static void InsertColumn(this CassandraSuperColumnFamily family, CassandraType key, CassandraType superColumnName, CassandraType name, BytesType value, DateTimeOffset timestamp, int? timeToLive)
		{
			var op = new InsertColumn(key, superColumnName, name, value, timestamp, timeToLive);
			family.ExecuteOperation(op);
		}

		#endregion

		#region AddColumn

		public static void InsertColumn(this CassandraSuperColumnFamily family, CassandraType key, CassandraType superColumnName, CassandraType columnName, long columnValue)
		{
			var op = new AddColumn(key, superColumnName, columnName, columnValue);
			family.ExecuteOperation(op);
		}

		#endregion

		#region GetColumn

		public static FluentColumn GetColumn(this CassandraSuperColumnFamily family, CassandraType key, FluentColumnPath path)
		{
			var columnName = path.Column.ColumnName;
			var superColumnName = path.SuperColumn.ColumnName;
			return GetColumn(family, key, superColumnName, columnName);
		}

		public static FluentColumn GetColumn(this CassandraSuperColumnFamily family, CassandraType key, CassandraType superColumnName, CassandraType columnName)
		{
			var op = new GetColumn(key, superColumnName, columnName);
			return family.ExecuteOperation(op);
		}

		#endregion

		#region GetSuperColumn

		public static FluentSuperColumn GetSuperColumn(this CassandraSuperColumnFamily family, CassandraType key, FluentColumnParent parent)
		{
			var superColumnName = parent.SuperColumn.ColumnName;
			return GetSuperColumn(family, key, superColumnName);
		}

		public static FluentSuperColumn GetSuperColumn(this CassandraSuperColumnFamily family, CassandraType key, CassandraType superColumnName)
		{
			var op = new GetSuperColumn(key, superColumnName);
			return family.ExecuteOperation(op);
		}

		#endregion

		#region RemoveColumn

		public static void RemoveColumn(this CassandraSuperColumnFamily family, CassandraType key, FluentColumnPath path)
		{
			var columnName = path.Column.ColumnName;
			var superColumnName = path.SuperColumn.ColumnName;
			RemoveColumn(family, key, superColumnName, columnName);
		}

		public static void RemoveColumn(this CassandraSuperColumnFamily family, CassandraType key, CassandraType superColumnName, CassandraType columnName)
		{
			var op = new RemoveColumn(key, superColumnName, columnName);
			family.ExecuteOperation(op);
		}

		#endregion

		#region RemoveSuperColumn

		public static void RemoveSuperColumn(this CassandraSuperColumnFamily family, CassandraType key, FluentColumnParent parent)
		{
			var superColumnName = parent.SuperColumn.ColumnName;
			RemoveSuperColumn(family, key, superColumnName);
		}

		public static void RemoveSuperColumn(this CassandraSuperColumnFamily family, CassandraType key, CassandraType superColumnName)
		{
			var op = new RemoveSuperColumn(key, superColumnName);
			family.ExecuteOperation(op);
		}

		#endregion

		#region RemoveColumn

		public static void RemoveKey(this CassandraSuperColumnFamily family, CassandraType key)
		{
			var op = new RemoveKey(key);
			family.ExecuteOperation(op);
		}

		#endregion

		#region GetSingleSuperColumn

		public static FluentSuperColumn GetSingleSuperColumn(this CassandraSuperColumnFamily family, CassandraType key, CassandraType superColumnName, IEnumerable<CassandraType> columnNames)
		{
			var op = new GetSuperColumnSlice(key, superColumnName, new ColumnSlicePredicate(columnNames));
			return family.ExecuteOperation(op);
		}

		public static FluentSuperColumn GetSingleSuperColumn(this CassandraSuperColumnFamily family, CassandraType key, CassandraType superColumnName, CassandraType columnStart, CassandraType columnEnd, bool reversed = false, int count = 100)
		{
			var op = new GetSuperColumnSlice(key, superColumnName, new RangeSlicePredicate(columnStart, columnEnd, reversed, count));
			return family.ExecuteOperation(op);
		}

		#endregion

		#region GetSingle

		public static FluentSuperColumnFamily GetSingle(this CassandraSuperColumnFamily family, CassandraType key, IEnumerable<CassandraType> columnNames)
		{
			var op = new GetSuperColumnFamilySlice(key, new ColumnSlicePredicate(columnNames));
			return family.ExecuteOperation(op);
		}

		public static FluentSuperColumnFamily GetSingle(this CassandraSuperColumnFamily family, CassandraType key, CassandraType columnStart, CassandraType columnEnd, bool reversed = false, int count = 100)
		{
			var op = new GetSuperColumnFamilySlice(key, new RangeSlicePredicate(columnStart, columnEnd, reversed, count));
			return family.ExecuteOperation(op);
		}

		#endregion

		#region GetSuperColumns

		// queryable

		public static ICassandraQueryable<FluentSuperColumn, CassandraType> GetSuperColumns(this CassandraSuperColumnFamily family, BytesType[] keys, CassandraType superColumnName)
		{
			var setup = new CassandraQuerySetup<FluentSuperColumn, CassandraType> {
				Keys = keys,
				SuperColumnName = superColumnName,
				CreateQueryOperation = (x, slice) => new MultiGetSuperColumnSlice(x.Keys, x.SuperColumnName, slice)
			};
			return ((ICassandraQueryProvider)family).CreateQuery(setup, null);
		}

		public static ICassandraQueryable<FluentSuperColumn, CassandraType> GetSuperColumns(this CassandraSuperColumnFamily family, BytesType startKey, BytesType endKey, string startToken, string endToken, int keyCount, CassandraType superColumnName)
		{
			var setup = new CassandraQuerySetup<FluentSuperColumn, CassandraType> {
				KeyRange = new CassandraKeyRange(startKey, endKey, startToken, endToken, keyCount),
				SuperColumnName = superColumnName,
				CreateQueryOperation = (x, slice) => new GetSuperColumnRangeSlices(x.KeyRange, x.SuperColumnName, slice)
			};
			return ((ICassandraQueryProvider)family).CreateQuery(setup, null);
		}

		public static ICassandraQueryable<FluentSuperColumn, CassandraType> GetSuperColumns(this CassandraSuperColumnFamily family, BytesType startKey, int keyCount, Expression<Func<FluentRecord<FluentColumn>, bool>> expression)
		{
			var setup = new CassandraQuerySetup<FluentSuperColumn, CassandraType> {
				IndexClause = new CassandraIndexClause(startKey, keyCount, expression),
				CreateQueryOperation = (s, slice) => new GetSuperColumnIndexedSlices(s.IndexClause, s.SuperColumnName, slice)
			};
			return ((ICassandraQueryProvider)family).CreateQuery(setup, null);
		}

		// multi_get_slice

		public static IEnumerable<FluentSuperColumn> GetSuperColumns(this CassandraSuperColumnFamily family, IEnumerable<BytesType> keys, CassandraType superColumnName, IEnumerable<CassandraType> columnNames)
		{
			var op = new MultiGetSuperColumnSlice(keys, superColumnName, new ColumnSlicePredicate(columnNames));
			return family.ExecuteOperation(op);
		}

		public static IEnumerable<FluentSuperColumn> GetSuperColumns(this CassandraSuperColumnFamily family, IEnumerable<BytesType> keys, CassandraType superColumnName, CassandraType columnStart, CassandraType columnEnd, bool columnsReversed = false, int columnCount = 100)
		{
			var op = new MultiGetSuperColumnSlice(keys, superColumnName, new RangeSlicePredicate(columnStart, columnEnd, columnsReversed, columnCount));
			return family.ExecuteOperation(op);
		}

		// get_range_slice

		public static IEnumerable<FluentSuperColumn> GetSuperColumns(this CassandraSuperColumnFamily family, BytesType startKey, BytesType endKey, string startToken, string endToken, int keyCount, CassandraType superColumnName, IEnumerable<CassandraType> columnNames)
		{
			var op = new GetSuperColumnRangeSlices(new CassandraKeyRange(startKey, endKey, startToken, endToken, keyCount), superColumnName, new ColumnSlicePredicate(columnNames));
			return family.ExecuteOperation(op);
		}

		public static IEnumerable<FluentSuperColumn> GetSuperColumns(this CassandraSuperColumnFamily family, BytesType startKey, BytesType endKey, string startToken, string endToken, int keyCount, CassandraType superColumnName, CassandraType columnStart, CassandraType columnEnd, bool columnsReversed = false, int columnCount = 100)
		{
			var op = new GetSuperColumnRangeSlices(new CassandraKeyRange(startKey, endKey, startToken, endToken, keyCount), superColumnName, new RangeSlicePredicate(columnStart, columnEnd, columnsReversed, columnCount));
			return family.ExecuteOperation(op);
		}

		// get_indexed_slices

		public static IEnumerable<FluentSuperColumn> GetSuperColumns(this CassandraSuperColumnFamily family, BytesType startKey, int keyCount, Expression<Func<FluentRecord<FluentColumn>, bool>> expression, CassandraType superColumnName, IEnumerable<CassandraType> columnNames)
		{
			var op = new GetSuperColumnIndexedSlices(new CassandraIndexClause(startKey, keyCount, expression), superColumnName, new ColumnSlicePredicate(columnNames));
			return family.ExecuteOperation(op);
		}

		public static IEnumerable<FluentSuperColumn> GetSuperColumns(this CassandraSuperColumnFamily family, BytesType startKey, int keyCount, Expression<Func<FluentRecord<FluentColumn>, bool>> expression, CassandraType superColumnName, CassandraType columnStart, CassandraType columnEnd, bool columnsReversed = false, int columnCount = 100)
		{
			var op = new GetSuperColumnIndexedSlices(new CassandraIndexClause(startKey, keyCount, expression), superColumnName, new RangeSlicePredicate(columnStart, columnEnd, columnsReversed, columnCount));
			return family.ExecuteOperation(op);
		}

		#endregion

		#region Get

		// queryable

		public static ICassandraQueryable<FluentSuperColumnFamily, CassandraType> Get(this CassandraSuperColumnFamily family, params BytesType[] keys)
		{
			var setup = new CassandraQuerySetup<FluentSuperColumnFamily, CassandraType> {
				Keys = keys,
				CreateQueryOperation = (x, slice) => new MultiGetSuperColumnFamilySlice(x.Keys, slice)
			};
			return ((ICassandraQueryProvider)family).CreateQuery(setup, null);
		}

		public static ICassandraQueryable<FluentSuperColumnFamily, CassandraType> Get(this CassandraSuperColumnFamily family, BytesType startKey, BytesType endKey, string startToken, string endToken, int keyCount)
		{
			var setup = new CassandraQuerySetup<FluentSuperColumnFamily, CassandraType> {
				KeyRange = new CassandraKeyRange(startKey, endKey, startToken, endToken, keyCount),
				CreateQueryOperation = (x, slice) => new GetSuperColumnFamilyRangeSlices(x.KeyRange, slice)
			};
			return ((ICassandraQueryProvider)family).CreateQuery(setup, null);
		}

		// multi_get_slice

		public static IEnumerable<FluentSuperColumnFamily> Get(this CassandraSuperColumnFamily family, IEnumerable<BytesType> keys, IEnumerable<CassandraType> columnNames)
		{
			var op = new MultiGetSuperColumnFamilySlice(keys, new ColumnSlicePredicate(columnNames));
			return family.ExecuteOperation(op);
		}

		public static IEnumerable<FluentSuperColumnFamily> Get(this CassandraSuperColumnFamily family, IEnumerable<BytesType> keys, CassandraType columnStart, CassandraType columnEnd, bool columnsReversed = false, int columnCount = 100)
		{
			var op = new MultiGetSuperColumnFamilySlice(keys, new RangeSlicePredicate(columnStart, columnEnd, columnsReversed, columnCount));
			return family.ExecuteOperation(op);
		}

		// get_range_slice

		public static IEnumerable<FluentSuperColumnFamily> Get(this CassandraSuperColumnFamily family, BytesType startKey, BytesType endKey, string startToken, string endToken, int keyCount, IEnumerable<CassandraType> columnNames)
		{
			var op = new GetSuperColumnFamilyRangeSlices(new CassandraKeyRange(startKey, endKey, startToken, endToken, keyCount), new ColumnSlicePredicate(columnNames));
			return family.ExecuteOperation(op);
		}

		public static IEnumerable<FluentSuperColumnFamily> Get(this CassandraSuperColumnFamily family, BytesType startKey, BytesType endKey, string startToken, string endToken, int keyCount, CassandraType columnStart, CassandraType columnEnd, bool columnsReversed = false, int columnCount = 100)
		{
			var op = new GetSuperColumnFamilyRangeSlices(new CassandraKeyRange(startKey, endKey, startToken, endToken, keyCount), new RangeSlicePredicate(columnStart, columnEnd, columnsReversed, columnCount));
			return family.ExecuteOperation(op);
		}

		#endregion
	}
}
