using System;
using System.Collections.Generic;
using FluentCassandra.Operations;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public static class CassandraColumnFamilyOperations
	{
		#region ColumnCount

		public static int ColumnCount(this CassandraColumnFamily family, CassandraObject key, IEnumerable<CassandraObject> columnNames)
		{
			var op = new ColumnCount(key, new CassandraColumnSlicePredicate(columnNames));
			return family.ExecuteOperation(op);
		}

		public static int ColumnCount(this CassandraColumnFamily family, CassandraObject key, CassandraObject columnStart, CassandraObject columnEnd, bool reversed = false, int count = 100)
		{
			var op = new ColumnCount(key, new CassandraRangeSlicePredicate(columnStart, columnEnd, reversed, count));
			return family.ExecuteOperation(op);
		}

		#endregion

		#region InsertColumn

		public static void InsertColumn(this CassandraColumnFamily family, CassandraObject key, FluentColumn column)
		{
			InsertColumn(family, key, column.GetPath());
		}

		public static void InsertColumn(this CassandraColumnFamily family, CassandraObject key, FluentColumnPath path)
		{
			var columnName = path.Column.ColumnName;
			var columnValue = path.Column.ColumnValue;
			var timestamp = path.Column.ColumnTimestamp;
			var timeToLive = path.Column.ColumnSecondsUntilDeleted;

			var op = new InsertColumn(key, columnName, columnValue, timestamp, timeToLive);
			family.ExecuteOperation(op);
		}

		public static void InsertColumn(this CassandraColumnFamily family, CassandraObject key, CassandraObject columnName, BytesType columnValue)
		{
			InsertColumn(family, key, columnName, columnValue, DateTimeOffset.UtcNow, null);
		}

		public static void InsertColumn(this CassandraColumnFamily family, CassandraObject key, CassandraObject columnName, BytesType columnValue, DateTimeOffset timestamp, int? timeToLive)
		{
			var op = new InsertColumn(key, columnName, columnValue, timestamp, timeToLive);
			family.ExecuteOperation(op);
		}

		#endregion

		#region AddColumn

		public static void InsertCounterColumn(this CassandraColumnFamily family, CassandraObject key, CassandraObject columnName, long columnValue)
		{
			var op = new AddColumn(key, columnName, columnValue);
			family.ExecuteOperation(op);
		}

		#endregion

		#region GetColumn

		public static FluentColumn GetColumn(this CassandraColumnFamily family, CassandraObject key, FluentColumnPath path)
		{
			var columnName = path.Column.ColumnName;
			return GetColumn(family, key, columnName);
		}

		public static FluentColumn GetColumn(this CassandraColumnFamily family, CassandraObject key, CassandraObject columnName)
		{
			var op = new GetColumn(key, columnName);
			return family.ExecuteOperation(op);
		}

		#endregion

		#region RemoveColumn

		public static void RemoveColumn(this CassandraColumnFamily family, CassandraObject key, FluentColumnPath path)
		{
			var columnName = path.Column.ColumnName;
			RemoveColumn(family, key, columnName);
		}

		public static void RemoveColumn(this CassandraColumnFamily family, CassandraObject key, CassandraObject columnName)
		{
			var op = new Remove(key, columnName);
			family.ExecuteOperation(op);
		}

		#endregion

		#region GetSingle

		public static FluentColumnFamily GetSingle(this CassandraColumnFamily family, CassandraObject key, IEnumerable<CassandraObject> columnNames)
		{
			var op = new GetColumnFamilySlice(key, new CassandraColumnSlicePredicate(columnNames));
			return family.ExecuteOperation(op);
		}

		public static FluentColumnFamily GetSingle(this CassandraColumnFamily family, CassandraObject key, CassandraObject columnStart, CassandraObject columnEnd, bool columnsReversed = false, int columnCount = 100)
		{
			var op = new GetColumnFamilySlice(key, new CassandraRangeSlicePredicate(columnStart, columnEnd, columnsReversed, columnCount));
			return family.ExecuteOperation(op);
		}

		#endregion

		#region Get

		// queryable

		public static CassandraSlicePredicateQuery<FluentColumnFamily> Get(this CassandraColumnFamily family)
		{
			return family.CreateCassandraSlicePredicateQuery<FluentColumnFamily>(null);
		}

		public static CassandraSlicePredicateQuery<FluentColumnFamily> Get(this CassandraColumnFamily family, params CassandraObject[] keys)
		{
			return Get(family).FetchKeys(keys);
		}

		#endregion
	}
}