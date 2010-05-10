using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FluentCassandra.Types;
using FluentCassandra.Operations;

namespace FluentCassandra
{
	public static class CassandraColumnFamilyOperations
	{
		#region ColumnCount

		public static int ColumnCount<CompareWith>(this CassandraColumnFamily<CompareWith> family, string key)
			where CompareWith : CassandraType
		{
			var op = new ColumnCount(key);
			return family.ExecuteOperation(op);
		}

		#endregion

		#region InsertColumn

		public static void InsertColumn<CompareWith>(this CassandraColumnFamily<CompareWith> family, string key, IFluentColumn<CompareWith> column)
			where CompareWith : CassandraType
		{
			InsertColumn<CompareWith>(family, key, column.GetPath());
		}

		public static void InsertColumn<CompareWith>(this CassandraColumnFamily<CompareWith> family, string key, FluentColumnPath path)
			where CompareWith : CassandraType
		{
			var columnName = path.Column.Name;
			var columnValue = path.Column.Value;
			var timestamp = path.Column.Timestamp;

			var op = new InsertColumn(key, columnName, columnValue, timestamp);
			family.ExecuteOperation(op);
		}

		public static void InsertColumn<CompareWith>(this CassandraColumnFamily<CompareWith> family, string key, CompareWith columnName, BytesType columnValue)
			where CompareWith : CassandraType
		{
			InsertColumn<CompareWith>(family, key, columnName, columnValue, DateTimeOffset.UtcNow);
		}

		public static void InsertColumn<CompareWith>(this CassandraColumnFamily<CompareWith> family, string key, CompareWith columnName, BytesType columnValue, DateTimeOffset timestamp)
			where CompareWith : CassandraType
		{
			var op = new InsertColumn(key, columnName, columnValue, timestamp);
			family.ExecuteOperation(op);
		}

		#endregion

		#region GetColumn

		public static IFluentColumn<CompareWith> GetColumn<CompareWith>(this CassandraColumnFamily<CompareWith> family, string key, FluentColumnPath path)
			where CompareWith : CassandraType
		{
			var columnName = (CompareWith)path.Column.Name;
			return GetColumn<CompareWith>(family, key, columnName);
		}

		public static IFluentColumn<CompareWith> GetColumn<CompareWith>(this CassandraColumnFamily<CompareWith> family, string key, CompareWith columnName)
			where CompareWith : CassandraType
		{
			var op = new GetColumn<CompareWith>(key, columnName);
			return family.ExecuteOperation(op);
		}

		#endregion

		#region RemoveColumn

		public static void RemoveColumn<CompareWith>(this CassandraColumnFamily<CompareWith> family, string key, FluentColumnPath path)
			where CompareWith : CassandraType
		{
			var columnName = (CompareWith)path.Column.Name;
			RemoveColumn<CompareWith>(family, key, columnName);
		}

		public static void RemoveColumn<CompareWith>(this CassandraColumnFamily<CompareWith> family, string key, CompareWith columnName)
			where CompareWith : CassandraType
		{
			var op = new RemoveColumn(key, columnName);
			family.ExecuteOperation(op);
		}

		#endregion

		#region RemoveKey

		public static void RemoveKey<CompareWith>(this CassandraColumnFamily<CompareWith> family, string key)
			where CompareWith : CassandraType
		{
			var op = new RemoveKey(key);
			family.ExecuteOperation(op);
		}

		#endregion

		#region GetSingle

		public static IFluentColumnFamily<CompareWith> GetSingle<CompareWith>(this CassandraColumnFamily<CompareWith> family, string key, IEnumerable<CompareWith> columnNames)
			where CompareWith : CassandraType
		{
			var op = new GetColumnFamilySlice<CompareWith>(key, new ColumnSlicePredicate(columnNames));
			return family.ExecuteOperation(op);
		}

		public static IFluentColumnFamily<CompareWith> GetSingle<CompareWith>(this CassandraColumnFamily<CompareWith> family, string key, CompareWith columnStart, CompareWith columnEnd, bool columnsReversed = false, int columnCount = 100)
			where CompareWith : CassandraType
		{
			var op = new GetColumnFamilySlice<CompareWith>(key, new RangeSlicePredicate(columnStart, columnEnd, columnsReversed, columnCount));
			return family.ExecuteOperation(op);
		}

		#endregion

		#region Get

		// multi_get_slice

		public static IEnumerable<IFluentColumnFamily<CompareWith>> Get<CompareWith>(this CassandraColumnFamily<CompareWith> family, IEnumerable<string> keys, IEnumerable<CompareWith> columnNames)
			where CompareWith : CassandraType
		{
			var op = new MultiGetColumnFamilySlice<CompareWith>(keys, new ColumnSlicePredicate(columnNames));
			return family.ExecuteOperation(op);
		}

		public static IEnumerable<IFluentColumnFamily<CompareWith>> Get<CompareWith>(this CassandraColumnFamily<CompareWith> family, IEnumerable<string> keys, CompareWith columnStart, CompareWith columnEnd, bool columnsReversed = false, int columnCount = 100)
			where CompareWith : CassandraType
		{
			var op = new MultiGetColumnFamilySlice<CompareWith>(keys, new RangeSlicePredicate(columnStart, columnEnd, columnsReversed, columnCount));
			return family.ExecuteOperation(op);
		}

		// get_range_slice

		public static IEnumerable<IFluentColumnFamily<CompareWith>> Get<CompareWith>(this CassandraColumnFamily<CompareWith> family, string startKey, string endKey, string startToken, string endToken, int keyCount, IEnumerable<CompareWith> columnNames)
			where CompareWith : CassandraType
		{
			var op = new GetColumnFamilyRangeSlice<CompareWith>(new CassandraKeyRange(startKey, endKey, startToken, endToken, keyCount), new ColumnSlicePredicate(columnNames));
			return family.ExecuteOperation(op);
		}

		public static IEnumerable<IFluentColumnFamily<CompareWith>> Get<CompareWith>(this CassandraColumnFamily<CompareWith> family, string startKey, string endKey, string startToken, string endToken, int keyCount, CompareWith columnStart, CompareWith columnEnd, bool columnsReversed = false, int columnCount = 100)
			where CompareWith : CassandraType
		{
			var op = new GetColumnFamilyRangeSlice<CompareWith>(new CassandraKeyRange(startKey, endKey, startToken, endToken, keyCount), new RangeSlicePredicate(columnStart, columnEnd, columnsReversed, columnCount));
			return family.ExecuteOperation(op);
		}

		#endregion
	}
}