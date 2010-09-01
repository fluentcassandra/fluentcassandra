using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FluentCassandra.Types;
using FluentCassandra.Operations;

namespace FluentCassandra
{
	public static class CassandraSuperColumnFamilyOperations
	{
		#region ColumnCount

		public static int ColumnCount<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, BytesType key, IEnumerable<CompareWith> columnNames)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var op = new ColumnCount(key, new ColumnSlicePredicate(columnNames));
			return family.ExecuteOperation(op);
		}

		public static int ColumnCount<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, BytesType key, CompareWith columnStart, CompareWith columnEnd, bool reversed = false, int count = 100)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var op = new ColumnCount(key, new RangeSlicePredicate(columnStart, columnEnd, reversed, count));
			return family.ExecuteOperation(op);
		}

		public static int ColumnCount<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, BytesType key, CompareWith superColumnName, IEnumerable<CompareWith> columnNames)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var op = new ColumnCount(key, superColumnName, new ColumnSlicePredicate(columnNames));
			return family.ExecuteOperation(op);
		}

		public static int ColumnCount<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, BytesType key, CompareWith superColumnName, CompareWith columnStart, CompareWith columnEnd, bool reversed = false, int count = 100)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var op = new ColumnCount(key, superColumnName, new RangeSlicePredicate(columnStart, columnEnd, reversed, count));
			return family.ExecuteOperation(op);
		}

		#endregion

		#region InsertColumn

		public static void InsertColumn<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, BytesType key, IFluentColumn<CompareSubcolumnWith> column)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			InsertColumn<CompareWith, CompareSubcolumnWith>(family, key, column.GetPath());
		}

		public static void InsertColumn<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, BytesType key, FluentColumnPath path)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var superColumnName = path.SuperColumn.ColumnName;
			var name = path.Column.ColumnName;
			var value = path.Column.ColumnValue;
			var timestamp = path.Column.ColumnTimestamp;

			var op = new InsertColumn(key, superColumnName, name, value, timestamp);
			family.ExecuteOperation(op);
		}

		public static void InsertColumn<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, BytesType key, CompareWith superColumnName, CompareSubcolumnWith name, BytesType value)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			InsertColumn<CompareWith, CompareSubcolumnWith>(family, key, superColumnName, name, value, DateTimeOffset.UtcNow);
		}

		public static void InsertColumn<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, BytesType key, CompareWith superColumnName, CompareSubcolumnWith name, BytesType value, DateTimeOffset timestamp)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var op = new InsertColumn(key, superColumnName, name, value, timestamp);
			family.ExecuteOperation(op);
		}

		#endregion

		#region GetColumn

		public static IFluentColumn<CompareSubcolumnWith> GetColumn<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, BytesType key, FluentColumnPath path)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var columnName = (CompareSubcolumnWith)path.Column.ColumnName;
			var superColumnName = (CompareWith)path.SuperColumn.ColumnName;
			return GetColumn<CompareWith, CompareSubcolumnWith>(family, key, superColumnName, columnName);
		}

		public static IFluentColumn<CompareSubcolumnWith> GetColumn<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, BytesType key, CompareWith superColumnName, CompareSubcolumnWith columnName)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var op = new GetColumn<CompareSubcolumnWith>(key, superColumnName, columnName);
			return family.ExecuteOperation(op);
		}

		#endregion

		#region GetSuperColumn

		public static IFluentSuperColumn<CompareWith, CompareSubcolumnWith> GetSuperColumn<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, BytesType key, FluentColumnParent parent)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var superColumnName = (CompareWith)parent.SuperColumn.ColumnName;
			return GetSuperColumn<CompareWith, CompareSubcolumnWith>(family, key, superColumnName);
		}

		public static IFluentSuperColumn<CompareWith, CompareSubcolumnWith> GetSuperColumn<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, BytesType key, CompareWith superColumnName)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var op = new GetSuperColumn<CompareWith, CompareSubcolumnWith>(key, superColumnName);
			return family.ExecuteOperation(op);
		}

		#endregion

		#region RemoveColumn

		public static void RemoveColumn<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, BytesType key, FluentColumnPath path)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var columnName = (CompareSubcolumnWith)path.Column.ColumnName;
			var superColumnName = (CompareWith)path.SuperColumn.ColumnName;
			RemoveColumn<CompareWith, CompareSubcolumnWith>(family, key, superColumnName, columnName);
		}

		public static void RemoveColumn<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, BytesType key, CompareWith superColumnName, CompareSubcolumnWith columnName)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var op = new RemoveColumn(key, superColumnName, columnName);
			family.ExecuteOperation(op);
		}

		#endregion

		#region RemoveSuperColumn

		public static void RemoveSuperColumn<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, BytesType key, FluentColumnParent parent)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var superColumnName = (CompareWith)parent.SuperColumn.ColumnName;
			RemoveSuperColumn<CompareWith, CompareSubcolumnWith>(family, key, superColumnName);
		}

		public static void RemoveSuperColumn<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, BytesType key, CompareWith superColumnName)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var op = new RemoveSuperColumn(key, superColumnName);
			family.ExecuteOperation(op);
		}

		#endregion

		#region RemoveColumn

		public static void RemoveKey<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, BytesType key)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var op = new RemoveKey(key);
			family.ExecuteOperation(op);
		}

		#endregion

		#region GetSingleSuperColumn

		public static IFluentSuperColumn<CompareWith, CompareSubcolumnWith> GetSingleSuperColumn<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, BytesType key, CompareWith superColumnName, IEnumerable<CompareSubcolumnWith> columnNames)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var op = new GetSuperColumnSlice<CompareWith, CompareSubcolumnWith>(key, superColumnName, new ColumnSlicePredicate(columnNames));
			return family.ExecuteOperation(op);
		}

		public static IFluentSuperColumn<CompareWith, CompareSubcolumnWith> GetSingleSuperColumn<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, BytesType key, CompareWith superColumnName, CompareSubcolumnWith columnStart, CompareSubcolumnWith columnEnd, bool reversed = false, int count = 100)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var op = new GetSuperColumnSlice<CompareWith, CompareSubcolumnWith>(key, superColumnName, new RangeSlicePredicate(columnStart, columnEnd, reversed, count));
			return family.ExecuteOperation(op);
		}

		#endregion

		#region GetSingle

		public static IFluentSuperColumnFamily<CompareWith, CompareSubcolumnWith> GetSingle<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, BytesType key, IEnumerable<CompareWith> columnNames)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var op = new GetSuperColumnFamilySlice<CompareWith, CompareSubcolumnWith>(key, new ColumnSlicePredicate(columnNames));
			return family.ExecuteOperation(op);
		}

		public static IFluentSuperColumnFamily<CompareWith, CompareSubcolumnWith> GetSingle<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, BytesType key, CompareWith columnStart, CompareWith columnEnd, bool reversed = false, int count = 100)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var op = new GetSuperColumnFamilySlice<CompareWith, CompareSubcolumnWith>(key, new RangeSlicePredicate(columnStart, columnEnd, reversed, count));
			return family.ExecuteOperation(op);
		}

		#endregion

		#region GetSuperColumns

		// queryable

		public static ICassandraQueryable<IFluentSuperColumn<CompareWith, CompareSubcolumnWith>, CompareSubcolumnWith> GetSuperColumns<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, BytesType[] keys, CompareWith superColumnName)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var op = new MultiGetSuperColumnSlice<CompareWith, CompareSubcolumnWith>(keys, superColumnName, null);
			return ((ICassandraQueryProvider)family).CreateQuery(op, null);
		}

		public static ICassandraQueryable<IFluentSuperColumn<CompareWith, CompareSubcolumnWith>, CompareSubcolumnWith> GetSuperColumns<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, BytesType startKey, BytesType endKey, string startToken, string endToken, int keyCount, CompareWith superColumnName)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var op = new GetSuperColumnRangeSlice<CompareWith, CompareSubcolumnWith>(new CassandraKeyRange(startKey, endKey, startToken, endToken, keyCount), superColumnName, null);
			return ((ICassandraQueryProvider)family).CreateQuery(op, null);
		}

		// multi_get_slice

		public static IEnumerable<IFluentSuperColumn<CompareWith, CompareSubcolumnWith>> GetSuperColumns<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, IEnumerable<BytesType> keys, CompareWith superColumnName, IEnumerable<CompareSubcolumnWith> columnNames)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var op = new MultiGetSuperColumnSlice<CompareWith, CompareSubcolumnWith>(keys, superColumnName, new ColumnSlicePredicate(columnNames));
			return family.ExecuteOperation(op);
		}

		public static IEnumerable<IFluentSuperColumn<CompareWith, CompareSubcolumnWith>> GetSuperColumns<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, IEnumerable<BytesType> keys, CompareWith superColumnName, CompareSubcolumnWith columnStart, CompareSubcolumnWith columnEnd, bool columnsReversed = false, int columnCount = 100)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var op = new MultiGetSuperColumnSlice<CompareWith, CompareSubcolumnWith>(keys, superColumnName, new RangeSlicePredicate(columnStart, columnEnd, columnsReversed, columnCount));
			return family.ExecuteOperation(op);
		}

		// get_range_slice

		public static IEnumerable<IFluentSuperColumn<CompareWith, CompareSubcolumnWith>> GetSuperColumns<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, BytesType startKey, BytesType endKey, string startToken, string endToken, int keyCount, CompareWith superColumnName, IEnumerable<CompareSubcolumnWith> columnNames)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var op = new GetSuperColumnRangeSlice<CompareWith, CompareSubcolumnWith>(new CassandraKeyRange(startKey, endKey, startToken, endToken, keyCount), superColumnName, new ColumnSlicePredicate(columnNames));
			return family.ExecuteOperation(op);
		}

		public static IEnumerable<IFluentSuperColumn<CompareWith, CompareSubcolumnWith>> GetSuperColumns<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, BytesType startKey, BytesType endKey, string startToken, string endToken, int keyCount, CompareWith superColumnName, CompareSubcolumnWith columnStart, CompareSubcolumnWith columnEnd, bool columnsReversed = false, int columnCount = 100)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var op = new GetSuperColumnRangeSlice<CompareWith, CompareSubcolumnWith>(new CassandraKeyRange(startKey, endKey, startToken, endToken, keyCount), superColumnName, new RangeSlicePredicate(columnStart, columnEnd, columnsReversed, columnCount));
			return family.ExecuteOperation(op);
		}

		#endregion

		#region Get

		// queryable

		public static ICassandraQueryable<IFluentSuperColumnFamily<CompareWith, CompareSubcolumnWith>, CompareWith> Get<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, params BytesType[] keys)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var op = new MultiGetSuperColumnFamilySlice<CompareWith, CompareSubcolumnWith>(keys, null);
			return ((ICassandraQueryProvider)family).CreateQuery(op, null);
		}

		public static ICassandraQueryable<IFluentSuperColumnFamily<CompareWith, CompareSubcolumnWith>, CompareWith> Get<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, BytesType startKey, BytesType endKey, string startToken, string endToken, int keyCount)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var op = new GetSuperColumnFamilyRangeSlice<CompareWith, CompareSubcolumnWith>(new CassandraKeyRange(startKey, endKey, startToken, endToken, keyCount), null);
			return ((ICassandraQueryProvider)family).CreateQuery(op, null);
		}

		// multi_get_slice

		public static IEnumerable<IFluentSuperColumnFamily<CompareWith, CompareSubcolumnWith>> Get<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, IEnumerable<BytesType> keys, IEnumerable<CompareWith> columnNames)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var op = new MultiGetSuperColumnFamilySlice<CompareWith, CompareSubcolumnWith>(keys, new ColumnSlicePredicate(columnNames));
			return family.ExecuteOperation(op);
		}

		public static IEnumerable<IFluentSuperColumnFamily<CompareWith, CompareSubcolumnWith>> Get<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, IEnumerable<BytesType> keys, CompareWith columnStart, CompareWith columnEnd, bool columnsReversed = false, int columnCount = 100)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var op = new MultiGetSuperColumnFamilySlice<CompareWith, CompareSubcolumnWith>(keys, new RangeSlicePredicate(columnStart, columnEnd, columnsReversed, columnCount));
			return family.ExecuteOperation(op);
		}

		// get_range_slice

		public static IEnumerable<IFluentSuperColumnFamily<CompareWith, CompareSubcolumnWith>> Get<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, BytesType startKey, BytesType endKey, string startToken, string endToken, int keyCount, IEnumerable<CompareWith> columnNames)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var op = new GetSuperColumnFamilyRangeSlice<CompareWith, CompareSubcolumnWith>(new CassandraKeyRange(startKey, endKey, startToken, endToken, keyCount), new ColumnSlicePredicate(columnNames));
			return family.ExecuteOperation(op);
		}

		public static IEnumerable<IFluentSuperColumnFamily<CompareWith, CompareSubcolumnWith>> Get<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, BytesType startKey, BytesType endKey, string startToken, string endToken, int keyCount, CompareWith columnStart, CompareWith columnEnd, bool columnsReversed = false, int columnCount = 100)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var op = new GetSuperColumnFamilyRangeSlice<CompareWith, CompareSubcolumnWith>(new CassandraKeyRange(startKey, endKey, startToken, endToken, keyCount), new RangeSlicePredicate(columnStart, columnEnd, columnsReversed, columnCount));
			return family.ExecuteOperation(op);
		}

		#endregion
	}
}
