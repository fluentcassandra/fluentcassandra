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

		public static int ColumnCount<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, string key)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var op = new ColumnCount(key);
			return family.ExecuteOperation(op);
		}

		public static int ColumnCount<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, string key, CompareWith superColumnName)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var op = new ColumnCount(key, superColumnName);
			return family.ExecuteOperation(op);
		}

		#endregion

		#region InsertColumn

		public static void InsertColumn<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, string key, IFluentColumn<CompareSubcolumnWith> column)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			InsertColumn<CompareWith, CompareSubcolumnWith>(family, key, column.GetPath());
		}

		public static void InsertColumn<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, string key, FluentColumnPath path)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var superColumnName = path.SuperColumn.Name;
			var name = path.Column.Name;
			var value = path.Column.Value;
			var timestamp = path.Column.Timestamp;

			var op = new InsertColumn(key, superColumnName, name, value, timestamp);
			family.ExecuteOperation(op);
		}

		public static void InsertColumn<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, string key, CompareWith superColumnName, CompareSubcolumnWith name, BytesType value)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			InsertColumn<CompareWith, CompareSubcolumnWith>(family, key, superColumnName, name, value, DateTimeOffset.UtcNow);
		}

		public static void InsertColumn<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, string key, CompareWith superColumnName, CompareSubcolumnWith name, BytesType value, DateTimeOffset timestamp)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var op = new InsertColumn(key, superColumnName, name, value, timestamp);
			family.ExecuteOperation(op);
		}

		#endregion

		#region GetColumn

		public static IFluentColumn<CompareSubcolumnWith> GetColumn<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, string key, FluentColumnPath path)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var columnName = (CompareSubcolumnWith)path.Column.Name;
			var superColumnName = (CompareWith)path.SuperColumn.Name;
			return GetColumn<CompareWith, CompareSubcolumnWith>(family, key, superColumnName, columnName);
		}

		public static IFluentColumn<CompareSubcolumnWith> GetColumn<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, string key, CompareWith superColumnName, CompareSubcolumnWith columnName)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var op = new GetColumn<CompareSubcolumnWith>(key, superColumnName, columnName);
			return family.ExecuteOperation(op);
		}

		#endregion

		#region GetSuperColumn

		public static IFluentSuperColumn<CompareWith, CompareSubcolumnWith> GetSuperColumn<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, string key, FluentColumnParent parent)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var superColumnName = (CompareWith)parent.SuperColumn.Name;
			return GetSuperColumn<CompareWith, CompareSubcolumnWith>(family, key, superColumnName);
		}

		public static IFluentSuperColumn<CompareWith, CompareSubcolumnWith> GetSuperColumn<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, string key, CompareWith superColumnName)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var op = new GetSuperColumn<CompareWith, CompareSubcolumnWith>(key, superColumnName);
			return family.ExecuteOperation(op);
		}

		#endregion

		#region RemoveColumn

		public static void RemoveColumn<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, string key, FluentColumnPath path)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var columnName = (CompareSubcolumnWith)path.Column.Name;
			var superColumnName = (CompareWith)path.SuperColumn.Name;
			RemoveColumn<CompareWith, CompareSubcolumnWith>(family, key, superColumnName, columnName);
		}

		public static void RemoveColumn<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, string key, CompareWith superColumnName, CompareSubcolumnWith columnName)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var op = new RemoveColumn(key, superColumnName, columnName);
			family.ExecuteOperation(op);
		}

		#endregion

		#region RemoveSuperColumn

		public static void RemoveSuperColumn<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, string key, FluentColumnParent parent)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var superColumnName = (CompareWith)parent.SuperColumn.Name;
			RemoveSuperColumn<CompareWith, CompareSubcolumnWith>(family, key, superColumnName);
		}

		public static void RemoveSuperColumn<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, string key, CompareWith superColumnName)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var op = new RemoveSuperColumn(key, superColumnName);
			family.ExecuteOperation(op);
		}

		#endregion

		#region RemoveColumn

		public static void RemoveKey<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, string key)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var op = new RemoveKey(key);
			family.ExecuteOperation(op);
		}

		#endregion

		#region GetSingleSuperColumn

		public static IFluentSuperColumn<CompareWith, CompareSubcolumnWith> GetSingleSuperColumn<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, string key, CompareWith superColumnName, IEnumerable<CompareSubcolumnWith> columnNames)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var op = new GetSuperColumnSlice<CompareWith, CompareSubcolumnWith>(key, superColumnName, new ColumnSlicePredicate(columnNames));
			return family.ExecuteOperation(op);
		}

		public static IFluentSuperColumn<CompareWith, CompareSubcolumnWith> GetSingleSuperColumn<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, string key, CompareWith superColumnName, CompareSubcolumnWith columnStart, CompareSubcolumnWith columnEnd, bool reversed = false, int count = 100)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var op = new GetSuperColumnSlice<CompareWith, CompareSubcolumnWith>(key, superColumnName, new RangeSlicePredicate(columnStart, columnEnd, reversed, count));
			return family.ExecuteOperation(op);
		}

		#endregion

		#region GetSingle

		public static IFluentSuperColumnFamily<CompareWith, CompareSubcolumnWith> GetSingle<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, string key, IEnumerable<CompareWith> columnNames)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var op = new GetSuperColumnFamilySlice<CompareWith, CompareSubcolumnWith>(key, new ColumnSlicePredicate(columnNames));
			return family.ExecuteOperation(op);
		}

		public static IFluentSuperColumnFamily<CompareWith, CompareSubcolumnWith> GetSingle<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, string key, CompareWith columnStart, CompareWith columnEnd, bool reversed = false, int count = 100)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var op = new GetSuperColumnFamilySlice<CompareWith, CompareSubcolumnWith>(key, new RangeSlicePredicate(columnStart, columnEnd, reversed, count));
			return family.ExecuteOperation(op);
		}

		#endregion

		#region GetSuperColumns

		public static IEnumerable<IFluentSuperColumn<CompareWith, CompareSubcolumnWith>> GetSuperColumns<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, IEnumerable<string> keys, CompareWith superColumnName, IEnumerable<CompareSubcolumnWith> columnNames)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var op = new MultiGetSuperColumnSlice<CompareWith, CompareSubcolumnWith>(keys, superColumnName, new ColumnSlicePredicate(columnNames));
			return family.ExecuteOperation(op);
		}

		public static IEnumerable<IFluentSuperColumn<CompareWith, CompareSubcolumnWith>> GetSuperColumns<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, IEnumerable<string> keys, CompareWith superColumnName, CompareSubcolumnWith columnStart, CompareSubcolumnWith columnEnd, bool reversed = false, int count = 100)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var op = new MultiGetSuperColumnSlice<CompareWith, CompareSubcolumnWith>(keys, superColumnName, new RangeSlicePredicate(columnStart, columnEnd, reversed, count));
			return family.ExecuteOperation(op);
		}

		#endregion

		#region Get

		public static IEnumerable<IFluentSuperColumnFamily<CompareWith, CompareSubcolumnWith>> Get<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, IEnumerable<string> keys, IEnumerable<CompareWith> columnNames)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var op = new MultiGetSuperColumnFamilySlice<CompareWith, CompareSubcolumnWith>(keys, new ColumnSlicePredicate(columnNames));
			return family.ExecuteOperation(op);
		}

		public static IEnumerable<IFluentSuperColumnFamily<CompareWith, CompareSubcolumnWith>> Get<CompareWith, CompareSubcolumnWith>(this CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> family, IEnumerable<string> keys, CompareWith columnStart, CompareWith columnEnd, bool reversed = false, int count = 100)
			where CompareWith : CassandraType
			where CompareSubcolumnWith : CassandraType
		{
			var op = new MultiGetSuperColumnFamilySlice<CompareWith, CompareSubcolumnWith>(keys, new RangeSlicePredicate(columnStart, columnEnd, reversed, count));
			return family.ExecuteOperation(op);
		}

		#endregion
	}
}
