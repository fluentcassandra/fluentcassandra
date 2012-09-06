using System;
using System.Collections.Generic;
using FluentCassandra.Operations;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public static class CassandraSuperColumnFamilyOperations
	{
		#region ColumnCount

		public static int ColumnCount(this CassandraSuperColumnFamily family, CassandraObject key, IEnumerable<CassandraObject> columnNames)
		{
			var op = new ColumnCount(key, new CassandraColumnSlicePredicate(columnNames));
			return family.ExecuteOperation(op);
		}

		public static int ColumnCount(this CassandraSuperColumnFamily family, CassandraObject key, CassandraObject columnStart, CassandraObject columnEnd, bool reversed = false, int count = 100)
		{
			var op = new ColumnCount(key, new CassandraRangeSlicePredicate(columnStart, columnEnd, reversed, count));
			return family.ExecuteOperation(op);
		}

		public static int SuperColumnCount(this CassandraSuperColumnFamily family, CassandraObject key, CassandraObject superColumnName, IEnumerable<CassandraObject> columnNames)
		{
			var op = new ColumnCount(key, superColumnName, new CassandraColumnSlicePredicate(columnNames));
			return family.ExecuteOperation(op);
		}

		public static int SuperColumnCount(this CassandraSuperColumnFamily family, CassandraObject key, CassandraObject superColumnName, CassandraObject columnStart, CassandraObject columnEnd, bool reversed = false, int count = 100)
		{
			var op = new ColumnCount(key, superColumnName, new CassandraRangeSlicePredicate(columnStart, columnEnd, reversed, count));
			return family.ExecuteOperation(op);
		}

		#endregion

		#region InsertColumn

		public static void InsertColumn(this CassandraSuperColumnFamily family, CassandraObject key, FluentColumn column)
		{
			InsertColumn(family, key, column.GetPath());
		}

		public static void InsertColumn(this CassandraSuperColumnFamily family, CassandraObject key, FluentColumnPath path)
		{
			var superColumnName = path.SuperColumn.ColumnName;
			var name = path.Column.ColumnName;
			var value = path.Column.ColumnValue;
			var timestamp = path.Column.ColumnTimestamp;
			var timeToLive = path.Column.ColumnSecondsUntilDeleted;

			var op = new InsertColumn(key, superColumnName, name, value, timestamp, timeToLive);
			family.ExecuteOperation(op);
		}

		public static void InsertColumn(this CassandraSuperColumnFamily family, CassandraObject key, CassandraObject superColumnName, CassandraObject name, BytesType value)
		{
			InsertColumn(family, key, superColumnName, name, value, DateTimeOffset.UtcNow, null);
		}

		public static void InsertColumn(this CassandraSuperColumnFamily family, CassandraObject key, CassandraObject superColumnName, CassandraObject name, BytesType value, DateTimeOffset timestamp, int? timeToLive)
		{
			var op = new InsertColumn(key, superColumnName, name, value, timestamp, timeToLive);
			family.ExecuteOperation(op);
		}

		#endregion

		#region AddColumn

		public static void InsertCounterColumn(this CassandraSuperColumnFamily family, CassandraObject key, CassandraObject superColumnName, CassandraObject columnName, long columnValue)
		{
			var op = new AddColumn(key, superColumnName, columnName, columnValue);
			family.ExecuteOperation(op);
		}

		#endregion

		#region GetColumn

		public static FluentColumn GetColumn(this CassandraSuperColumnFamily family, CassandraObject key, FluentColumnPath path)
		{
			var columnName = path.Column.ColumnName;
			var superColumnName = path.SuperColumn.ColumnName;
			return GetColumn(family, key, superColumnName, columnName);
		}

		public static FluentColumn GetColumn(this CassandraSuperColumnFamily family, CassandraObject key, CassandraObject superColumnName, CassandraObject columnName)
		{
			var op = new GetColumn(key, superColumnName, columnName);
			return family.ExecuteOperation(op);
		}

		#endregion

		#region GetSuperColumn

		public static FluentSuperColumn GetSuperColumn(this CassandraSuperColumnFamily family, CassandraObject key, FluentColumnParent parent)
		{
			var superColumnName = parent.SuperColumn.ColumnName;
			return GetSuperColumn(family, key, superColumnName);
		}

		public static FluentSuperColumn GetSuperColumn(this CassandraSuperColumnFamily family, CassandraObject key, CassandraObject superColumnName)
		{
			var op = new GetSuperColumn(key, superColumnName);
			return family.ExecuteOperation(op);
		}

		#endregion

		#region RemoveColumn

		public static void RemoveColumn(this CassandraSuperColumnFamily family, CassandraObject key, FluentColumnPath path)
		{
			var columnName = path.Column.ColumnName;
			var superColumnName = path.SuperColumn.ColumnName;
			RemoveColumn(family, key, superColumnName, columnName);
		}

		public static void RemoveColumn(this CassandraSuperColumnFamily family, CassandraObject key, CassandraObject superColumnName)
		{
			RemoveColumn(family, key, superColumnName, null);
		}

		public static void RemoveColumn(this CassandraSuperColumnFamily family, CassandraObject key, CassandraObject superColumnName, CassandraObject columnName)
		{
			var op = new Remove(key, superColumnName, columnName);
			family.ExecuteOperation(op);
		}

		#endregion

		#region GetSingleSuperColumn

		public static FluentSuperColumn GetSingleSuperColumn(this CassandraSuperColumnFamily family, CassandraObject key, CassandraObject superColumnName, IEnumerable<CassandraObject> columnNames)
		{
			var op = new GetSuperColumnSlice(key, superColumnName, new CassandraColumnSlicePredicate(columnNames));
			return family.ExecuteOperation(op);
		}

		public static FluentSuperColumn GetSingleSuperColumn(this CassandraSuperColumnFamily family, CassandraObject key, CassandraObject superColumnName, CassandraObject columnStart, CassandraObject columnEnd, bool reversed = false, int count = 100)
		{
			var op = new GetSuperColumnSlice(key, superColumnName, new CassandraRangeSlicePredicate(columnStart, columnEnd, reversed, count));
			return family.ExecuteOperation(op);
		}

		#endregion

		#region GetSingle

		public static FluentSuperColumnFamily GetSingle(this CassandraSuperColumnFamily family, CassandraObject key, IEnumerable<CassandraObject> columnNames)
		{
			var op = new GetSuperColumnFamilySlice(key, new CassandraColumnSlicePredicate(columnNames));
			return family.ExecuteOperation(op);
		}

		public static FluentSuperColumnFamily GetSingle(this CassandraSuperColumnFamily family, CassandraObject key, CassandraObject columnStart, CassandraObject columnEnd, bool reversed = false, int count = 100)
		{
			var op = new GetSuperColumnFamilySlice(key, new CassandraRangeSlicePredicate(columnStart, columnEnd, reversed, count));
			return family.ExecuteOperation(op);
		}

		#endregion

		#region Get

		// queryable

		public static CassandraSlicePredicateQuery<FluentSuperColumnFamily> Get(this CassandraSuperColumnFamily family)
		{
			return family.CreateCassandraSlicePredicateQuery<FluentSuperColumnFamily>(null);
		}

		public static CassandraSlicePredicateQuery<FluentSuperColumnFamily> Get(this CassandraSuperColumnFamily family, params CassandraObject[] keys)
		{
			return Get(family).FetchKeys(keys);
		}

		#endregion
	}
}
