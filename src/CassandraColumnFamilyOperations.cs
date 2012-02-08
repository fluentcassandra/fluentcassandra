using System;
using System.Collections.Generic;
using FluentCassandra.Types;
using FluentCassandra.Operations;
using System.Linq.Expressions;
using FluentCassandra.Linq;

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

		public static void InsertColumn(this CassandraColumnFamily family, CassandraObject key, CassandraObject columnName, long columnValue)
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

		public static ICassandraQueryable<FluentColumnFamily, CassandraObject> Get(this CassandraColumnFamily family, params CassandraObject[] keys)
		{
			var setup = new CassandraQuerySetup<FluentColumnFamily, CassandraObject> {
				Keys = keys,
				CreateQueryOperation = (s, slice) => new MultiGetColumnFamilySlice(s.Keys, slice)
			};
			return ((ICassandraQueryProvider)family).CreateQuery(setup, null);
		}

		public static ICassandraQueryable<FluentColumnFamily, CassandraObject> Get(this CassandraColumnFamily family, CassandraObject startKey, CassandraObject endKey, string startToken, string endToken, int keyCount)
		{
			var setup = new CassandraQuerySetup<FluentColumnFamily, CassandraObject> {
				KeyRange = new CassandraKeyRange(startKey, endKey, startToken, endToken, keyCount),
				CreateQueryOperation = (s, slice) => new GetColumnFamilyRangeSlices(s.KeyRange, slice)
			};
			return ((ICassandraQueryProvider)family).CreateQuery(setup, null);
		}

		public static ICassandraQueryable<FluentColumnFamily, CassandraObject> Get(this CassandraColumnFamily family, CassandraObject startKey, int keyCount, Expression<Func<IFluentRecordExpression, bool>> expression)
		{
			var setup = new CassandraQuerySetup<FluentColumnFamily, CassandraObject> {
				IndexClause = new CassandraIndexClause(startKey, keyCount, expression),
				CreateQueryOperation = (s, slice) => new GetColumnFamilyIndexedSlices(s.IndexClause, slice)
			};
			return ((ICassandraQueryProvider)family).CreateQuery(setup, null);
		}

		#endregion

		public static IEnumerable<ICqlRow> Cql(this CassandraColumnFamily family, UTF8Type cqlQuery)
		{
			var op = new ExecuteCqlQuery(cqlQuery, family.Context.ConnectionBuilder.CompressCqlQueries);
			return family.ExecuteOperation(op);
		}
	}
}