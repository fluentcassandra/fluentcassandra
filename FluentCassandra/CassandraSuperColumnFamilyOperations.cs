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
	}
}
