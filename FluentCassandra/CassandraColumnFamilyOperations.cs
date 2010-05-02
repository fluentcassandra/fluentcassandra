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
			var name = path.Column.Name;
			var value = path.Column.Value;
			var timestamp = path.Column.Timestamp;

			var op = new InsertColumn(key, name, value, timestamp);
			family.ExecuteOperation(op);
		}

		public static void InsertColumn<CompareWith>(this CassandraColumnFamily<CompareWith> family, string key, CompareWith name, BytesType value)
			where CompareWith : CassandraType
		{
			InsertColumn<CompareWith>(family, key, name, value, DateTimeOffset.UtcNow);
		}

		public static void InsertColumn<CompareWith>(this CassandraColumnFamily<CompareWith> family, string key, CompareWith name, BytesType value, DateTimeOffset timestamp)
			where CompareWith : CassandraType
		{
			var op = new InsertColumn(key, name, value, timestamp);
			family.ExecuteOperation(op);
		}

		#endregion
	}
}
