using System;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public class CassandraColumnFamily<CompareWith> : BaseCassandraColumnFamily
		where CompareWith : CassandraType
	{
		public CassandraColumnFamily(CassandraContext context, string columnFamily)
			: base(context, columnFamily) { }

		public Type CompareWithType { get { return typeof(CompareWith); } }

		public FluentColumnFamily<CompareWith> CreateRecord(BytesType key)
		{
			return new FluentColumnFamily<CompareWith>(key, FamilyName);
		}
	}
}