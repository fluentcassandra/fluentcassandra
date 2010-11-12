using System;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public class CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> : BaseCassandraColumnFamily
		where CompareWith : CassandraType
		where CompareSubcolumnWith : CassandraType
	{
		public CassandraSuperColumnFamily(CassandraContext context, string columnFamily)
			: base(context, columnFamily) { }

		public Type CompareWithType { get { return typeof(CompareWith); } }
		public Type CompareSubcolumnWithType { get { return typeof(CompareSubcolumnWith); } }

		public FluentSuperColumnFamily<CompareWith, CompareSubcolumnWith> CreateRecord(string key)
		{
			return new FluentSuperColumnFamily<CompareWith, CompareSubcolumnWith>(key, FamilyName);
		}
	}
}
