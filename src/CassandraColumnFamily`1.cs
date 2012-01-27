using System;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public partial class CassandraColumnFamily<CompareWith> : BaseCassandraColumnFamily
		where CompareWith : CassandraType
	{
		public CassandraColumnFamily(CassandraContext context, string columnFamily)
			: base(context, columnFamily) { }

		public Type CompareWithType { get { return typeof(CompareWith); } }

		public FluentColumnFamily<CompareWith> CreateRecord(BytesType key)
		{
			if (key.Length == 0)
				throw new ArgumentException("'key' is not allowed to be zero length.", "key");

			return new FluentColumnFamily<CompareWith>(key, FamilyName);
		}
	}
}