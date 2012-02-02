using System;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public partial class CassandraColumnFamily : BaseCassandraColumnFamily
	{
		public CassandraColumnFamily(CassandraContext context, string columnFamily)
			: base(context, columnFamily) { }

		public FluentColumnFamily CreateRecord(CassandraType key)
		{
			if (key.GetValue<byte[]>().Length == 0)
				throw new ArgumentException("'key' is not allowed to be zero length.", "key");

			return new FluentColumnFamily(key, FamilyName);
		}
	}
}