using System;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public class CassandraSuperColumnFamily : BaseCassandraColumnFamily
	{
		public CassandraSuperColumnFamily(CassandraContext context, string columnFamily)
			: base(context, columnFamily) { }

		public FluentSuperColumnFamily CreateRecord(CassandraType key)
		{
			if (key.GetValue<byte[]>().Length == 0)
				throw new ArgumentException("'key' is not allowed to be zero length.", "key");

			return new FluentSuperColumnFamily(key, FamilyName);
		}
	}
}
