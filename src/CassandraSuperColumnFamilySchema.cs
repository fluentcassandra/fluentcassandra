using System;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public class CassandraSuperColumnFamilySchema : CassandraColumnFamilySchema
	{
		public CassandraSuperColumnFamilySchema()
		{
			SubColumnNameType = typeof(BytesType);
		}

		public Type SubColumnNameType { get; set; }
	}
}