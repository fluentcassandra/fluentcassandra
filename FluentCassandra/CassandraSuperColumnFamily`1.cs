using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public class CassandraSuperColumnFamily<CompareWith, CompareSubcolumnWith> : BaseCassandraColumnFamily
		where CompareWith : CassandraType
		where CompareSubcolumnWith : CassandraType
	{
		public CassandraSuperColumnFamily(CassandraContext context, CassandraKeyspace keyspace, IConnection connection, string columnFamily)
			: base(context, keyspace, connection, columnFamily) { }

		public Type CompareWithType { get { return typeof(CompareWith); } }
	}
}
