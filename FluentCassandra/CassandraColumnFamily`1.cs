using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Linq.Expressions;
using Thrift.Transport;
using Thrift.Protocol;
using Apache.Cassandra;
using FluentCassandra.Configuration;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public class CassandraColumnFamily<CompareWith> : BaseCassandraColumnFamily
		where CompareWith : CassandraType
	{
		public CassandraColumnFamily(CassandraContext context, string columnFamily)
			: base(context, columnFamily) { }

		public Type CompareWithType { get { return typeof(CompareWith); } }

		public FluentColumnFamily<CompareWith> CreateFluentFamily(string key)
		{
			return new FluentColumnFamily<CompareWith>(key, FamilyName);
		}
	}
}