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
		public CassandraSuperColumnFamily(CassandraContext context, string columnFamily)
			: base(context, columnFamily) { }

		public Type CompareWithType { get { return typeof(CompareWith); } }
		public Type CompareSubcolumnWithType { get { return typeof(CompareSubcolumnWith); } }

		public FluentSuperColumnFamily<CompareWith, CompareSubcolumnWith> CreateFluentFamily(string key)
		{
			return new FluentSuperColumnFamily<CompareWith, CompareSubcolumnWith>(key, FamilyName);
		}
	}
}
