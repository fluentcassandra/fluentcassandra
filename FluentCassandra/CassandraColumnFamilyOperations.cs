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
		public static int ColumnCount<CompareWith>(this CassandraColumnFamily<CompareWith> family, string key) 
			where CompareWith : CassandraType
		{
			var op = new ColumnCount(key);
			return family.ExecuteOperation(op);
		}
	}
}
