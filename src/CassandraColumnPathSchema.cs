using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public class CassandraColumnPathSchema
	{
		public string ColumnFamily { get; set; }
		public CassandraType SuperColumnNameType { get; set; }
		public CassandraType ColumnNameType { get; set; }
	}
}
