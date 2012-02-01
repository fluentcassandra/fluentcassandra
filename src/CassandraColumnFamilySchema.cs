using System;
using System.Collections.Generic;
using System.Linq;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public class CassandraColumnFamilySchema
	{
		public CassandraColumnFamilySchema()
		{
			Key = typeof(BytesType);
			Columns = new Dictionary<CassandraType, Type>();
		}

		public string FamilyName { get; set; }
		public Type Key { get; set; }
		public IDictionary<CassandraType, Type> Columns { get; set; }
	}
}
