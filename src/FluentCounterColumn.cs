using System;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public class FluentCounterColumn : FluentColumn
	{
		public FluentCounterColumn(CassandraColumnSchema schema = null)
			: base(schema)
		{
			schema = GetSchema();
			schema.ValueType = CassandraType.CounterColumnType;
			
			// ensure value type is set to column counter type
			SetSchema(schema);
		}
	}
}