using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public interface IFluentBaseColumn<CompareWith> : IFluentBaseColumn
		where CompareWith : CassandraType
	{
		new CompareWith ColumnName { get; }
	}
}
