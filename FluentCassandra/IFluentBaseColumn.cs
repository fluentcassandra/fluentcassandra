using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public interface IFluentBaseColumn
	{
		CassandraType ColumnName { get; }

		IFluentBaseColumnFamily Family { get; }

		FluentColumnPath GetPath();
		FluentColumnParent GetParent();

		void SetParent(FluentColumnParent parent);
	}
}
