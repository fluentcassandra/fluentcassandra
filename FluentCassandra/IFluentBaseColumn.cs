using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public interface IFluentBaseColumn
	{
		CassandraType Name { get; }

		FluentColumnPath GetPath();
		FluentColumnParent GetParent();

		void SetParent(FluentColumnParent parent);
	}
}
