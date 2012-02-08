using System;
using System.Linq;
using FluentCassandra.Types;
using System.Dynamic;
using System.Collections.Generic;

namespace FluentCassandra.Linq
{
	public interface ICqlRow : IDynamicMetaObjectProvider
	{
		CassandraObject Key { get; set; }
		CassandraObject this[CassandraObject columnName] { get; }

		IList<FluentColumn> Columns { get; }
	}
}
