using System;
using System.Linq;
using FluentCassandra.Types;
using System.Dynamic;
using System.Collections.Generic;

namespace FluentCassandra.Linq
{
	public interface ICqlRow : IDynamicMetaObjectProvider
	{
		CassandraType Key { get; set; }
		CassandraType this[CassandraType columnName] { get; }

		IList<FluentColumn> Columns { get; }
	}
}
