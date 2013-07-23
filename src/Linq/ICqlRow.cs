using FluentCassandra.Types;
using System.Collections.Generic;
using System.Dynamic;

namespace FluentCassandra.Linq
{
	public interface ICqlRow : IDynamicMetaObjectProvider
	{
		CassandraObject Key { get; }
		CassandraObject this[CassandraObject columnName] { get; }

		IList<FluentColumn> Columns { get; }
	}
}
