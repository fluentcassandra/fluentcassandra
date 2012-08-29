using System;
using System.Collections.Generic;
using System.Linq;
using FluentCassandra.Linq;

namespace FluentCassandra.ObjectSerializer
{
	public interface IObjectSerializer
	{
		Func<ICqlRow, object> GenerateRowDeserializer();

		object Deserialize(ICqlRow row);
		IEnumerable<object> Deserialize(IEnumerable<ICqlRow> rows);
	}
}
