using System;
using System.Collections.Generic;
using System.Linq;
using FluentCassandra.Linq;

namespace FluentCassandra.ObjectSerializer
{
	public interface IObjectSerializer
	{
		Func<ICqlRow, ObjectSerializerConventions, object> GenerateRowDeserializer();

		object Deserialize(ICqlRow row, ObjectSerializerConventions conventions = null);
		IEnumerable<object> Deserialize(IEnumerable<ICqlRow> rows, ObjectSerializerConventions conventions = null);
	}
}
