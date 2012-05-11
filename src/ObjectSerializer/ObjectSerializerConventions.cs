using System;
using System.Collections.Generic;
using System.Linq;

namespace FluentCassandra.ObjectSerializer
{
	public class ObjectSerializerConventions
	{
		public ObjectSerializerConventions()
		{
			AreKeyPropertyNamesCaseSensitive = false;
			KeyPropertyNames = new List<string>(new[] { "Key", "Id" });
		}

		public bool AreKeyPropertyNamesCaseSensitive { get; set; }
		public List<string> KeyPropertyNames { get; set; }
	}
}
