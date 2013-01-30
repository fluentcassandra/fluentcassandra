using System;
using System.Linq;
using FluentCassandra.Connections;

namespace FluentCassandra
{
	public class CassandraDatabaseSetupFixture
	{
		private static bool DatabaseHasBeenCleaned = false;

		public CassandraDatabaseSetup DatabaseSetup(bool? reset = null, string cqlVersion = CqlVersion.Edge)
		{
			if (reset == null && !DatabaseHasBeenCleaned)
			{
				DatabaseHasBeenCleaned = true;
				
				// refresh the entire database
				return new CassandraDatabaseSetup(reset: true, cqlVersion: cqlVersion);
			}

			return new CassandraDatabaseSetup(reset: reset ?? false, cqlVersion: cqlVersion);
		}
	}
}
