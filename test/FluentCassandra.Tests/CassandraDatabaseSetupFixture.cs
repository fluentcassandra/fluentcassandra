using System;
using System.Linq;

namespace FluentCassandra
{
	public class CassandraDatabaseSetupFixture
	{
		private static bool DatabaseHasBeenCleaned = false;

		public CassandraDatabaseSetup DatabaseSetup(bool? reset = null)
		{
			if (reset == null && !DatabaseHasBeenCleaned)
			{
				DatabaseHasBeenCleaned = true;
				
				// refresh the entire database
				return new CassandraDatabaseSetup(reset: true);
			}

			return new CassandraDatabaseSetup(reset: reset ?? false);
		}
	}
}
