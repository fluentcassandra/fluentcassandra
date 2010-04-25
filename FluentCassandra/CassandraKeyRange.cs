using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FluentCassandra
{
	public class CassandraKeyRange
	{
		public CassandraKeyRange()
		{
			Count = 100;
		}

		public string StartKey { get; set; }
		public string EndKey { get; set; }
		public string StartToken { get; set; }
		public string EndToken { get; set; }
		public int Count { get; set; }
	}
}
