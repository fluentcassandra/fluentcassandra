using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;

namespace FluentCassandra.Operations
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

		internal KeyRange CreateKeyRange()
		{
			return new KeyRange {
				Start_key = StartKey,
				End_key = EndKey,
				Start_token = StartToken,
				End_token = EndToken,
				Count = Count
			};
		}
	}
}
