using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;

namespace FluentCassandra.Operations
{
	public class CassandraKeyRange
	{
		public CassandraKeyRange(string startKey, string endKey, string startToken, string endToken, int count)
		{
			StartKey = startKey;
			EndKey = endKey;
			StartToken = startToken;
			EndToken = endToken;
			Count = count;
		}

		public string StartKey { get; private set; }
		public string EndKey { get; private set; }
		public string StartToken { get; private set; }
		public string EndToken { get; private set; }
		public int Count { get; private set; }

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
