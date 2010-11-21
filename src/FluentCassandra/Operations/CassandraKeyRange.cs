using System;
using Apache.Cassandra;
using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class CassandraKeyRange
	{
		public CassandraKeyRange(BytesType startKey, BytesType endKey, string startToken, string endToken, int count)
		{
			StartKey = startKey;
			EndKey = endKey;
			StartToken = startToken;
			EndToken = endToken;
			Count = count;
		}

		public BytesType StartKey { get; private set; }
		public BytesType EndKey { get; private set; }
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
