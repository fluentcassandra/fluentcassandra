using FluentCassandra.Types;

namespace FluentCassandra.Operations
{
	public class CassandraKeyRange
	{
		public CassandraKeyRange(CassandraObject startKey, CassandraObject endKey, string startToken, string endToken, int count)
		{
			StartKey = startKey;
			EndKey = endKey;
			StartToken = startToken;
			EndToken = endToken;
			Count = count;
		}

		public CassandraObject StartKey { get; private set; }
		public CassandraObject EndKey { get; private set; }
		public string StartToken { get; private set; }
		public string EndToken { get; private set; }
		public int Count { get; private set; }
	}
}
