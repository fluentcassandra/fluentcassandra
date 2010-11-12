using System;
using System.Collections.Generic;

namespace FluentCassandra
{
	public class CassandraTokenRange
	{
		internal CassandraTokenRange(string startToken, string endToken, IList<string> endPoints)
		{
			StartToken = startToken;
			EndToken = endToken;
			EndPoints = endPoints;
		}

		public string StartToken { get; private set; }
		public string EndToken { get; private set; }
		public IList<string> EndPoints { get; private set; }
	}
}
