using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Thrift.Transport;
using Thrift.Protocol;
using Apache.Cassandra;

namespace FluentCassandra
{
	public class CassandraKeyspace
	{
		private readonly string _keyspaceName;

		/// <summary>
		/// 
		/// </summary>
		/// <param name="keyspaceName"></param>
		/// <param name="connecton"></param>
		public CassandraKeyspace(string keyspaceName)
		{
			_keyspaceName = keyspaceName;
		}

		/// <summary>
		/// 
		/// </summary>
		public string KeyspaceName
		{
			get { return _keyspaceName; }
		}

		#region Cassandra Descriptions For Server

		public IEnumerable<CassandraTokenRange> DescribeRingFor(Server server)
		{
			using (var session = new CassandraSession(server))
			{
				var tokenRanges = session.GetClient().describe_ring(KeyspaceName);

				foreach (var tokenRange in tokenRanges)
					yield return new CassandraTokenRange(tokenRange.Start_token, tokenRange.End_token, tokenRange.Endpoints);
			}
		}

		public IDictionary<string, Dictionary<string, string>> DescribeFor(Server server)
		{
			using (var session = new CassandraSession(server))
			{
				var desc = session.GetClient().describe_keyspace(KeyspaceName);
				return desc;
			}
		}

		#endregion
	}
}
