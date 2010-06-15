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
	}
}
