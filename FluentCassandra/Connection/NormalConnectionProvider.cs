using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FluentCassandra
{
	public class NormalConnectionProvider : ConnectionProvider
	{
		/// <summary>
		/// 
		/// </summary>
		/// <param name="builder"></param>
		public NormalConnectionProvider(ConnectionBuilder builder)
			: base(builder) { }

		/// <summary>
		/// 
		/// </summary>
		/// <returns></returns>
		public override IConnection CreateNewConnection()
		{
			var server = Builder.Servers.FirstOrDefault();

			if (server == null)
				throw new CassandraException("No connection could be made because no servers were defined.");

			var conn = new Connection(server, Builder.Timeout);
			return conn;
		}
	}
}
