using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FluentCassandra
{
	public abstract class ConnectionProvider : IConnectionProvider
	{
		public abstract IConnection Open();
		public abstract bool Close(IConnection connection);

		/// <summary>
		/// 
		/// </summary>
		/// <param name="builder"></param>
		protected ConnectionProvider(ConnectionBuilder builder)
		{
			Builder = builder;
		}

		/// <summary>
		/// 
		/// </summary>
		public ConnectionBuilder Builder { get; private set; }

		/// <summary>
		/// 
		/// </summary>
		/// <returns></returns>
		public IConnection CreateNewConnection()
		{
			var conn = new Connection(Builder);
			return conn;
		}
	}
}
