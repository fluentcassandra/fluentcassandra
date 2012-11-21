﻿using System;
using FluentCassandra.Apache.Cassandra;
using System.Collections.Generic;

namespace FluentCassandra.Connections
{
	public interface IConnectionBuilder
	{
		string Keyspace { get; }
		IList<Server> Servers { get; }

		bool Pooling { get; }
		int MinPoolSize { get; }
		int MaxPoolSize { get; }
		int MaxRetries { get; }
		TimeSpan ConnectionTimeout { get; }
		TimeSpan ConnectionLifetime { get; }
        TimeSpan ServerRecoveryInterval { get; }
		ConnectionType ConnectionType { get; }
        int BufferSize { get; }
		ConsistencyLevel ReadConsistency { get; }
		ConsistencyLevel WriteConsistency { get; }

		string CqlVersion { get; }
		bool CompressCqlQueries { get; }

		string Username { get; }
		string Password { get; }

		string Uuid { get; }
	}
}
