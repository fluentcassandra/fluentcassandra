﻿using FluentCassandra.Types;
using System;

namespace FluentCassandra.Operations
{
	public class CassandraColumn
	{
		public CassandraObject Name { get; set; }
		public CassandraObject Value { get; set; }
		public DateTimeOffset Timestamp { get; set; }
		public int? Ttl { get; set; }
	}
}
