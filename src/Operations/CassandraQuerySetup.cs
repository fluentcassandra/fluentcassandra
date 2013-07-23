using FluentCassandra.Types;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace FluentCassandra.Operations
{
	internal class CassandraQuerySetup
	{
		public CassandraQuerySetup()
		{
			Reverse = false;

			KeyCount = 100;
			ColumnCount = 100;

			Keys = new CassandraObject[0];
			Columns = new CassandraObject[0];
		}

		public IEnumerable<CassandraObject> Keys
		{
			get;
			internal set;
		}

		public IEnumerable<CassandraObject> Columns
		{
			get;
			internal set;
		}

		public CassandraObject SuperColumnName
		{
			get;
			internal set;
		}

		public int KeyCount
		{
			get;
			internal set;
		}

		public int ColumnCount
		{
			get;
			internal set;
		}

		public CassandraObject StartKey
		{
			get;
			internal set;
		}

		public CassandraObject EndKey
		{
			get;
			internal set;
		}

		public CassandraObject StartColumn
		{
			get;
			internal set;
		}

		public CassandraObject EndColumn
		{
			get;
			internal set;
		}

		public string StartToken
		{
			get;
			internal set;
		}

		public string EndToken
		{
			get;
			internal set;
		}

		public bool Reverse
		{
			get;
			internal set;
		}

		public Expression<Func<IFluentRecordExpression, bool>> IndexClause
		{
			get;
			internal set;
		}
	}
}