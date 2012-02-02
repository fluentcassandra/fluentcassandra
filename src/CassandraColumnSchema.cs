using System;
using System.Collections.Generic;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public class CassandraColumnSchema
	{
		private Type _nameType;

		public CassandraType Name { get; set; }

		public Type NameType
		{
			get
			{
				if (_nameType == null && Name != null)
					_nameType = Name.GetType();

				return _nameType;
			}
			set { _nameType = value; }
		}

		public virtual Type ValueType { get; set; }
	}

	public class CassandraSuperColumnSchema : CassandraColumnSchema
	{
		public Type ColumnNameType { get; set; }

		public IList<CassandraColumnSchema> Columns { get; set; }

		public override Type ValueType
		{
			get { throw new NotSupportedException(); }
			set { throw new NotSupportedException(); }
		}
	}
}