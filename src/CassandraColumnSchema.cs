using System;
using FluentCassandra.Types;

namespace FluentCassandra
{
	public class CassandraColumnSchema
	{
		public CassandraColumnSchema()
		{
			NameType = typeof(BytesType);
			ValueType = typeof(BytesType);
		}

		private Type _nameType;
		private CassandraType _name;

		public CassandraType Name
		{
			get { return _name; }
			set
			{
				if (value != null)
					NameType = value.GetType();

				_name = value;
			}
		}

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
}