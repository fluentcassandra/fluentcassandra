using FluentCassandra.Types;

namespace FluentCassandra
{
	public interface IFluentBaseColumn
	{
		CassandraObject ColumnName { get; }

		IFluentBaseColumnFamily Family { get; }

		CassandraColumnSchema GetSchema();
		void SetSchema(CassandraColumnSchema schema);

		FluentColumnPath GetPath();
		FluentColumnParent GetParent();

		void SetParent(FluentColumnParent parent);
	}
}
