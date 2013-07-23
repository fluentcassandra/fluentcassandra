
namespace FluentCassandra.Linq
{
	internal interface ICassandraColumnFamilyInfo
	{
		string FamilyName { get; }
		CassandraColumnFamilySchema GetSchema();
	}
}
