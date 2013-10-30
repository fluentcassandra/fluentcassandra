namespace FluentCassandra.Types
{
    internal class SetTypeConverter<T> : ListTypeConverter<T> where T : CassandraObject
    {
        protected override string CollectionStringBegin
        {
            get { return "{"; }
        }

        protected override string CollectionStringEnd
        {
            get { return "}"; }
        }
    }
}
