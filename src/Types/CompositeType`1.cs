using System;
using System.Collections.Generic;
using System.Linq;

namespace FluentCassandra.Types
{
	public class CompositeType<T1> : CompositeType
		where T1 : CassandraType
	{
		public CompositeType(T1 t1)
		{
			SetValue(new CassandraType[] { t1 });
		}

		public CompositeType() { }

		public T1 Item1 { get { return (T1)GetValue<List<CassandraType>>()[0]; } }
	}

	public class CompositeType<T1, T2> : CompositeType
		where T1 : CassandraType
		where T2 : CassandraType
	{
		public CompositeType(T1 t1, T2 t2)
		{
			SetValue(new CassandraType[] { t1, t2 });
		}

		public CompositeType() { }

		public T1 Item1 { get { return (T1)GetValue<List<CassandraType>>()[0]; } }
		public T2 Item2 { get { return (T2)GetValue<List<CassandraType>>()[1]; } }
	}

	public class CompositeType<T1, T2, T3> : CompositeType
		where T1 : CassandraType
		where T2 : CassandraType
		where T3 : CassandraType
	{
		public CompositeType(T1 t1, T2 t2, T3 t3)
		{
			SetValue(new CassandraType[] { t1, t2, t3 });
		}

		public CompositeType() { }

		public T1 Item1 { get { return (T1)GetValue<List<CassandraType>>()[0]; } }
		public T2 Item2 { get { return (T2)GetValue<List<CassandraType>>()[1]; } }
		public T3 Item3 { get { return (T3)GetValue<List<CassandraType>>()[2]; } }
	}

	public class CompositeType<T1, T2, T3, T4> : CompositeType
		where T1 : CassandraType
		where T2 : CassandraType
		where T3 : CassandraType
		where T4 : CassandraType
	{
		public CompositeType(T1 t1, T2 t2, T3 t3, T4 t4)
		{
			SetValue(new CassandraType[] { t1, t2, t3, t4 });
		}

		public CompositeType() { }

		public T1 Item1 { get { return (T1)GetValue<List<CassandraType>>()[0]; } }
		public T2 Item2 { get { return (T2)GetValue<List<CassandraType>>()[1]; } }
		public T3 Item3 { get { return (T3)GetValue<List<CassandraType>>()[2]; } }
		public T4 Item4 { get { return (T4)GetValue<List<CassandraType>>()[3]; } }
	}

	public class CompositeType<T1, T2, T3, T4, T5> : CompositeType
		where T1 : CassandraType
		where T2 : CassandraType
		where T3 : CassandraType
		where T4 : CassandraType
		where T5 : CassandraType
	{
		public CompositeType(T1 t1, T2 t2, T3 t3, T4 t4, T5 t5)
		{
			SetValue(new CassandraType[] { t1, t2, t3, t4, t5 });
		}

		public CompositeType() { }

		public T1 Item1 { get { return (T1)GetValue<List<CassandraType>>()[0]; } }
		public T2 Item2 { get { return (T2)GetValue<List<CassandraType>>()[1]; } }
		public T3 Item3 { get { return (T3)GetValue<List<CassandraType>>()[2]; } }
		public T4 Item4 { get { return (T4)GetValue<List<CassandraType>>()[3]; } }
		public T5 Item5 { get { return (T5)GetValue<List<CassandraType>>()[4]; } }
	}
}
