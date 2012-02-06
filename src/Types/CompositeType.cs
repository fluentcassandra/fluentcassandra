using System;
using System.Collections.Generic;
using System.Linq;

namespace FluentCassandra.Types
{
	public class CompositeType : CassandraType, IList<CassandraType>
	{
		private static readonly CompositeTypeConverter Converter = new CompositeTypeConverter();

		#region Create

		public static CompositeType<T1> Create<T1>(T1 t1) 
			where T1 : CassandraType
		{
			return new CompositeType<T1>(t1);
		}

		public static CompositeType<T1, T2> Create<T1, T2>(T1 t1, T2 t2)
			where T1 : CassandraType
			where T2 : CassandraType
		{
			return new CompositeType<T1, T2>(t1, t2);
		}

		public static CompositeType<T1, T2, T3> Create<T1, T2, T3>(T1 t1, T2 t2, T3 t3)
			where T1 : CassandraType
			where T2 : CassandraType
			where T3 : CassandraType
		{
			return new CompositeType<T1, T2, T3>(t1, t2, t3);
		}

		public static CompositeType<T1, T2, T3, T4> Create<T1, T2, T3, T4>(T1 t1, T2 t2, T3 t3, T4 t4)
			where T1 : CassandraType
			where T2 : CassandraType
			where T3 : CassandraType
			where T4 : CassandraType
		{
			return new CompositeType<T1, T2, T3, T4>(t1, t2, t3, t4);
		}

		public static CompositeType<T1, T2, T3, T4, T5> Create<T1, T2, T3, T4, T5>(T1 t1, T2 t2, T3 t3, T4 t4, T5 t5)
			where T1 : CassandraType
			where T2 : CassandraType
			where T3 : CassandraType
			where T4 : CassandraType
			where T5 : CassandraType
		{
			return new CompositeType<T1, T2, T3, T4, T5>(t1, t2, t3, t4, t5);
		}

		public static CompositeType Create(params CassandraType[] types)
		{
			return new CompositeType { _value = types.ToList() };
		}

		#endregion

		public CompositeType()
		{
			ComponentTypeHints = new List<Type>();
		}

		#region Implimentation

		protected override object GetValueInternal(Type type)
		{
			return Converter.ConvertTo(_value, type);
		}

		public override void SetValue(object obj)
		{
			if (obj != null && obj.GetType().GetInterfaces().Contains(typeof(IEnumerable<CassandraType>)))
				ComponentTypeHints = ((IEnumerable<CassandraType>)obj).Select(t => t.GetType()).ToList();

			_value = Converter.ConvertFrom(obj);
		}

		public override byte[] ToBigEndian()
		{
			return Converter.ToBigEndian(_value);
		}

		public override void SetValueFromBigEndian(byte[] value)
		{
			_value = Converter.FromBigEndian(value, ComponentTypeHints);
		}

		protected override TypeCode TypeCode
		{
			get { return TypeCode.Object; }
		}

		public override string ToString()
		{
			return GetValue<string>();
		}

		#endregion

		protected override object GetRawValue() { return _value; }

		public List<Type> ComponentTypeHints { get; set; }

		private List<CassandraType> _value;

		#region Equality

		public override bool Equals(object obj)
		{
			List<CassandraType> objArray;

			if (obj is CompositeType)
				objArray = ((CompositeType)obj)._value;
			else
				objArray = Converter.ConvertFrom(obj);

			if (objArray == null)
				return false;

			if (objArray.Count != _value.Count)
				return false;

			for (int i = 0; i < objArray.Count; i++)
			{
				if (!_value[i].Equals(objArray[i]))
					return false;
			}

			return true;
		}

		public override int GetHashCode()
		{
			return _value.GetHashCode();
		}

		#endregion

		#region Conversion

		public static implicit operator DynamicCompositeType(CompositeType type)
		{
			return (DynamicCompositeType)type._value;
		}

		public static implicit operator CompositeType(DynamicCompositeType type)
		{
			var value =  type.GetValue<List<CassandraType>>();

			return new CompositeType { 
				_value = value,
				ComponentTypeHints = value.Select(t => t.GetType()).ToList()
			};
		}

		public static implicit operator List<CassandraType>(CompositeType type)
		{
			return type._value;
		}

		public static implicit operator CompositeType(CassandraType[] s)
		{
			var value = new List<CassandraType>(s);

			return new CompositeType {
				_value = value,
				ComponentTypeHints = value.Select(t => t.GetType()).ToList()
			};
		}

		public static implicit operator CompositeType(List<CassandraType> s)
		{
			var value = s;

			return new CompositeType {
				_value = value,
				ComponentTypeHints = value.Select(t => t.GetType()).ToList()
			};
		}

		public static implicit operator CompositeType(object[] s)
		{
			return new CompositeType { _value = new List<CassandraType>(s.Select(o => CassandraType.GetTypeFromObject<BytesType>(o))) };
		}

		public static implicit operator CompositeType(List<object> s)
		{
			return new CompositeType { _value = new List<CassandraType>(s.Select(o => CassandraType.GetTypeFromObject<BytesType>(o))) };
		}

		public static implicit operator byte[](CompositeType o) { return ConvertTo<byte[]>(o); }
		public static implicit operator CompositeType(byte[] o) { return ConvertFrom(o); }

		private static T ConvertTo<T>(CompositeType type)
		{
			if (type == null)
				return default(T);

			return type.GetValue<T>();
		}

		private static CompositeType ConvertFrom(object o)
		{
			var type = new CompositeType();
			type.SetValue(o);
			return type;
		}

		#endregion

		#region IList<CassandraType> Members

		int IList<CassandraType>.IndexOf(CassandraType item)
		{
			return _value.IndexOf(item);
		}

		void IList<CassandraType>.Insert(int index, CassandraType item)
		{
			_value.Insert(index, item);
		}

		void IList<CassandraType>.RemoveAt(int index)
		{
			_value.RemoveAt(index);
		}

		public CassandraType this[int index]
		{
			get { return _value[index]; }
			set { _value[index] = value; }
		}

		#endregion

		#region ICollection<CassandraType> Members

		public void Add(CassandraType item)
		{
			_value.Add(item);
		}

		public void Clear()
		{
			_value.Clear();
		}

		public bool Contains(CassandraType item)
		{
			return _value.Contains(item);
		}

		void ICollection<CassandraType>.CopyTo(CassandraType[] array, int arrayIndex)
		{
			_value.CopyTo(array, arrayIndex);
		}

		public int Count
		{
			get { return _value.Count; }
		}

		bool ICollection<CassandraType>.IsReadOnly
		{
			get { return ((ICollection<CassandraType>)_value).IsReadOnly; }
		}

		public bool Remove(CassandraType item)
		{
			return _value.Remove(item);
		}

		#endregion

		#region IEnumerable<CassandraType> Members

		public IEnumerator<CassandraType> GetEnumerator()
		{
			return _value.GetEnumerator();
		}

		#endregion

		#region IEnumerable Members

		System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
		{
			return _value.GetEnumerator();
		}

		#endregion
	}
}
