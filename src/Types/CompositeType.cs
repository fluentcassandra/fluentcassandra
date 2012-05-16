using System;
using System.Collections.Generic;
using System.Linq;

namespace FluentCassandra.Types
{
	public class CompositeType : CassandraObject, IList<CassandraObject>
	{
		private static readonly CompositeTypeConverter Converter = new CompositeTypeConverter();

		#region Create

		public static CompositeType<T1> Create<T1>(T1 t1) 
			where T1 : CassandraObject
		{
			return new CompositeType<T1>(t1);
		}

		public static CompositeType<T1, T2> Create<T1, T2>(T1 t1, T2 t2)
			where T1 : CassandraObject
			where T2 : CassandraObject
		{
			return new CompositeType<T1, T2>(t1, t2);
		}

		public static CompositeType<T1, T2, T3> Create<T1, T2, T3>(T1 t1, T2 t2, T3 t3)
			where T1 : CassandraObject
			where T2 : CassandraObject
			where T3 : CassandraObject
		{
			return new CompositeType<T1, T2, T3>(t1, t2, t3);
		}

		public static CompositeType<T1, T2, T3, T4> Create<T1, T2, T3, T4>(T1 t1, T2 t2, T3 t3, T4 t4)
			where T1 : CassandraObject
			where T2 : CassandraObject
			where T3 : CassandraObject
			where T4 : CassandraObject
		{
			return new CompositeType<T1, T2, T3, T4>(t1, t2, t3, t4);
		}

		public static CompositeType<T1, T2, T3, T4, T5> Create<T1, T2, T3, T4, T5>(T1 t1, T2 t2, T3 t3, T4 t4, T5 t5)
			where T1 : CassandraObject
			where T2 : CassandraObject
			where T3 : CassandraObject
			where T4 : CassandraObject
			where T5 : CassandraObject
		{
			return new CompositeType<T1, T2, T3, T4, T5>(t1, t2, t3, t4, t5);
		}

		public static CompositeType Create(params CassandraObject[] types)
		{
			return new CompositeType { _value = types.ToList() };
		}

		#endregion

		public CompositeType()
		{
			ComponentTypeHints = new List<CassandraType>();
			_value = new List<CassandraObject>();
		}

		public CompositeType(IEnumerable<CassandraType> hints)
		{
			ComponentTypeHints = new List<CassandraType>(hints);
			_value = new List<CassandraObject>();
		}

		#region Implimentation

		protected override object GetValueInternal(Type type)
		{
			return Converter.ConvertTo(_value, type);
		}

		public override void SetValue(object obj)
		{
			if (obj != null && obj.GetType().GetInterfaces().Contains(typeof(IEnumerable<CassandraObject>)))
				ComponentTypeHints = ((IEnumerable<CassandraObject>)obj).Select(t => new CassandraType(t.GetType().Name)).ToList();

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

		public List<CassandraType> ComponentTypeHints { get; set; }

		private List<CassandraObject> _value;

		#region Equality

		public override bool Equals(object obj)
		{
			List<CassandraObject> objArray;

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
			var value =  type.GetValue<List<CassandraObject>>();

			return new CompositeType { 
				_value = value,
				ComponentTypeHints = value.Select(t => new CassandraType(t.GetType().Name)).ToList()
			};
		}

		public static implicit operator List<CassandraObject>(CompositeType type)
		{
			return type._value;
		}

		public static implicit operator CompositeType(CassandraObject[] s)
		{
			var value = new List<CassandraObject>(s);

			return new CompositeType {
				_value = value,
				ComponentTypeHints = value.Select(t => new CassandraType(t.GetType().Name)).ToList()
			};
		}

		public static implicit operator CompositeType(List<CassandraObject> s)
		{
			var value = s;

			return new CompositeType {
				_value = value,
				ComponentTypeHints = value.Select(t => new CassandraType(t.GetType().Name)).ToList()
			};
		}

		public static implicit operator CompositeType(object[] s)
		{
			return new CompositeType { _value = new List<CassandraObject>(s.Select(o => CassandraObject.GetCassandraObjectFromObject(o, CassandraType.BytesType))) };
		}

		public static implicit operator CompositeType(List<object> s)
		{
			return new CompositeType { _value = new List<CassandraObject>(s.Select(o => CassandraObject.GetCassandraObjectFromObject(o, CassandraType.BytesType))) };
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

		int IList<CassandraObject>.IndexOf(CassandraObject item)
		{
			return _value.IndexOf(item);
		}

		void IList<CassandraObject>.Insert(int index, CassandraObject item)
		{
			_value.Insert(index, item);
		}

		void IList<CassandraObject>.RemoveAt(int index)
		{
			_value.RemoveAt(index);
		}

		public CassandraObject this[int index]
		{
			get { return _value[index]; }
			set { _value[index] = value; }
		}

		#endregion

		#region ICollection<CassandraType> Members

		public void Add(CassandraObject item)
		{
			_value.Add(item);
		}

		public void Clear()
		{
			_value.Clear();
		}

		public bool Contains(CassandraObject item)
		{
			return _value.Contains(item);
		}

		void ICollection<CassandraObject>.CopyTo(CassandraObject[] array, int arrayIndex)
		{
			_value.CopyTo(array, arrayIndex);
		}

		public int Count
		{
			get { return _value.Count; }
		}

		bool ICollection<CassandraObject>.IsReadOnly
		{
			get { return ((ICollection<CassandraObject>)_value).IsReadOnly; }
		}

		public bool Remove(CassandraObject item)
		{
			return _value.Remove(item);
		}

		#endregion

		#region IEnumerable<CassandraType> Members

		public IEnumerator<CassandraObject> GetEnumerator()
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
