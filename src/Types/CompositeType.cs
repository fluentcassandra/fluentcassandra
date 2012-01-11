using System;
using System.Collections.Generic;
using System.Linq;

namespace FluentCassandra.Types
{
	public class CompositeType : CassandraType, IList<CassandraType>
	{
		private static readonly CompositeTypeConverter Converter = new CompositeTypeConverter();

		#region Implimentation

		public override object GetValue(Type type)
		{
			return GetValue(_value, type, Converter);
		}

		public override void SetValue(object obj)
		{
			_value = SetValue(obj, Converter);
		}

		internal override byte[] ToBigEndian()
		{
			return Converter.ToBigEndian(_value);
		}

		internal override void SetValueFromBigEndian(byte[] value)
		{
			_value = Converter.FromBigEndian(value);
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

		private List<CassandraType> _value;

		#region Equality

		public override bool Equals(object obj)
		{
			List<CassandraType> objArray;

			if (obj is CompositeType)
				objArray = ((CompositeType)obj)._value;
			else
				objArray = CassandraType.GetValue<List<CassandraType>>(obj, Converter);

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
			return new CompositeType { _value = type.GetValue<List<CassandraType>>() };
		}

		public static implicit operator List<CassandraType>(CompositeType type)
		{
			return type._value;
		}

		public static implicit operator CompositeType(CassandraType[] s)
		{
			return new CompositeType { _value = new List<CassandraType>(s) };
		}

		public static implicit operator CompositeType(List<CassandraType> s)
		{
			return new CompositeType { _value = s };
		}

		public static implicit operator CompositeType(object[] s)
		{
			return new CompositeType { _value = new List<CassandraType>(s.Select(o => CassandraType.GetType<BytesType>(o))) };
		}

		public static implicit operator CompositeType(List<object> s)
		{
			return new CompositeType { _value = new List<CassandraType>(s.Select(o => CassandraType.GetType<BytesType>(o))) };
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
