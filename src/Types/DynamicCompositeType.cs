using System;
using System.Collections.Generic;
using System.Linq;

namespace FluentCassandra.Types
{
	public class DynamicCompositeType : CassandraType, IList<CassandraType>
	{
		private readonly DynamicCompositeTypeConverter Converter;

		public DynamicCompositeType()
			: this(new Dictionary<char, Type> {
				{ 'a', typeof(AsciiType) },
				{ 'b', typeof(BytesType) },
				{ 'i', typeof(IntegerType) },
				{ 'x', typeof(LexicalUUIDType) },
				{ 'l', typeof(LongType) },
				{ 't', typeof(TimeUUIDType) },
				{ 's', typeof(UTF8Type) },
				{ 'u', typeof(UUIDType) }
			}) { }

		public DynamicCompositeType(IDictionary<char, Type> aliases)
		{
			Converter = new DynamicCompositeTypeConverter(aliases);
		}

		#region Implimentation

		public override object GetValue(Type type)
		{
			return GetValue(_value, type, Converter);
		}

		public override void SetValue(object obj)
		{
			_value = SetValue(obj, Converter);
		}

		public override byte[] ToBigEndian()
		{
			return Converter.ToBigEndian(_value);
		}

		public override void SetValueFromBigEndian(byte[] value)
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

		internal override object GetRawValue() { return _value; }

		private List<CassandraType> _value;

		#region Equality

		public override bool Equals(object obj)
		{
			List<CassandraType> objArray;

			if (obj is DynamicCompositeType)
				objArray = ((DynamicCompositeType)obj)._value;
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

		public static implicit operator CompositeType(DynamicCompositeType type)
		{
			return (CompositeType)type._value;
		}

		public static implicit operator DynamicCompositeType(CompositeType type)
		{
			return new DynamicCompositeType { _value = type.GetValue<List<CassandraType>>() };
		}

		public static implicit operator List<CassandraType>(DynamicCompositeType type)
		{
			return type._value;
		}

		public static implicit operator DynamicCompositeType(CassandraType[] s)
		{
			return new DynamicCompositeType { _value = new List<CassandraType>(s) };
		}

		public static implicit operator DynamicCompositeType(List<CassandraType> s)
		{
			return new DynamicCompositeType { _value = s };
		}

		public static implicit operator DynamicCompositeType(object[] s)
		{
			return new DynamicCompositeType { _value = new List<CassandraType>(s.Select(o => CassandraType.GetTypeFromObject<BytesType>(o))) };
		}

		public static implicit operator DynamicCompositeType(List<object> s)
		{
			return new DynamicCompositeType { _value = new List<CassandraType>(s.Select(o => CassandraType.GetTypeFromObject<BytesType>(o))) };
		}

		public static implicit operator byte[](DynamicCompositeType o) { return ConvertTo<byte[]>(o); }
		public static implicit operator DynamicCompositeType(byte[] o) { return ConvertFrom(o); }

		private static T ConvertTo<T>(DynamicCompositeType type)
		{
			if (type == null)
				return default(T);

			return type.GetValue<T>();
		}

		private static DynamicCompositeType ConvertFrom(object o)
		{
			var type = new DynamicCompositeType();
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