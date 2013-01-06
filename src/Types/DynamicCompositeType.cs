using System;
using System.Collections.Generic;
using System.Linq;

namespace FluentCassandra.Types
{
	public class DynamicCompositeType : CassandraObject, IList<CassandraObject>
	{
		private readonly DynamicCompositeTypeConverter Converter;

		public DynamicCompositeType()
			: this(new Dictionary<char, CassandraType> {
				{ 'b', CassandraType.BooleanType },
				{ 't', CassandraType.DateType },
				{ 'm', CassandraType.DecimalType },
				{ 'f', CassandraType.FloatType },
				{ 'd', CassandraType.DoubleType },
				{ 'i', CassandraType.Int32Type },
				{ 'l', CassandraType.LongType },
				{ 'z', CassandraType.IntegerType },
				{ 'a', CassandraType.AsciiType },
				{ 's', CassandraType.UTF8Type },
				{ 'x', CassandraType.BytesType },
				{ 'u', CassandraType.UUIDType },
				{ '1', CassandraType.TimeUUIDType },
				{ '2', CassandraType.LexicalUUIDType }
			}) { }

		public DynamicCompositeType(IDictionary<char, CassandraType> aliases)
		{
			Converter = new DynamicCompositeTypeConverter(aliases);
			_value = new List<CassandraObject>();
		}

		#region Implimentation

		protected override object GetValueInternal(Type type)
		{
			return Converter.ConvertTo(_value, type);
		}

		public override void SetValue(object obj)
		{
			_value = Converter.ConvertFrom(obj);
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

		#endregion

		public override object GetValue() { return _value; }

		private List<CassandraObject> _value;

		#region Equality

		public override bool Equals(object obj)
		{
			List<CassandraObject> objArray;

			if (obj is DynamicCompositeType)
				objArray = ((DynamicCompositeType)obj)._value;
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

		public static implicit operator CompositeType(DynamicCompositeType type)
		{
			return (CompositeType)type._value;
		}

		public static implicit operator DynamicCompositeType(CompositeType type)
		{
			return new DynamicCompositeType { _value = type.GetValue<List<CassandraObject>>() };
		}

		public static implicit operator List<CassandraObject>(DynamicCompositeType type)
		{
			return type._value;
		}

		public static implicit operator DynamicCompositeType(CassandraObject[] s)
		{
			return new DynamicCompositeType { _value = new List<CassandraObject>(s) };
		}

		public static implicit operator DynamicCompositeType(List<CassandraObject> s)
		{
			return new DynamicCompositeType { _value = s };
		}

		public static implicit operator DynamicCompositeType(object[] s)
		{
			return new DynamicCompositeType { _value = new List<CassandraObject>(s.Select(o => CassandraObject.GetCassandraObjectFromObject(o, CassandraType.BytesType))) };
		}

		public static implicit operator DynamicCompositeType(List<object> s)
		{
			return new DynamicCompositeType { _value = new List<CassandraObject>(s.Select(o => CassandraObject.GetCassandraObjectFromObject(o, CassandraType.BytesType))) };
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