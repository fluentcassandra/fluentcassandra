using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace FluentCassandra.Types
{
    public class ListType<T> : CassandraObject, IList<T> where T : CassandraObject
    {
        private static readonly ListTypeConverter<T> Converter = new ListTypeConverter<T>();

        #region Create

        public ListType() : this(new List<T>())
        {
        }

        public ListType(IEnumerable<T> objects)
        {
            _value = new List<T>(objects);
        }

        #endregion

        private List<T> _value;

        public static CassandraType ComponentType {
            get { return (CassandraType)typeof(T); } 
        }

        #region Implementation

        public override object GetValue()
        {
            return _value;
        }

        public override void SetValue(object obj)
        {
            if (obj != null && obj.GetType().GetInterfaces().Contains(typeof (IEnumerable<T>)))
                _value = Converter.ConvertFrom(obj);
        }

        protected override object GetValueInternal(Type type)
        {
            return Converter.ConvertTo(_value, type);
        }

        protected override TypeCode TypeCode
        {
            get { return TypeCode.Object; }
        }

        public override byte[] ToBigEndian()
        {
            return Converter.ToBigEndian(_value);
        }

        public override void SetValueFromBigEndian(byte[] value)
        {
            _value = Converter.FromBigEndian(value);
        }

        #endregion

        #region Equality

        public override bool Equals(object obj)
        {
            List<T> objArray;

            if (obj is List<T>)
                objArray = ((ListType<T>)obj)._value;
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
            unchecked
            {
                int hash = 17;
                foreach (var keyPart in _value)
                {
                    hash = hash * 23 + keyPart.GetHashCode();
                }
                return hash;
            }
        }

        #endregion

        #region Conversion

        public static implicit operator List<T>(ListType<T> type)
        {
            return type._value;
        }

        public static implicit operator ListType<T>(List<T> obj)
        {
            return new ListType<T>(obj);
        }

        public static ListType<T> From<TIn>(List<TIn> obj)
        {
            //Check to make sure we can convert
            if (ComponentType.CreateInstance().CanConvertFrom(typeof(TIn)))
            {
                return new ListType<T>(obj.Select(o => (T) CassandraObject.GetCassandraObjectFromObject(o, typeof (T))));
            }

            throw new ArgumentException(string.Format("can't convert list of type {0} to ListType of type {1}", typeof(TIn), typeof(T)));
        }

        public static List<TOut> To<TOut>(ListType<T> obj)
        {
            //Check to make sure we can convert
            if (ComponentType.CreateInstance().CanConvertTo(typeof(TOut)))
            {
                return new List<TOut>(obj.Select(o => o.GetValue<TOut>()));
            }

            throw new ArgumentException(string.Format("can't convert ListType of type {1} to List of type {0} to ", typeof(T), typeof(TOut)));
        }

        public static implicit operator byte[](ListType<T> o) { return ConvertTo<byte[]>(o); }
        public static implicit operator ListType<T>(byte[] o) { return ConvertFrom(o); }

        public static implicit operator ListType<T>(object[] s)
        {
            return new ListType<T>{ _value = new List<T>(s.Select(o => (T)CassandraObject.GetCassandraObjectFromObject(o, typeof(T)))) };
        }

        public static implicit operator ListType<T>(List<object> s)
        {
            return new ListType<T>{ _value = new List<T>(s.Select(o => (T)CassandraObject.GetCassandraObjectFromObject(o, typeof(T)))) };
        }

        private static TOut ConvertTo<TOut>(ListType<T> type)
        {
            if (type == null)
                return default(TOut);

            return type.GetValue<TOut>();
        }

        private static ListType<T> ConvertFrom(object o)
        {
            var type = new ListType<T>();
            type.SetValue(o);
            return type;
        }

        public override bool CanConvertFrom(Type sourceType)
        {
            return Converter.CanConvertFrom(sourceType);
        }

        public override bool CanConvertTo(Type destinationType)
        {
            return Converter.CanConvertTo(destinationType);
        }

        #endregion

        #region IList<CassandraObject> Members

        public int IndexOf(T item)
        {
            return _value.IndexOf(item);
        }

        public void Insert(int index, T item)
        {
            _value.Insert(index, item);
        }

        public void RemoveAt(int index)
        {
            _value.RemoveAt(index);
        }

        public T this[int index]
        {
            get { return _value[index]; }
            set { _value[index] = value; }
        }

        #endregion

        #region ICollection<CassandraType> Members

        public void Add(T item)
        {
            _value.Add(item);
        }

        public void Clear()
        {
            _value.Clear();
        }

        public bool Contains(T item)
        {
            return _value.Contains(item);
        }

        public void CopyTo(T[] array, int arrayIndex)
        {
            _value.CopyTo(array, arrayIndex);
        }

        public bool Remove(T item)
        {
            return _value.Remove(item);
        }

        public int Count { get { return _value.Count; } }
        public bool IsReadOnly { get { return ((ICollection<CassandraObject>)_value).IsReadOnly; } }

        #endregion

        #region IEnumerable<CassandraType> Members

        public IEnumerator<T> GetEnumerator()
        {
            return _value.GetEnumerator();
        }

        #endregion

        #region IEnumerable Members

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion
    }
}
