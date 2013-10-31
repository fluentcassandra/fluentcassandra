using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace FluentCassandra.Types
{
    public class MapType<TKey, TValue> : CassandraObject, IDictionary<TKey, TValue> where TKey : CassandraObject where TValue : CassandraObject
    {
        private static readonly MapTypeConverter<TKey, TValue> Converter = new MapTypeConverter<TKey, TValue>();

        #region Create

        public MapType() : this(new Dictionary<TKey, TValue>()) { } 

        public MapType(IDictionary<TKey, TValue> objects)
        {
            _value = new Dictionary<TKey, TValue>(objects);
        }

        #endregion

        private Dictionary<TKey, TValue> _value;

        public static CassandraType KeyType
        {
            get { return (CassandraType)typeof(TKey); }
        }

        public static CassandraType ValueType
        {
            get { return (CassandraType)typeof(TValue); }
        }

        #region Implementation

        public override object GetValue()
        {
            return _value;
        }

        public override void SetValue(object obj)
        {
            if (obj != null)
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

        public override bool CanConvertFrom(Type sourceType)
        {
            return Converter.CanConvertFrom(sourceType);
        }

        public override bool CanConvertTo(Type destinationType)
        {
            return Converter.CanConvertTo(destinationType);
        }

        public static implicit operator MapType<TKey, TValue>(List<KeyValuePair<TKey, TValue>> obj) { return new MapType<TKey, TValue>(obj.ToDictionary(k => k.Key, v => v.Value)); }
        public static implicit operator List<KeyValuePair<TKey, TValue>>(MapType<TKey, TValue> type) { return type._value.ToList(); }
        public static implicit operator Dictionary<TKey, TValue>(MapType<TKey, TValue> type){ return type._value; }
        public static implicit operator MapType<TKey, TValue>(Dictionary<TKey, TValue> obj){ return new MapType<TKey, TValue>(obj); }
        public static implicit operator byte[](MapType<TKey, TValue> o) { return ConvertTo<byte[]>(o); }
        public static implicit operator MapType<TKey, TValue>(byte[] o) { return ConvertFrom(o); }

        private static TOut ConvertTo<TOut>(MapType<TKey, TValue> type)
        {
            if (type == null)
                return default(TOut);

            return type.GetValue<TOut>();
        }

        private static MapType<TKey, TValue> ConvertFrom(object o)
        {
            var type = new MapType<TKey, TValue>();
            type.SetValue(o);
            return type;
        }

        public static MapType<TKey, TValue> From<TKeyIn, TValueIn>(IDictionary<TKeyIn, TValueIn> obj)
        {
            //Check to make sure we can convert
            if (KeyType.CreateInstance().CanConvertFrom(typeof(TKeyIn)) && ValueType.CreateInstance().CanConvertFrom(typeof(TValueIn)))
            {
                return new MapType<TKey, TValue>(obj.ToDictionary(
                    k => (TKey)CassandraObject.GetCassandraObjectFromObject(k, KeyType), 
                    v => (TValue)CassandraObject.GetCassandraObjectFromObject(v, ValueType)));
            }

            throw new ArgumentException(string.Format("can't convert IDictionary of type {0},{1} to MapType of type {2},{3}", typeof(TKeyIn), typeof(TValueIn), typeof(TKey), typeof(TValue)));
        }

        public static Dictionary<TKeyOut, TValueOut> To<TKeyOut, TValueOut>(MapType<TKey, TValue> obj)
        {
            //Check to make sure we can convert
            if (KeyType.CreateInstance().CanConvertFrom(typeof(TKeyOut)) && ValueType.CreateInstance().CanConvertFrom(typeof(TValueOut)))
            {
                return obj.ToDictionary(k => k.Key.GetValue<TKeyOut>(), v => v.Value.GetValue<TValueOut>());
            }

            throw new ArgumentException(string.Format("can't convert MapType of type {0},{1} to Dictionary of type {2},{3}", typeof(TKey), typeof(TValue), typeof(TKeyOut), typeof(TValueOut)));
        }

        #endregion

        #region IDictionary<CassandraType,CassandraType> Members

        public void Add(KeyValuePair<TKey, TValue> item)
        {
            Add(item.Key, item.Value);
        }

        public void Clear()
        {
            _value.Clear();
        }

        public bool Contains(KeyValuePair<TKey, TValue> item)
        {
            return _value.Contains(item);
        }



        public bool Remove(KeyValuePair<TKey, TValue> item)
        {
            return Remove(item.Key);
        }


        public bool ContainsKey(TKey key)
        {
            return _value.ContainsKey(key);
        }

        public void Add(TKey key, TValue value)
        {
            _value.Add(key, value);
        }

        public bool Remove(TKey key)
        {
            return _value.Remove(key);
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            return _value.TryGetValue(key, out value);
        }

        public TValue this[TKey key]
        {
            get { return _value[key]; }
            set { _value[key] = value; }
        }

        public ICollection<TKey> Keys { get { return _value.Keys; } }
        public ICollection<TValue> Values { get { return _value.Values; } }

        #endregion

        #region ICollection<KeyValuePair<CassandraType,CassandraType>> Members

        public int Count { get { return _value.Count; } }
        public bool IsReadOnly { get { return ((ICollection<KeyValuePair<TKey, TValue>>)_value).IsReadOnly; } }

        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
        {
            ((ICollection<KeyValuePair<TKey, TValue>>)_value).CopyTo(array, arrayIndex);
        }

        #endregion

        #region IEnumerable<KeyValuePair<CassandraType,CassandraType>> Members

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
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
