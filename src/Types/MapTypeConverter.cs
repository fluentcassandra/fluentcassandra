using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace FluentCassandra.Types
{
    internal class MapTypeConverter<TKey, TValue> : CassandraObjectConverter<Dictionary<TKey, TValue>>
        where TKey : CassandraObject
        where TValue : CassandraObject
    {
        protected virtual string CollectionStringBegin { get { return "{"; } }

        protected virtual string CollectionStringEnd { get { return "}"; } }

        protected virtual string KeyValueSeparator{ get { return ":"; } }

        public override bool CanConvertFrom(Type sourceType)
        {
            return sourceType == typeof(byte[]) ||
                 sourceType.GetInterfaces().Contains(typeof(IDictionary<TKey, TValue>)) ||
                 sourceType.GetInterfaces().Contains(typeof(IEnumerable<KeyValuePair<TKey, TValue>>)) ||
                 sourceType.GetInterfaces().Contains(typeof(IEnumerable<object>));
        }

        public override bool CanConvertTo(Type destinationType)
        {
            return destinationType == typeof(byte[]) ||
                destinationType == typeof(Dictionary<TKey, TValue>) ||
                destinationType == typeof(Dictionary<CassandraObject, CassandraObject>) ||
                destinationType == typeof(List<KeyValuePair<TKey, TValue>>) ||
                destinationType == typeof(List<KeyValuePair<CassandraObject, CassandraObject>>) ||
                destinationType == typeof(KeyValuePair<CassandraObject, CassandraObject>[]) ||
                destinationType == typeof(List<object>) ||
                destinationType == typeof(object[]) ||
                destinationType == typeof(string);
        }

        public override Dictionary<TKey, TValue> ConvertFromInternal(object value)
        {
            if (value is byte[])
            {
                var components = new Dictionary<TKey, TValue>();

                var keyTypeHint = typeof(TKey);
                var valueTypeHint = typeof (TValue);

                using (var bytes = new MemoryStream((byte[])value))
                {
                    // number of key / value pairs
                    var numEntriesBytes = new byte[2];
                    if (bytes.Read(numEntriesBytes, 0, 2) <= 0)
                        return components;

                    var nEntries = BitConverter.ToUInt16(numEntriesBytes, 0);
                    for (var i = 0; i < nEntries; i++)
                    {
                        //get the length of the key
                        var keyLengthBytes = new byte[2];
                        bytes.Read(keyLengthBytes, 0, 2);
                        var keyLength = BitConverter.ToUInt16(keyLengthBytes, 0);

                        //read the content of the key into a buffer
                        var keyBuffer = new byte[keyLength];
                        bytes.Read(keyBuffer, 0, keyLength);
                        var entryKey = CassandraObject.GetCassandraObjectFromDatabaseByteArray(keyBuffer, keyTypeHint);

                        //get the length of the value
                        var valueLengthBytes = new byte[2];
                        bytes.Read(keyLengthBytes, 0, 2);
                        var valueLength = BitConverter.ToUInt16(valueLengthBytes, 0);

                        //read the content of the key into a buffer
                        var valueBuffer = new byte[valueLength];
                        bytes.Read(valueBuffer, 0, valueLength);
                        var entryValue = CassandraObject.GetCassandraObjectFromDatabaseByteArray(valueBuffer, valueTypeHint);

                        components.Add((TKey)entryKey, (TValue)entryValue);
                    }
                }

                return components;
            }

            if (value.GetType().GetInterfaces().Contains(typeof(IEnumerable<object>)))
                return new Dictionary<TKey, TValue>(((IEnumerable<object>)value).Cast<KeyValuePair<TKey, TValue>>().ToDictionary(k => k.Key, v => v.Value));

            if (value.GetType().GetInterfaces().Contains(typeof(IEnumerable<KeyValuePair<TKey, TValue>>)))
                return new Dictionary<TKey, TValue>(((IEnumerable<KeyValuePair<TKey, TValue>>)value).ToDictionary(k => k.Key, v => v.Value));

            return null;
        }

        public override object ConvertToInternal(Dictionary<TKey, TValue> value, Type destinationType)
        {
            if (destinationType == typeof(string))
                return CollectionStringBegin + String.Join(",", value.Select(x => string.Format("{0}{1}{2}", x.Key, KeyValueSeparator, x.Value))) + CollectionStringEnd; //should format the map into a JSON-esque object
        }
    }
}
