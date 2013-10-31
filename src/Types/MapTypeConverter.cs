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
                        bytes.Read(valueLengthBytes, 0, 2);
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

            if (destinationType == typeof(byte[]))
            {
                var components = value;

                using (var bytes = new MemoryStream())
                {
                    //write the number of elements
                    var elements = (ushort)components.Count;
                    bytes.Write(BitConverter.GetBytes(elements), 0, 2);

                    foreach (var c in components)
                    {
                        
                        var keyBytes = c.Key.ToBigEndian();

                        //key length
                        var keyLength = (ushort)keyBytes.Length;
                        bytes.Write(BitConverter.GetBytes(keyLength), 0, 2);

                        //key value
                        bytes.Write(keyBytes, 0, keyLength);

                        var valueBytes = c.Value.ToBigEndian();

                        // value length
                        var valueLength = (ushort)valueBytes.Length;
                        bytes.Write(BitConverter.GetBytes(valueLength), 0, 2);

                        // value
                        bytes.Write(valueBytes, 0, valueLength);
                    }

                    return bytes.ToArray();
                }

            }

            if (destinationType == typeof(KeyValuePair<CassandraObject, CassandraObject>[]))
                return value.ToArray();

            if (destinationType == typeof(object[]))
                return value.Cast<object>().ToArray();

            if (destinationType == typeof(List<KeyValuePair<CassandraObject, CassandraObject>>) || destinationType == typeof(List<KeyValuePair<TKey, TValue>>))
                return value.ToList();

            if (destinationType == typeof (Dictionary<TKey, TValue>) ||
                destinationType == typeof (Dictionary<CassandraObject, CassandraObject>))
                return value;

            if (destinationType == typeof(List<object>))
                return value.Cast<object>().ToList();

            return null;
        }

        public override byte[] ToBigEndian(Dictionary<TKey, TValue> value)
        {
            var components = value;

            using (var bytes = new MemoryStream())
            {
                //write the number of elements
                var elements = (ushort)components.Count;
                bytes.Write(ConvertEndian(BitConverter.GetBytes(elements)), 0, 2);

                foreach (var c in components)
                {

                    var keyBytes = c.Key.ToBigEndian();

                    //key length
                    var keyLength = (ushort)keyBytes.Length;
                    bytes.Write(ConvertEndian(BitConverter.GetBytes(keyLength)), 0, 2);

                    //key value
                    bytes.Write(keyBytes, 0, keyLength);

                    var valueBytes = c.Value.ToBigEndian();

                    // value length
                    var valueLength = (ushort)valueBytes.Length;
                    bytes.Write(ConvertEndian(BitConverter.GetBytes(valueLength)), 0, 2);

                    // value
                    bytes.Write(valueBytes, 0, valueLength);
                }

                return bytes.ToArray();
            }
        }

        public override Dictionary<TKey, TValue> FromBigEndian(byte[] value)
        {
            var components = new Dictionary<TKey, TValue>();

            var keyTypeHint = typeof(TKey);
            var valueTypeHint = typeof(TValue);

            using (var bytes = new MemoryStream((byte[])value))
            {
                // number of key / value pairs
                var numEntriesBytes = new byte[2];
                if (bytes.Read(numEntriesBytes, 0, 2) <= 0)
                    return components;

                var nEntries = BitConverter.ToUInt16(ConvertEndian(numEntriesBytes), 0);
                for (var i = 0; i < nEntries; i++)
                {
                    //get the length of the key
                    var keyLengthBytes = new byte[2];
                    bytes.Read(keyLengthBytes, 0, 2);
                    var keyLength = BitConverter.ToUInt16(ConvertEndian(keyLengthBytes), 0);

                    //read the content of the key into a buffer
                    var keyBuffer = new byte[keyLength];
                    bytes.Read(keyBuffer, 0, keyLength);
                    var entryKey = CassandraObject.GetCassandraObjectFromDatabaseByteArray(keyBuffer, keyTypeHint);

                    //get the length of the value
                    var valueLengthBytes = new byte[2];
                    bytes.Read(valueLengthBytes, 0, 2);
                    var valueLength = BitConverter.ToUInt16(ConvertEndian(valueLengthBytes), 0);

                    //read the content of the key into a buffer
                    var valueBuffer = new byte[valueLength];
                    bytes.Read(valueBuffer, 0, valueLength);
                    var entryValue = CassandraObject.GetCassandraObjectFromDatabaseByteArray(valueBuffer, valueTypeHint);

                    components.Add((TKey)entryKey, (TValue)entryValue);
                }
            }

            return components;
        }
    }
}
