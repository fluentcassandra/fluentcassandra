using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace FluentCassandra.Types
{
    internal class ListTypeConverter<T> : CassandraObjectConverter<List<T>> where T : CassandraObject
    {
        protected virtual string CollectionStringBegin
        {
            get { return "["; }
        }

        protected virtual string CollectionStringEnd
        {
            get { return "]"; }
        }

        public override bool CanConvertFrom(Type sourceType)
        {
            return sourceType == typeof(byte[]) ||
                 sourceType.GetInterfaces().Contains(typeof(IEnumerable<T>)) ||
                 sourceType.GetInterfaces().Contains(typeof(IEnumerable<object>));
        }

        public override bool CanConvertTo(Type destinationType)
        {
            return destinationType == typeof(byte[]) ||
                destinationType == typeof(List<T>) ||
                destinationType == typeof(List<CassandraObject>) ||
                destinationType == typeof(CassandraObject[]) ||
                destinationType == typeof(List<object>) ||
                destinationType == typeof(object[]) ||
                destinationType == typeof(string);
        }

        public override List<T> ConvertFromInternal(object value)
        {
            if (value is byte[])
            {
                var components = new List<T>();

                var typeHint = typeof(T);

                using (var bytes = new MemoryStream((byte[])value))
                {
                    // number of elements
                    var numElementsBytes = new byte[2];
                    if (bytes.Read(numElementsBytes, 0, 2) <= 0)
                        return components;

                    var nElements = BitConverter.ToUInt16(numElementsBytes, 0);
                    for (var i = 0; i < nElements; i++)
                    {
                        //get the length of this element
                        var elementLengthBytes = new byte[2];
                        bytes.Read(elementLengthBytes, 0, 2);
                        var elementLength = BitConverter.ToUInt16(elementLengthBytes, 0);

                        //read the content of the element into a buffer
                        var buffer = new byte[elementLength];
                        bytes.Read(buffer, 0, elementLength);
                        var component = CassandraObject.GetCassandraObjectFromDatabaseByteArray(buffer, typeHint);
                        components.Add((T)component);
                    }
                }

                return components;
            }

            if (value.GetType().GetInterfaces().Contains(typeof(IEnumerable<object>)))
                return new List<T>(((IEnumerable<object>)value).Cast<T>());

            if (value.GetType().GetInterfaces().Contains(typeof(IEnumerable<T>)))
                return new List<T>((IEnumerable<T>)value);

            return null;
        }

        public override object ConvertToInternal(List<T> value, Type destinationType)
        {
            if (destinationType == typeof(string))
                return CollectionStringBegin + String.Join(",", value) + CollectionStringEnd; //should format the list into a JSON-esque object

            if (destinationType == typeof(byte[]))
            {
                var components = value;

                using (var bytes = new MemoryStream())
                {
                    //write the number of lengths
                    var elements = (ushort)components.Count;
                    bytes.Write(BitConverter.GetBytes(elements), 0, 2);

                    foreach (var c in components)
                    {
                        var b = c.ToBigEndian();
                        var length = (ushort)b.Length;

                        // value length
                        bytes.Write(BitConverter.GetBytes(length), 0, 2);

                        // value
                        bytes.Write(b, 0, length);
                    }

                    return bytes.ToArray();
                }

            }

            if (destinationType == typeof(CassandraObject[]))
                return value.ToArray();

            if (destinationType == typeof(object[]))
                return value.Cast<object>().ToArray();

            if (destinationType == typeof(List<T>))
                return value;

            if (destinationType == typeof(List<CassandraObject>))
                return value;

            if (destinationType == typeof(List<object>))
                return value.Cast<object>().ToList();

            return null;
        }

        public override byte[] ToBigEndian(List<T> value)
        {
            var components = value;

            using (var bytes = new MemoryStream())
            {
                //write the number of lengths
                var elements = (ushort)components.Count;
                bytes.Write(ConvertEndian(BitConverter.GetBytes(elements)), 0, 2);

                foreach (var c in components)
                {
                    var b = c.ToBigEndian();
                    var length = (ushort)b.Length;

                    // value length
                    bytes.Write(ConvertEndian(BitConverter.GetBytes(length)), 0, 2);

                    // value
                    bytes.Write(b, 0, length);
                }

                return bytes.ToArray();
            }
        }

        public override List<T> FromBigEndian(byte[] value)
        {
            var components = new List<T>();

            var typeHint = typeof(T);

            using (var bytes = new MemoryStream(value))
            {
                // number of elements
                var numElementsBytes = new byte[2];
                if (bytes.Read(numElementsBytes, 0, 2) <= 0)
                    return components;

                var nElements = BitConverter.ToUInt16(ConvertEndian(numElementsBytes), 0);
                for (var i = 0; i < nElements; i++)
                {
                    //get the length of this element
                    var elementLengthBytes = new byte[2];
                    bytes.Read(elementLengthBytes, 0, 2);
                    var elementLength = BitConverter.ToUInt16(ConvertEndian(elementLengthBytes), 0);

                    //read the content of the element into a buffer
                    var buffer = new byte[elementLength];
                    bytes.Read(buffer, 0, elementLength);
                    var component = CassandraObject.GetCassandraObjectFromDatabaseByteArray(buffer, typeHint);
                    components.Add((T)component);
                }
            }

            return components;
        }
    }
}
