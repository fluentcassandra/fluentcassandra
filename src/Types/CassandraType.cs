﻿using System;
using System.Linq;
using System.Numerics;

namespace FluentCassandra.Types
{
	public sealed class CassandraType
	{
		public static readonly CassandraType AsciiType = new CassandraType("org.apache.cassandra.db.marshal.AsciiType");
		public static readonly CassandraType BooleanType = new CassandraType("org.apache.cassandra.db.marshal.BooleanType");
		public static readonly CassandraType BytesType = new CassandraType("org.apache.cassandra.db.marshal.BytesType");
		public static readonly CassandraType DateType = new CassandraType("org.apache.cassandra.db.marshal.DateType");
		public static readonly CassandraType DecimalType = new CassandraType("org.apache.cassandra.db.marshal.DecimalType");
		public static readonly CassandraType DoubleType = new CassandraType("org.apache.cassandra.db.marshal.DoubleType");
		public static readonly CassandraType FloatType = new CassandraType("org.apache.cassandra.db.marshal.FloatType");
		public static readonly CassandraType Int32Type = new CassandraType("org.apache.cassandra.db.marshal.Int32Type");
		public static readonly CassandraType IntegerType = new CassandraType("org.apache.cassandra.db.marshal.IntegerType");
		public static readonly CassandraType LexicalUUIDType = new CassandraType("org.apache.cassandra.db.marshal.LexicalUUIDType");
		public static readonly CassandraType LongType = new CassandraType("org.apache.cassandra.db.marshal.LongType");
		public static readonly CassandraType TimeUUIDType = new CassandraType("org.apache.cassandra.db.marshal.TimeUUIDType");
		public static readonly CassandraType UTF8Type = new CassandraType("org.apache.cassandra.db.marshal.UTF8Type");
		public static readonly CassandraType UUIDType = new CassandraType("org.apache.cassandra.db.marshal.UUIDType");
		public static readonly CassandraType ColumnCounterType = new CassandraType("org.apache.cassandra.db.marshal.ColumnCounterType");

        private static readonly Type[] _fluentCompositeTypes = new Type[] { typeof(CompositeType<>), typeof(CompositeType<,>), typeof(CompositeType<,,>), typeof(CompositeType<,,,>), typeof(CompositeType<,,,,>) };


		private readonly string _dbType;
		private Type _type;

		public CassandraType(string type)
		{
			if (type == null || type.Length == 0)
				throw new ArgumentNullException("type");

			_dbType = type;
		}

		public CassandraObject CreateInstance()
		{
			if (_type == null)
				Parse();

			var type = Activator.CreateInstance(_type) as CassandraObject;

			if (type == null)
				return null;

			return type;
		}

		public string DatabaseType { get { return _dbType; } }

		public Type FluentType
		{
			get
			{
				if (_type == null)
					Parse();

				return _type;
			}
		}

		private void Parse() 
		{
            var baseType = _dbType;
            if (baseType.EndsWith(")", StringComparison.Ordinal))
            {
                baseType = baseType.Substring(0, baseType.IndexOf("("));
            }
            baseType = baseType.Substring(baseType.LastIndexOf('.') + 1).ToLower();

            switch (baseType)
			{
				case "asciitype": _type = typeof(AsciiType); break;
				case "booleantype": _type = typeof(BooleanType); break;
				case "bytestype": _type = typeof(BytesType); break;
				case "datetype": _type = typeof(DateType); break;
				case "decimaltype": _type = typeof(DecimalType); break;
				case "doubletype": _type = typeof(DoubleType); break;
				case "floattype": _type = typeof(FloatType); break;
				case "int32type": _type = typeof(Int32Type); break;
				case "integertype": _type = typeof(IntegerType); break;
				case "lexicaluuidtype": _type = typeof(LexicalUUIDType); break;
				case "longtype": _type = typeof(LongType); break;
				case "timeuuidtype": _type = typeof(TimeUUIDType); break;
				case "utf8type": _type = typeof(UTF8Type); break;
				case "uuidtype": _type = typeof(UUIDType); break;
                case "compositetype": _type = ParseCompositeType(_dbType); break;
				default: throw new CassandraException("Type '" + _dbType + "' not found.");
			}
		}

        private static Type ParseCompositeType(string dbType)
        {
            var typesStart = dbType.IndexOf('(') + 1;
            var typesEnd = dbType.LastIndexOf(')');
            var fluentTypes = dbType.Substring(typesStart, typesEnd - typesStart).Split(',').Select(type => new CassandraType(type).FluentType).ToArray();

            var matchingCompositeType = _fluentCompositeTypes.Where(t => t.GetGenericArguments().Count() == fluentTypes.Count()).FirstOrDefault();
            if (matchingCompositeType == null)
            {
                throw new CassandraException("Type '" + dbType + "' has unsupported number of composite keys " + fluentTypes.Length + ".");
            }
            return matchingCompositeType.MakeGenericType(fluentTypes);
        }

		public override string ToString()
		{
			return FluentType.Name;
		}

		public static CassandraType GetCassandraType(Type sourceType)
		{
			if (sourceType.BaseType == typeof(CassandraObject))
				return new CassandraType(sourceType.Name) { _type = sourceType };

			var destinationType = (CassandraType)null;

			switch (Type.GetTypeCode(sourceType))
			{
				case TypeCode.DateTime:
					destinationType = DateType;
					break;

				case TypeCode.Boolean:
					destinationType = BooleanType;
					break;

				case TypeCode.Double:
					destinationType = DoubleType;
					break;

				case TypeCode.Single:
					destinationType = FloatType;
					break;

				case TypeCode.Int64:
				case TypeCode.UInt64:
					destinationType = LongType;
					break;

				case TypeCode.Int16:
				case TypeCode.Int32:
				case TypeCode.UInt16:
				case TypeCode.UInt32:
					destinationType = Int32Type;
					break;

				case TypeCode.Decimal:
					destinationType = DecimalType;
					break;

				case TypeCode.Char:
				case TypeCode.String:
					destinationType = UTF8Type;
					break;

				case TypeCode.Byte:
				case TypeCode.SByte:
					destinationType = BytesType;
					break;

				default:
					if (sourceType == typeof(DateTimeOffset))
						destinationType = DateType;

					if (sourceType == typeof(BigInteger))
						destinationType = IntegerType;

					if (sourceType == typeof(Guid))
						destinationType = UUIDType;

					if (sourceType == typeof(char[]))
						destinationType = UTF8Type;

					if (destinationType == null)
						destinationType = BytesType;
					break;
			}

			return destinationType;
		}

		public static implicit operator CassandraType(Type type)
		{
			return GetCassandraType(type);
		}
	}
}
