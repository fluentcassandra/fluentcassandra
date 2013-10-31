using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Numerics;
using System.Text;

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
		public static readonly CassandraType CounterColumnType = new CassandraType("org.apache.cassandra.db.marshal.CounterColumnType");
		public static readonly CassandraType EmptyType = new CassandraType("org.apache.cassandra.db.marshal.EmptyType");
		public static readonly CassandraType InetAddressType = new CassandraType("org.apache.cassandra.db.marshal.InetAddressType");


        private static readonly CassandraType _ListType = new CassandraType("org.apache.cassandra.db.marshal.ListType");
        private static readonly CassandraType _SetType = new CassandraType("org.apache.cassandra.db.marshal.SetType");
		private static readonly CassandraType _CompositeType = new CassandraType("org.apache.cassandra.db.marshal.CompositeType");
		private static readonly CassandraType _DynamicCompositeType = new CassandraType("org.apache.cassandra.db.marshal.DynamicCompositeType");

		private const string _ReversedType = "org.apache.cassandra.db.marshal.ReversedType";

		private readonly string _dbType;
		private Type _type;
		private bool? _typeReversed;

		private IList<CassandraType> _compositeTypes;
		private IDictionary<char, CassandraType> _dynamicCompositeType;

		public CassandraType(string type)
		{
			if (string.IsNullOrEmpty(type))
				throw new ArgumentNullException("type");

			_dbType = type;
		}

		public CassandraObject CreateInstance()
		{
			if (_type == null)
				Init();

			CassandraObject obj;

			if (_type == typeof(CompositeType))
				obj = new CompositeType(_compositeTypes);
			else if (_type == typeof(DynamicCompositeType))
				obj = new DynamicCompositeType(_dynamicCompositeType);
			else
				obj = Activator.CreateInstance(_type) as CassandraObject;

			if (obj == null)
				return null;

			return obj;
		}

		public string DatabaseType { get { return _dbType; } }

		public bool Reversed
		{
			get
			{
				if (_typeReversed == null)
					Init();

				return _typeReversed.Value;
			}
		}

		public Type FluentType
		{
			get
			{
				if (_type == null)
					Init();

				return _type;
			}
		}

		private void Init()
		{
			_typeReversed = false;
			Parse(_dbType);
		}

		private void Parse(string dbType) 
		{
			int compositeStart = dbType.IndexOf('(');

			// check for composite type
			if (compositeStart == -1) {
				_type = GetSystemType(dbType);
				return;
			}

			var part1 = dbType.Substring(0, compositeStart);
			var part2 = dbType.Substring(compositeStart);

			_type = GetSystemType(part1);

			if (_type == typeof(CompositeType))
				ParseCompositeType(part2);
			else if (_type == typeof(DynamicCompositeType))
				ParseDynamicCompositeType(part2);
			else if (_type == typeof(ReversedType))
				ParseReversedType(part2);
            else if (_type == typeof (ListType<>))
                ParseListType(part2);
            else if (_type == typeof(SetType<>))
                ParseSetType(part2);
			else
				throw new CassandraException("Type '" + dbType + "' not found.");
		}

        private void ParseSetType(string part)
        {
            //construct the generic SetType (has an indentical implmentation to ListType)
            ParseListType(part);
        }

	    private void ParseListType(string part)
	    {
            //construct the generic ListType
            part = part.Trim('(', ')');
	        var listType = GetSystemType(part);
	        _type = _type.MakeGenericType(listType);
	    }

	    private void ParseReversedType(string part)
		{
			part = part.Trim('(', ')');
			_typeReversed = true;
			Parse(part);
		}

		private void ParseCompositeType(string part)
		{
			part = part.Trim('(', ')');
			var parts = part.Split(',');

			_compositeTypes = new List<CassandraType>();
			foreach (var p in parts)
				_compositeTypes.Add(GetSystemType(p));
		}

		private void ParseDynamicCompositeType(string part)
		{
			part = part.Trim('(', ')');
			var parts = part.Split(',');

			_dynamicCompositeType = new Dictionary<char, CassandraType>();
			foreach (var p in parts)
			{
				char alias = p[0];

				if (alias < 33 || alias > 127)
					throw new CassandraException("An alias should be a single character in [0..9a..bA..B-+._&]");

				if (p[1] != '=' || p[2] != '>')
					throw new CassandraException("Expecting operator '=>' after the alias");

				string type = p.Substring(3);
				_dynamicCompositeType.Add(alias, GetSystemType(type));
			}
		}

		public override string ToString()
		{
			return _dbType;
		}

		public static CassandraType CompositeType(params CassandraType[] hints)
		{
			return CompositeType((IEnumerable<CassandraType>)hints);
		}

		public static CassandraType CompositeType(IEnumerable<CassandraType> hints)
		{
			var sb = new StringBuilder();
			sb.Append(_CompositeType);
			sb.Append("(");
			sb.Append(String.Join(",", hints));
			sb.Append(")");

			return new CassandraType(sb.ToString());
		}

		public static CassandraType DynamicCompositeType(IDictionary<char, CassandraType> aliases)
		{
			var sb = new StringBuilder();
			sb.Append(_DynamicCompositeType);
			sb.Append("(");
			sb.Append(String.Join(",", aliases.Select(x => x.Key + "=>" + x.Value)));
			sb.Append(")");

			return new CassandraType(sb.ToString());
		}

		public static CassandraType ReversedType(CassandraType baseType)
		{
			var sb = new StringBuilder();
			sb.Append(_ReversedType);
			sb.Append("(");
			sb.Append(baseType.DatabaseType);
			sb.Append(")");

			return new CassandraType(sb.ToString());
		}

		public static CassandraType GetCassandraType(CassandraObject obj)
		{
			var typeName = obj.GetType().Name;
			var cassandraType = (CassandraType)null;

			switch (typeName.ToLower())
			{
				case "asciitype": cassandraType = AsciiType; break;
				case "booleantype": cassandraType = BooleanType; break;
				case "bytestype": cassandraType = BytesType; break;
				case "datetype": cassandraType = DateType; break;
				case "decimaltype": cassandraType = DecimalType; break;
				case "doubletype": cassandraType = DoubleType; break;
				case "floattype": cassandraType = FloatType; break;
				case "int32type": cassandraType = Int32Type; break;
				case "integertype": cassandraType = IntegerType; break;
				case "lexicaluuidtype": cassandraType = LexicalUUIDType; break;
				case "longtype": cassandraType = LongType; break;
				case "timeuuidtype": cassandraType = TimeUUIDType; break;
				case "utf8type": cassandraType = UTF8Type; break;
				case "uuidtype": cassandraType = UUIDType; break;
				case "emptytype": cassandraType = EmptyType; break;
				case "inetaddresstype": cassandraType = InetAddressType; break;
				// these need work
				//case "compositetype": cassandraType = CompositeType; break;
				//case "dynamiccompositetype": cassandraType = DynamicCompositeType; break;
				//case "countercolumntype": cassandraType = CounterColumnType; break;
				//case "reversedtype": cassandraType = ReversedType; break;
				default: throw new CassandraException("Type '" + typeName + "' not found.");
			}

			return cassandraType;
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
					destinationType = Int32Type;
					break;

				default:
					if (sourceType == typeof(IPAddress))
						destinationType = InetAddressType;

					if (sourceType == typeof(DateTimeOffset))
						destinationType = DateType;

					if (sourceType == typeof(BigInteger))
						destinationType = IntegerType;

					if (sourceType == typeof(BigDecimal))
						destinationType = DecimalType;

					if (sourceType == typeof(Guid))
						destinationType = UUIDType;

					if (sourceType == typeof(char[]))
						destinationType = UTF8Type;

					if (sourceType == typeof(byte[]))
						destinationType = BytesType;

					if (destinationType == null)
						destinationType = BytesType;
					break;
			}

			return destinationType;
		}

		public static CassandraType GetCassandraType(string type)
		{
			return new CassandraType(type);
		}

		public static Type GetSystemType(CassandraType baseType)
		{
			return GetSystemType(baseType.DatabaseType);
		}

		public static Type GetSystemType(string dbType)
		{
			Type type;

			switch (dbType.Substring(dbType.LastIndexOf('.') + 1).ToLower()) {
				case "asciitype": type = typeof(AsciiType); break;
				case "booleantype": type = typeof(BooleanType); break;
				case "bytestype": type = typeof(BytesType); break;
				case "datetype": type = typeof(DateType); break;
				case "decimaltype": type = typeof(DecimalType); break;
				case "doubletype": type = typeof(DoubleType); break;
				case "floattype": type = typeof(FloatType); break;
				case "int32type": type = typeof(Int32Type); break;
				case "integertype": type = typeof(IntegerType); break;
				case "lexicaluuidtype": type = typeof(LexicalUUIDType); break;
				case "longtype": type = typeof(LongType); break;
				case "timeuuidtype": type = typeof(TimeUUIDType); break;
				case "utf8type": type = typeof(UTF8Type); break;
				case "uuidtype": type = typeof(UUIDType); break;
				case "compositetype": type = typeof(CompositeType); break;
				case "dynamiccompositetype": type = typeof(DynamicCompositeType); break;
				case "countercolumntype": type = typeof(CounterColumnType); break;
				case "reversedtype": type = typeof(ReversedType); break;
				case "emptytype": type = typeof(EmptyType); break;
				case "inetaddresstype": type = typeof(InetAddressType); break;
                case "listtype": type = typeof (ListType<>); break;
                case "settype": type = typeof (SetType<>); break;
				default: throw new CassandraException("Type '" + dbType + "' not found.");
			}

			return type;
		}

		public static implicit operator CassandraType(Type type)
		{
			return GetCassandraType(type);
		}
	}
}
