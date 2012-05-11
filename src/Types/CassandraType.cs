using System;
using System.Linq;
using System.Numerics;
using System.Collections.Generic;
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

		private static readonly CassandraType _CompositeType = new CassandraType("org.apache.cassandra.db.marshal.CompositeType");
		private static readonly CassandraType _DynamicCompositeType = new CassandraType("org.apache.cassandra.db.marshal.DynamicCompositeType");

		private readonly string _dbType;
		private Type _type;

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
				Parse();

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
            // Pay attention to any ReversedType on the Column Index
		    const string reversedTypeString = "ReversedType(";
            var reversedIndex = _dbType.IndexOf(reversedTypeString, StringComparison.Ordinal);
		    var workingtype = _dbType;
            if (reversedIndex > 0)
            {
                workingtype = workingtype.Substring(reversedIndex + reversedTypeString.Length);
                var lastClosing = workingtype.LastIndexOf(")", StringComparison.Ordinal);
                workingtype = workingtype.Substring(0, lastClosing).TrimEnd('.');
            }

			var compositeStart = workingtype.IndexOf('(');

			// check for composite type
			if (compositeStart == -1) {
				_type = Parse(workingtype);
				return;
			}

			var part1 = workingtype.Substring(0, compositeStart);
			var part2 = workingtype.Substring(compositeStart);

			_type = Parse(part1);

			if (_type == typeof(CompositeType))
			{
				ParseCompositeType(part2);
			}
			else if (_type == typeof(DynamicCompositeType))
			{
				ParseDynamicCompositeType(part2);
			}
			else
			{
				throw new CassandraException("Type '" + _dbType + "' not found.");
			}
		}

		private void ParseCompositeType(string part)
		{
			part = part.Trim('(', ')');
			var parts = part.Split(',');

			_compositeTypes = new List<CassandraType>();
			foreach (var p in parts)
				_compositeTypes.Add(Parse(p));
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
				_dynamicCompositeType.Add(alias, Parse(type));
			}
		}

		private Type Parse(string dbType) 
		{
			Type type;

			switch (dbType.Substring(dbType.LastIndexOf('.') + 1).ToLower())
			{
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
				    default: throw new CassandraException("Type '" + _dbType + "' not found.");
			}

			return type;
		}

		public override string ToString()
		{
			return _dbType;
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

	    public static CassandraType Reversed(CassandraType baseType)
	    {
	        var underlyingType = baseType.DatabaseType;
            // Add the Reversed keyword
	        var lastDot = underlyingType.LastIndexOf('.');
	        underlyingType = underlyingType.Insert(lastDot + 1, "ReversedType(");
	        underlyingType = underlyingType + ")";

	        var result = new CassandraType(underlyingType);
	        result.Parse();

	        return result;
	    }
	}
}
