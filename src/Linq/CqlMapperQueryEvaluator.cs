using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace FluentCassandra.Linq
{
	internal class CqlMapperQueryEvaluator
	{
		private DynamicParameters _parameters;
		private int _parameterCount;
		private IList<string> _fields;
		private IList<string> _groupBy;
		private IList<string> _orderBy;

		private string _table;

		internal CqlMapperQueryEvaluator()
		{
			_parameters = new DynamicParameters();
			_parameterCount = 0;

			_fields = new List<string>();
			_orderBy = new List<string>();
			_groupBy = new List<string>();
		}

		public string Query
		{
			get
			{
				var select = Fields;
				var from = _table;
				var where = WhereCriteria;
				var groupBy = GroupBy;
				var having = HavingCriteria;
				var orderBy = OrderBy;

				if (select == "*" && groupBy != "")
					select = groupBy + ", COUNT(*)";

				var query = String.Format("SELECT {0} \nFROM {1}", select, from);

				if (!String.IsNullOrWhiteSpace(where))
					query += " \nWHERE " + where;

				if (!String.IsNullOrWhiteSpace(groupBy))
					query += " \nGROUP BY " + groupBy;

				if (!String.IsNullOrWhiteSpace(having))
					query += " \nHAVING " + having;

				if (!String.IsNullOrWhiteSpace(orderBy))
					query += " \nORDER BY " + orderBy;

				return query;
			}
		}

		public DynamicParameters Parameters
		{
			get
			{
				return _parameters;
			}
		}

		private string Fields
		{
			get
			{
				if (_fields.Count == 0)
					return "*";

				return String.Join(", ", _fields.ToArray());
			}
		}

		private string OrderBy
		{
			get
			{
				return String.Join(", ", _orderBy.ToArray());
			}
		}

		private string GroupBy
		{
			get
			{
				return String.Join(", ", _groupBy.ToArray());
			}
		}

		public bool AfterGroupBy
		{
			get
			{
				return _groupBy.Count > 0;
			}
		}

		private string WhereCriteria { get; set; }

		private string HavingCriteria { get; set; }

		private void AddTable(CqlMapperQueryProvider provider)
		{
			_table = provider.Table;
		}

		private void AddGroupBy(Expression exp)
		{
			Evaluate(exp, AddGroupBy);
		}

		private void AddGroupBy(string field)
		{
			_groupBy.Add(field);
		}

		private void AddOrderBy(string field, bool asc)
		{
			_orderBy.Add(field + (asc ? " ASC" : " DESC"));
		}

		private void AddField(Expression exp)
		{
			if (exp.NodeType == ExpressionType.MemberAccess
				&& AfterGroupBy
				&& GetPropertyName(exp) == "Key")
			{
				foreach (var groupBy in _groupBy)
					AddField(groupBy);
			}
			else
			{
				Evaluate(exp, AddField);
			}
		}

		private void AddField(string field)
		{
			_fields.Add(field);
		}

		private void AddCriteria(Expression exp)
		{
			if (AfterGroupBy)
			{
				string newCriteria = VisitExpression(exp);

				if (!String.IsNullOrEmpty(HavingCriteria))
					HavingCriteria = "(" + HavingCriteria + " AND " + newCriteria + ")";
				else
					HavingCriteria = newCriteria;
			}
			else
			{
				string newCriteria = VisitExpression(exp);

				if (!String.IsNullOrEmpty(WhereCriteria))
					WhereCriteria = "(" + WhereCriteria + " AND " + newCriteria + ")";
				else
					WhereCriteria = newCriteria;
			}
		}

		#region Expression Helpers

		private static Expression SimplifyExpression(Expression exp)
		{
			switch (exp.NodeType)
			{
				case ExpressionType.Convert:
				case ExpressionType.Quote:
					return SimplifyExpression(((UnaryExpression)exp).Operand);

				case ExpressionType.Lambda:
					return SimplifyExpression(((LambdaExpression)exp).Body);

				default:
					return exp;
			}
		}

		private static string GetPropertyName(Expression exp)
		{
			exp = SimplifyExpression(exp);

			if (exp.NodeType != ExpressionType.MemberAccess)
				throw new NotSupportedException(exp.NodeType.ToString() + " is not supported.");

			return ((MemberExpression)exp).Member.Name;
		}

		#endregion

		#region Expression Parsing

		public static string GetSql(Expression expression)
		{
			var eval = GetEvaluator(expression);
			return eval.Query;
		}

		public static CqlMapperQueryEvaluator GetEvaluator(Expression expression)
		{
			var eval = new CqlMapperQueryEvaluator();
			eval.Evaluate(expression);

			return eval;
		}

		private void Evaluate(Expression exp, Action<string> call = null)
		{
			switch (exp.NodeType)
			{
				case ExpressionType.Lambda:
					Evaluate(((LambdaExpression)exp).Body);
					break;

				case ExpressionType.Call:
					VisitMethodCall((MethodCallExpression)exp);
					break;

				case ExpressionType.New:
					VisitNew((NewExpression)exp, call);
					break;

				case ExpressionType.MemberInit:
					VisitMemberInit((MemberInitExpression)exp, call);
					break;

				case ExpressionType.MemberAccess:
					VisitMemberAccess((MemberExpression)exp, call);
					break;

				case ExpressionType.Constant:
					AddTable(((ConstantExpression)exp).Value as CqlMapperQueryProvider);
					break;
			}
		}

		private void VisitMemberAccess(MemberExpression exp, Action<string> call)
		{
			call(GetPropertyName(exp));
		}

		private void VisitMemberInit(MemberInitExpression exp, Action<string> call)
		{
			foreach (MemberAssignment member in exp.Bindings)
				call(GetPropertyName(member.Expression));
		}

		private void VisitNew(NewExpression exp, Action<string> call)
		{
			foreach (var arg in exp.Arguments)
				call(GetPropertyName(arg));

			VisitMemberInit(Expression.MemberInit(exp, new MemberBinding[0]), call);
		}

		private void VisitMethodCall(MethodCallExpression exp)
		{
			Evaluate(exp.Arguments[0]);

			if (exp.Method.Name == "Where")
				AddCriteria(exp.Arguments[1]);
			else if (exp.Method.Name == "OrderBy" || exp.Method.Name == "ThenBy")
				AddOrderBy(GetPropertyName(exp.Arguments[1]), true);
			else if (exp.Method.Name == "OrderByDescending" || exp.Method.Name == "ThenByDescending")
				AddOrderBy(GetPropertyName(exp.Arguments[1]), false);
			else if (exp.Method.Name == "Select")
				AddField(SimplifyExpression(exp.Arguments[1]));
			else if (exp.Method.Name == "GroupBy")
				AddGroupBy(SimplifyExpression(exp.Arguments[1]));
			else if (exp.Method.Name == "Sum")
				Evaluate(SimplifyExpression(exp.Arguments[1]), propName => AddField("SUM(" + propName + ")"));
			else if (exp.Method.Name == "Avg")
				Evaluate(SimplifyExpression(exp.Arguments[1]), propName => AddField("AVG(" + propName + ")"));
			else
				throw new NotSupportedException("Method call to " + exp.Method.Name + " is not supported.");
		}

		private string VisitExpression(Expression exp)
		{
			switch (exp.NodeType)
			{
				case ExpressionType.Convert:
				case ExpressionType.Lambda:
				case ExpressionType.Quote:
					return VisitExpression(SimplifyExpression(exp));

				case ExpressionType.Not:
					return VisitUnaryExpression((UnaryExpression)exp);

				case ExpressionType.Equal:
				case ExpressionType.NotEqual:
				case ExpressionType.GreaterThan:
				case ExpressionType.GreaterThanOrEqual:
				case ExpressionType.LessThan:
				case ExpressionType.LessThanOrEqual:
					return VisitRelationalExpression((BinaryExpression)exp);

				case ExpressionType.And:
				case ExpressionType.AndAlso:
				case ExpressionType.Or:
				case ExpressionType.OrElse:
					return VisitConditionalExpression((BinaryExpression)exp);

				case ExpressionType.Call:
					return VisitMethodCallExpression((MethodCallExpression)exp);

				default:
					throw new NotSupportedException(exp.NodeType.ToString() + " is not supported.");
			}
		}

		private string VisitUnaryExpression(UnaryExpression exp)
		{
			switch (exp.NodeType)
			{
				case ExpressionType.Not:
					return "NOT (" + VisitExpression(exp.Operand) + ")";

				default:
					throw new NotSupportedException(exp.NodeType.ToString() + " is not a supported unary criteria.");
			}
		}

		private string VisitMethodCallExpression(MethodCallExpression exp)
		{
			if (exp.Method.Name == "Contains")
			{
				var left = GetPropertyName(exp.Arguments[1]);
				string right = "@param" + _parameterCount++;

				var values = (IEnumerable)Expression.Lambda(exp.Arguments[0]).Compile().DynamicInvoke();
				Parameters.Add(right, values);

				return left + " IN (" + right + ")";
			}
			else
				throw new NotSupportedException("Method call to " + exp.Method.Name + " is not supported.");
		}

		private string VisitRelationalExpression(BinaryExpression exp)
		{
			string criteria;

			string left = GetPropertyName(exp.Left);
			string right = "@param" + _parameterCount++;

			object rightObj = Expression.Lambda(exp.Right).Compile().DynamicInvoke();
			Parameters.Add(right, rightObj);

			switch (exp.NodeType)
			{
				case ExpressionType.Equal:
					if (rightObj == null)
						criteria = left + " IS NULL";
					else
						criteria = left + " = " + right;
					break;
				case ExpressionType.NotEqual:
					if (rightObj == null)
						criteria = left + " IS NOT NULL";
					else
						criteria = left + " != " + right;
					break;
				case ExpressionType.GreaterThan:
					criteria = left + " > " + right;
					break;
				case ExpressionType.GreaterThanOrEqual:
					criteria = left + " >= " + right;
					break;
				case ExpressionType.LessThan:
					criteria = left + " < " + right;
					break;
				case ExpressionType.LessThanOrEqual:
					criteria = left + " <= " + right;
					break;

				default:
					throw new NotSupportedException(
						exp.NodeType.ToString() + " is not a supported relational criteria.");
			}

			return criteria;
		}

		private string VisitConditionalExpression(BinaryExpression exp)
		{
			string criteria;

			switch (exp.NodeType)
			{
				case ExpressionType.And:
				case ExpressionType.AndAlso:
					criteria = "(" + VisitExpression(exp.Left) + " AND " + VisitExpression(exp.Right) + ")";
					break;
				case ExpressionType.Or:
				case ExpressionType.OrElse:
					criteria = "(" + VisitExpression(exp.Left) + " OR " + VisitExpression(exp.Right) + ")";
					break;

				default:
					throw new NotSupportedException(exp.NodeType.ToString() + " is not a supported conditional criteria.");
			}

			return criteria;
		}
		#endregion
	}
}