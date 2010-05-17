using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FluentCassandra.Operations;
using System.Linq.Expressions;
using System.Collections;

namespace FluentCassandra.Linq
{
	internal class SliceQuery<T> : IQueryable<T>
	{
		private QueryableColumnFamilyOperation<T> _op;
		private BaseCassandraColumnFamily _family;

		internal SliceQuery(QueryableColumnFamilyOperation<T> op, BaseCassandraColumnFamily family)
		{
			_op = op;
			_family = family;
		}

		#region IEnumerable<TResult> Members

		public IEnumerator<T> GetEnumerator()
		{
			return ((IEnumerable<T>)((IQueryable)this).Provider.Execute(((IQueryable)this).Expression)).GetEnumerator();
		}

		#endregion

		#region IEnumerable Members

		System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}

		#endregion

		#region IQueryable Members

		public Type ElementType
		{
			get { return typeof(T); }
		}

		public Expression Expression
		{
			get { return Expression.Constant(this); }
		}

		public IQueryProvider Provider
		{
			get { return this; }
		}

		#endregion

		#region IQueryProvider Members

		public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
		{
			if (expression == null)
				throw new ArgumentNullException("expression");

			if (!typeof(IQueryable<TElement>).IsAssignableFrom(expression.Type))
				throw new ApplicationException("'expression' is not assignable from this type of repository.");

			var predicate = Evaluate(expression);
			_op.SlicePredicate = predicate;

			return this;
		}

		public IQueryable CreateQuery(Expression expression)
		{
			if (expression == null)
				throw new ArgumentNullException("expression");

			var predicate = Evaluate(expression);
			_op.SlicePredicate = predicate;

			return this;
		}

		public TResult Execute<TResult>(Expression expression)
		{
			if (expression == null)
				throw new ArgumentNullException("expression");

			var predicate = Evaluate(expression);
			_op.SlicePredicate = predicate;

			return (TResult)_family.ExecuteOperation(_op);
		}

		public object Execute(Expression expression)
		{
			if (expression == null)
				throw new ArgumentNullException("expression");

			var predicate = Evaluate(expression);
			_op.SlicePredicate = predicate;

			return _family.ExecuteOperation(_op);
		}

		#endregion

		private CassandraSlicePredicate Evaluate(Expression exp)
		{
			return Evaluate(parameters, exp, null);
		}

		private ReportDataParameters<T> Evaluate(ReportDataParameters<T> parameters, Expression exp, Func<Expression, string> expFunc)
		{
			switch (exp.NodeType)
			{
				case ExpressionType.Lambda:
					return Evaluate(parameters, ((LambdaExpression)exp).Body);

				case ExpressionType.Call:
					return VisitMethodCall(parameters, (MethodCallExpression)exp);

				case ExpressionType.New:
					return VisitNew(parameters, (NewExpression)exp);

				case ExpressionType.MemberInit:
					return VisitMemberInit(parameters, (MemberInitExpression)exp);

				case ExpressionType.MemberAccess:
					return VisitMemberAccess(parameters, (MemberExpression)exp, expFunc);

				case ExpressionType.Constant:
					return parameters;

				default:
					parameters.AddCriteria(exp);
					break;
			}

			return parameters;
		}

		private ReportDataParameters<T> VisitMemberAccess(ReportDataParameters<T> parameters, MemberExpression exp, Func<Expression, string> expFunc)
		{
			if (expFunc == null)
				parameters.AddField(GetPropertyName(exp));
			else
				parameters.AddField(expFunc(exp));

			return parameters;
		}

		private ReportDataParameters<T> VisitMemberInit(ReportDataParameters<T> parameters, MemberInitExpression exp)
		{
			parameters.SetMemberInitExpression(exp);
			foreach (MemberAssignment member in exp.Bindings)
				parameters.AddField(GetPropertyName(member.Expression));

			return parameters;
		}

		private ReportDataParameters<T> VisitNew(ReportDataParameters<T> parameters, NewExpression exp)
		{
			foreach (var arg in exp.Arguments)
				parameters.AddField(GetPropertyName(arg));

			return VisitMemberInit(parameters, Expression.MemberInit(exp, new MemberBinding[0]));
		}

		private ReportDataParameters<T> VisitMethodCall(ReportDataParameters<T> parameters, MethodCallExpression exp)
		{
			parameters = Evaluate(parameters, exp.Arguments[0]);

			if (exp.Method.Name == "Where")
				return Evaluate(parameters, exp.Arguments[1]);
			else if (exp.Method.Name == "OrderBy" || exp.Method.Name == "ThenBy")
				parameters.AddOrderBy(GetPropertyName(exp.Arguments[1]), true);
			else if (exp.Method.Name == "OrderByDescending" || exp.Method.Name == "ThenByDescending")
				parameters.AddOrderBy(GetPropertyName(exp.Arguments[1]), false);
			else if (exp.Method.Name == "BetweenDates")
				parameters.BetweenDates((DateTime)Expression.Lambda(exp.Arguments[1]).Compile().DynamicInvoke(), (DateTime)Expression.Lambda(exp.Arguments[2]).Compile().DynamicInvoke());
			else if (exp.Method.Name == "Select")
				return Evaluate(parameters, SimplifyExpression(exp.Arguments[1]));
			else if (exp.Method.Name == "Sum")
				return Evaluate(parameters, SimplifyExpression(exp.Arguments[1]), iexp => "SUM(" + GetPropertyName(iexp) + ")");
			else
				throw new NotSupportedException("Method call to " + exp.Method.Name + " is not supported.");

			return parameters;
		}

		#region Expression Helpers

		private static Expression SimplifyExpression(Expression exp)
		{
			return SimplifyExpression(exp, null);
		}

		private static Expression SimplifyExpression(Expression exp, ExpressionType? returnType)
		{
			if (returnType.HasValue && returnType.Value == exp.NodeType)
				return exp;

			switch (exp.NodeType)
			{
				case ExpressionType.Convert:
				case ExpressionType.Quote:
					return SimplifyExpression(((UnaryExpression)exp).Operand, returnType);

				case ExpressionType.Lambda:
					return SimplifyExpression(((LambdaExpression)exp).Body, returnType);

				default:
					return exp;
			}
		}

		private static string GetPropertyName(Expression exp)
		{
			exp = SimplifyExpression(exp);

			if (exp.NodeType != ExpressionType.MemberAccess)
				throw new NotSupportedException(exp.NodeType.ToString() + " is not supported.");

			MemberExpression mExp = (MemberExpression)exp;
			List<string> members = new List<string>();
			members.Insert(0, mExp.Member.Name);

			switch (mExp.Expression.NodeType)
			{
				case ExpressionType.MemberAccess:
					members.Insert(0, GetPropertyName(mExp.Expression));
					break;
				case ExpressionType.Parameter:
					members.Insert(0, "");
					break;

				default:
					throw new NotSupportedException(mExp.Expression.NodeType.ToString() + " is not supported.");
			}

			return String.Join(".", members.Where(s => !String.IsNullOrEmpty(s)).ToArray());
		}

		#endregion
	}
}
