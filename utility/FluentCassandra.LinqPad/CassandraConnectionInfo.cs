using System;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Xml.Linq;
using FluentCassandra.Connections;
using LINQPad.Extensibility.DataContext;

namespace FluentCassandra.LinqPad
{
	public class CassandraConnectionInfo
	{
		public const string CassandraConnectionInfoKey = "CassandraConnectionInfo";

		public IConnectionInfo ConntectionInfo { get; set; }

		private string _host = "127.0.0.1";
		[Required]
		public string Host
		{
			get
			{
				return _host;
			}

			set
			{
				if (_host == value)
				{
					return;
				}

				_host = value;
			}
		}

		private int _port = Server.DefaultPort;
		[Required]
		public int Port
		{
			get
			{
				return _port;
			}

			set
			{
				if (_port == value)
				{
					return;
				}

				_port = value;
			}
		}

		private int _timeout = Server.DefaultTimeout;
		public int Timeout
		{
			get
			{
				return _timeout;
			}

			set
			{
				if (_timeout == value)
				{
					return;
				}
				_timeout = value;
			}
		}

		private string _keyspace = null;
		public string Keyspace
		{
			get
			{
				return _keyspace;
			}

			set
			{
				if (_keyspace == value)
				{
					return;
				}

				_keyspace = value;
			}
		}

		private string _username = null;
		public string Username
		{
			get
			{
				return _username;
			}

			set
			{
				if (_username == value)
				{
					return;
				}

				_username = value;
			}
		}

		private string _password = null;
		public string Password
		{
			get
			{
				return _password;
			}

			set
			{
				if (_password == value)
					return;

				_password = value;
			}
		}

		public void Save()
		{
			var builder = new ConnectionBuilder(
				keyspace: Keyspace,
				host: Host,
				port: Port,
				connectionTimeout: Timeout,
				username: Username,
				password: Password);
			
			ConntectionInfo.DriverData.SetElementValue(CassandraConnectionInfoKey, builder.ConnectionString);
		}

		public bool CanSave()
		{
			return !String.IsNullOrWhiteSpace(Host)
				&& !String.IsNullOrWhiteSpace(Keyspace);
		}

		public FluentCassandra.CassandraContext CreateContext()
		{
			return new FluentCassandra.CassandraContext(
				keyspace: Keyspace,
				host: Host,
				port: Port,
				timeout: Timeout,
				username: Username,
				password: Password);
		}

		public static CassandraConnectionInfo Load(IConnectionInfo connectionInfo)
		{
			XElement xe = connectionInfo.DriverData.Element(CassandraConnectionInfoKey);
			if (xe != null)
			{
				var connectionString = xe.Value;
				var builder = new ConnectionBuilder(connectionString);
				var info = new CassandraConnectionInfo();
				info.ConntectionInfo = connectionInfo;

				info.Keyspace = builder.Keyspace;
				info.Host = builder.Servers[0].Host;
				info.Port = builder.Servers[0].Port;
				info.Timeout = builder.Servers[0].Timeout;
				info.Username = builder.Username;
				info.Password = builder.Password;

				return info;
			}

			return null;
		}
	}
}