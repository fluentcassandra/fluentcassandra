using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Xml.Linq;
using GalaSoft.MvvmLight;
using GalaSoft.MvvmLight.Command;
using LINQPad.Extensibility.DataContext;
using FluentCassandra.Connections;

namespace FluentCassandra.LinqPad
{
	public class CassandraConnectionInfo : ViewModelBase
	{
		public const string CassandraConnectionInfoKey = "CassandraConnectionInfo";

		public IConnectionInfo ConntectionInfo { get; set; }

		public RelayCommand SaveCommand { get; set; }

		public const string HostPropertyName = "Host";
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

				RaisePropertyChanged(HostPropertyName);
			}
		}

		public const string PortPropertyName = "Port";
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

				RaisePropertyChanged(PortPropertyName);
			}
		}

		public const string TimeoutPropertyName = "Timeout";
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

				RaisePropertyChanged(TimeoutPropertyName);
			}
		}

		public const string KeyspacePropertyName = "Keyspace";
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

				RaisePropertyChanged(KeyspacePropertyName);
			}
		}

		public const string UsernamePropertyName = "Username";
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

				RaisePropertyChanged(UsernamePropertyName);
			}
		}

		public const string PasswordPropertyName = "Password";
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

				RaisePropertyChanged(PasswordPropertyName);
			}
		}

		public CassandraConnectionInfo()
		{
			SaveCommand = new RelayCommand(Save, CanSave);
		}

		public void Save()
		{
			string pw = Password;
			if (!Password.IsNullOrWhitespace())
				Password = ConntectionInfo.Encrypt(Password);

			var builder = new ConnectionBuilder(
				keyspace: Keyspace,
				host: Host,
				port: Port,
				connectionTimeout: Timeout,
				username: Username,
				password: Password);
			
			ConntectionInfo.DriverData.SetElementValue(CassandraConnectionInfoKey, builder.ConnectionString);

			Password = pw;
		}

		public bool CanSave()
		{
			return !Host.IsNullOrWhitespace()
				&& !Keyspace.IsNullOrWhitespace();
		}

		public CassandraContext CreateContext()
		{
			return new CassandraContext(
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