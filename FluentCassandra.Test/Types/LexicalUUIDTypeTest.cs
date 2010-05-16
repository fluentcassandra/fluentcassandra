using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using FluentCassandra.Types;

namespace FluentCassandra.Test.Types
{
	[TestClass]
	public class LexicalUUIDTypeTest
	{
		private Guid guid = new Guid("38400000-8cf0-11bd-b23e-10b96e4ef00d");
		private byte[] dotNetByteOrder = new byte[] { 0x00, 0x00, 0x40, 0x38, 0xF0, 0x8C, 0xBD, 0x11, 0xB2, 0x3E, 0x10, 0xB9, 0x6E, 0x4E, 0xF0, 0x0D };
		private byte[] javaByteOrder = new byte[] { 0x38, 0x40, 0x00, 0x00, 0x8C, 0xF0, 0x11, 0xBD, 0xB2, 0x3E, 0x10, 0xB9, 0x6E, 0x4E, 0xF0, 0x0D };

		[TestMethod]
		public void Guid_To_JavaBytes()
		{
			// arrange
		
			// act
			TimeUUIDType actual = guid;

			// assert
			Assert.IsTrue(actual.GetValue<byte[]>().SequenceEqual(javaByteOrder));
		}

		[TestMethod]
		public void JavaBytes_To_Guid()
		{
			// arrange

			// act
			TimeUUIDType actual = new TimeUUIDType();
			actual.SetValue(javaByteOrder);

			// assert
			Assert.AreEqual(guid, (Guid)actual);
		}
	}
}
