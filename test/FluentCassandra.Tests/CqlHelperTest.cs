using System;
using System.Linq;
using System.Text;
using Xunit;

namespace FluentCassandra
{
    public class CQLHelperTest
    {
        [Fact]
        public void EscapeForCqlTest()
        {
            // arrange
            var expected = "My''Test''Data";
            
            // act
            var actual = CqlHelper.EscapeForCql("My'Test'Data");

            // assert
            Assert.Equal(expected, actual);
        }

        [Fact]
        public void FormatCqlTest()
        {
            string[] arr1 = new string[] { "one'", "tw'o", "'three" };
            string format = "{0} : {1} - {2}";
            // arrange
            var expected = "one'' : tw''o - ''three";

            // act
            var actual = CqlHelper.FormatCql(format, arr1);

            // assert
            Assert.Equal(expected, actual);
        }

        [Fact]
        public void EscapeBytesForCqlTest()
        {
            //arrange
            var bytes = Encoding.UTF8.GetBytes("hello world");
            var expectedHex = "0x68656c6c6f20776f726c64"; //generated via the textAsBlob function in CQL3 shell

            //act
            var hex = CqlHelper.EscapeForCql(bytes);

            //assert
            Assert.Equal(expectedHex, hex);
        }

    }
}
