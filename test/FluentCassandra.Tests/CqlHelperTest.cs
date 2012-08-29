using System;
using System.Linq;
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
    }
}
