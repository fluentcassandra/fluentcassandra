using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FluentCassandra.StressTest
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.Error.WriteLine("Stress Test 1");
            try
            {
                StressTest.StressTest1.Test();
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex.Message);
                Console.Error.WriteLine(ex.StackTrace);
            }
            Console.Error.WriteLine("");
            Console.Error.WriteLine("Stress Test 2");
            try
            {
                StressTest.StressTest2.Test();
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex.Message);
                Console.Error.WriteLine(ex.StackTrace);
            }
        }
    }
}
