using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ConsoleDemo
{
    class Program
    {
        static void Main(string[] args)
        {
            BatchInsertTest0();
            Console.WriteLine();
            BatchInsertTest1();
            Console.WriteLine();
            BatchInsertTest2();
            Console.WriteLine();

            Console.ReadKey();
        } 
        /// <summary>
        /// Oracle普通堆积SQL
        /// </summary>
        private static void BatchInsertTest0()
        {
            var retVal = string.Empty;
            retVal = OracleOpt.BatchInsertSQL(1000);
            Console.WriteLine("====================================");
            retVal = OracleOpt.BatchInsertSQL(10000);
            Console.WriteLine("====================================");
            //retVal = OracleOpt.BatchInsertSQL(100000);
            //Console.WriteLine("====================================");
        }
        /// <summary>
        /// Oracle批量插入,ODP特性
        /// </summary>
        private static void BatchInsertTest1()
        {
            var retVal = string.Empty;
            retVal = OracleOpt.BatchInsert(10000);
            Console.WriteLine("====================================");
            retVal = OracleOpt.BatchInsert(100000);
            Console.WriteLine("====================================");
            retVal = OracleOpt.BatchInsert(1000000);
            Console.WriteLine("====================================");
        }
        /// <summary>
        /// Oracle批量插入,ODP特性
        /// </summary>
        private static void BatchInsertTest2()
        {
            BatchInsertExec(10000);
            Console.WriteLine("====================================");
            BatchInsertExec(100000);
            Console.WriteLine("====================================");
            BatchInsertExec(1000000);
            Console.WriteLine("====================================");
        }
        private static void BatchInsertExec(int len)
        {
            int[] vdeptno = new int[len];
            string[] vdeptname = new string[len];
            string[] vloc = new string[len];
            DateTime[] vcreatetime = new DateTime[len];

            for (int i = 1; i <= len; i++)
            {
                vdeptno[i - 1] = i;
                vdeptname[i - 1] = i.ToString();
                vloc[i - 1] = i.ToString();
                vcreatetime[i - 1] = DateTime.Now;
            }
            var dicData = new Dictionary<string, object>()
            {
                {"deptno",vdeptno },
                {"deptname",vdeptname },
                {"loc",vloc },
                {"createtime",vcreatetime }
            };
            OracleOpt.BatchInsert("dept", dicData, OracleOpt.GetConnectionString(), len);
        }
    }
}
