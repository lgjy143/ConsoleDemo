using Oracle.DataAccess.Client;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace ConsoleDemo
{
    public class OracleOpt
    {
        public static string GetConnectionString(string connectionStringName = "EASConnectionString")
        {
            return ConfigurationManager.ConnectionStrings[connectionStringName].ConnectionString;
        }
        /// <summary>
        /// Oracle普通堆积SQL
        /// </summary>
        /// <param name="recc"></param>
        /// <returns></returns>
        public static string BatchInsertSQL(int recc)
        {
            var retVal = string.Empty;
            try
            {
                //string connectStr = "User Id=scott;Password=tiger;Data Source=";
                var connectStr = GetConnectionString();
                OracleConnection conn = new OracleConnection(connectStr);
                OracleCommand command = new OracleCommand();
                command.Connection = conn;
                conn.Open();

                Stopwatch sw = new Stopwatch();
                sw.Start();

                //通过循环写入大量的数据，这种方法显然是肉垫 
                for (int i = 0; i < recc; i++)
                {
                    string sql = "insert into dept values(" + i.ToString() + "," + i.ToString() + "," + i.ToString() + "," + "to_date('" + DateTime.Now + "', 'yyyy-MM-dd HH24:mi:ss')" + ")";

                    command.CommandText = sql;
                    command.ExecuteNonQuery();
                }

                sw.Stop();
                retVal = "普通插入:" + recc.ToString() + "耗时:" + sw.Elapsed.TotalSeconds.ToString() + " 秒";
                //System.Diagnostics.Debug.WriteLine("普通插入:" + recc.ToString() + "耗时:" + sw.ElapsedMilliseconds.ToString());
            }
            catch (Exception ex)
            {
                retVal = ex.Message;
            }
            Console.WriteLine(retVal);
            return retVal;
        }
        /// <summary>
        /// Oracle批量插入,ODP特性
        /// </summary>
        /// <param name="recc"></param>
        /// <returns></returns>
        public static string BatchInsert(int recc)
        {
            Console.WriteLine("[BatchInsert]:Start.");
            var retVal = string.Empty;
            try
            {
                //var connectionString = GetConnectionString("EASConnectionString");

                //int recc = 1000000;
                //设置一个数据库的连接串   
                //string connectStr = "User Id=scott;Password=tiger;Data Source=";
                OracleConnection conn = new OracleConnection(GetConnectionString());
                OracleCommand command = new OracleCommand();
                command.Connection = conn; //到此为止，还都是我们熟悉的代码，下面就要开始喽   
                                           //这个参数需要指定每次批插入的记录数   
                command.ArrayBindCount = recc;
                //在这个命令行中,用到了参数,参数我们很熟悉,但是这个参数在传值的时候   
                //用到的是数组,而不是单个的值,这就是它独特的地方   
                //command.CommandText = "insert into dept values(:deptno, :deptname, :loc)";
                command.CommandText = "insert into dept(deptno,deptname,loc,createtime)values(:deptno, :deptname, :loc, :createtime)";
                conn.Open();
                //下面定义几个数组,分别表示三个字段,数组的长度由参数直接给出   
                int[] deptNo = new int[recc];
                string[] dname = new string[recc];
                string[] loc = new string[recc];
                DateTime[] createtime = new DateTime[recc];
                // 为了传递参数,不可避免的要使用参数,下面会连续定义三个   
                // 从名称可以直接看出每个参数的含义,不在每个解释了   
                OracleParameter deptNoParam = new OracleParameter("deptno", OracleDbType.Int32);
                deptNoParam.Direction = ParameterDirection.Input;
                deptNoParam.Value = deptNo; command.Parameters.Add(deptNoParam);

                OracleParameter deptNameParam = new OracleParameter("deptname", OracleDbType.Varchar2);
                deptNameParam.Direction = ParameterDirection.Input;
                deptNameParam.Value = dname;
                command.Parameters.Add(deptNameParam);

                OracleParameter deptLocParam = new OracleParameter("loc", OracleDbType.Varchar2);
                deptLocParam.Direction = ParameterDirection.Input;
                deptLocParam.Value = loc;
                command.Parameters.Add(deptLocParam);

                OracleParameter deptCreatetimeParam = new OracleParameter("createtime", OracleDbType.TimeStamp);
                deptCreatetimeParam.Direction = ParameterDirection.Input;
                deptCreatetimeParam.Value = createtime;
                command.Parameters.Add(deptCreatetimeParam);

                Stopwatch sw = new Stopwatch();
                sw.Start();
                //在下面的循环中,先把数组定义好,而不是像上面那样直接生成SQL   
                for (int i = 0; i < recc; i++)
                {
                    deptNo[i] = i;
                    dname[i] = i.ToString();
                    loc[i] = i.ToString();
                    createtime[i] = DateTime.Now;
                }
                //这个调用将把参数数组传进SQL,同时写入数据库   
                command.ExecuteNonQuery();
                sw.Stop();
                //System.Diagnostics.Debug.WriteLine("批量插入:" + recc.ToString() + "耗时:" + sw.ElapsedMilliseconds.ToString());

                retVal = "批量插入:" + recc.ToString() + "耗时:" + sw.Elapsed.TotalSeconds.ToString() + " 秒";
            }
            catch (Exception ex)
            {
                retVal = ex.Message;
            }
            Console.WriteLine("[BatchInsert] :End. " + retVal);
            return retVal;
        }
        /// <summary>
        /// Oracle批量插入数据,ODP特性
        /// </summary>
        /// <param name="tableName">表名称</param>
        /// <param name="columnRowData">键-值存储的批量数据：键是列名称，值是对应的数据集合</param>
        /// <param name="conStr">连接字符串</param>
        /// <param name="len">每次批处理数据的大小</param>
        /// <returns></returns>
        public static int BatchInsert(string tableName, Dictionary<string, object> columnRowData, string conStr, int len)
        {
            if (string.IsNullOrEmpty(tableName))
            {
                throw new ArgumentException("必须指定批量插入的表名称", "tableName");
            }

            if (columnRowData == null || columnRowData.Count < 1)
            {
                throw new ArgumentException("必须指定批量插入的字段名称", "columnRowData");
            }

            Console.WriteLine("[BatchInsert]:Start.");
            Stopwatch sw = new Stopwatch();
            sw.Start();

            int iResult = 0;
            string[] dbColumns = columnRowData.Keys.ToArray();
            StringBuilder sbCmdText = new StringBuilder();
            if (columnRowData.Count > 0)
            {
                //准备插入的SQL
                sbCmdText.AppendFormat("INSERT INTO {0}(", tableName);
                sbCmdText.Append(string.Join(",", dbColumns));
                sbCmdText.Append(") VALUES (");
                sbCmdText.Append(":" + string.Join(",:", dbColumns));
                sbCmdText.Append(")");

                using (OracleConnection conn = new OracleConnection(conStr))
                {
                    using (OracleCommand cmd = conn.CreateCommand())
                    {
                        //绑定批处理的行数
                        cmd.ArrayBindCount = len;
                        cmd.BindByName = true;
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = sbCmdText.ToString();
                        cmd.CommandTimeout = 600;//10分钟

                        //创建参数
                        OracleParameter oraParam;
                        List<IDbDataParameter> cacher = new List<IDbDataParameter>();
                        OracleDbType dbType = OracleDbType.Object;
                        foreach (string colName in dbColumns)
                        {
                            dbType = GetOracleDbType(columnRowData[colName]);
                            oraParam = new OracleParameter(colName, dbType);
                            oraParam.Direction = ParameterDirection.Input;
                            oraParam.OracleDbTypeEx = dbType;

                            oraParam.Value = columnRowData[colName];
                            cmd.Parameters.Add(oraParam);
                        }
                        //打开连接
                        conn.Open();

                        /*执行批处理*/
                        var trans = conn.BeginTransaction();
                        try
                        {
                            cmd.Transaction = trans;
                            iResult = cmd.ExecuteNonQuery();
                            trans.Commit();
                        }
                        catch (Exception ex)
                        {
                            trans.Rollback();
                            throw ex;
                        }
                        finally
                        {
                            if (conn != null) conn.Close();
                        }
                    }
                }
            }

            sw.Stop();
            Console.WriteLine("[BatchInsert] :End. " + "批量插入:" + len.ToString() + "耗时:" + sw.Elapsed.TotalSeconds.ToString() + " 秒");

            return iResult;
        }
        /// <summary>
        /// 根据数据类型获取OracleDbType
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        private static OracleDbType GetOracleDbType(object value)
        {
            OracleDbType dataType = OracleDbType.Object;
            if (value is string[])
            {
                dataType = OracleDbType.Varchar2;
            }
            else if (value is DateTime[])
            {
                dataType = OracleDbType.TimeStamp;
            }
            else if (value is int[] || value is short[])
            {
                dataType = OracleDbType.Int32;
            }
            else if (value is long[])
            {
                dataType = OracleDbType.Int64;
            }
            else if (value is decimal[] || value is double[] || value is float[])
            {
                dataType = OracleDbType.Decimal;
            }
            else if (value is Guid[])
            {
                dataType = OracleDbType.Varchar2;
            }
            else if (value is bool[] || value is Boolean[])
            {
                dataType = OracleDbType.Byte;
            }
            else if (value is byte[])
            {
                dataType = OracleDbType.Blob;
            }
            else if (value is char[])
            {
                dataType = OracleDbType.Char;
            }
            return dataType;
        }
    }
}
