using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.IO;
using System.Configuration;

namespace OraStoredProcToCSV
{
    class Program
    {
        static void Main(string[] args)
        {
            string conn = "";
            try
            {
                conn = args[0];
            }
            catch
            {
                Console.WriteLine("First Argument must be a connection defined in the config file");
                return;
            }

            string type = "";
            try {
                type = args[1];
                Console.WriteLine(type);
                string[] validTypes = new string[]{"table","proc"};
                if (!validTypes.Contains(type)){
                    Console.WriteLine("Second Arg must be table or proc.");
                    return;
                }
            } catch{Console.WriteLine("Missing command type (second arg)");}


            string proc = "";
            try
            {
                proc =args[2];
            }
            catch
            {
                Console.WriteLine("Third Argument must be a Stored Procedure for ");
                return;
            }

            DataTable dt = new DataTable();
            if (type =="table"){               
                dt = OracleRS.GetDataTable("select * from " + proc , conn);
            }
            else if (type == "proc")
            {
                OracleRS.Command cmd = new OracleRS.Command(proc, conn);
                dt = cmd.GetDataTable();
            }
            else
            {
                return;
            }

            StringBuilder sb = new StringBuilder();

            IEnumerable<string> columnNames = dt.Columns.Cast<DataColumn>().
                                              Select(column => column.ColumnName);
            sb.AppendLine("\"" + string.Join("\",\"", columnNames) + "\"");

            foreach (DataRow row in dt.Rows)
            {
                IEnumerable<string> fields = row.ItemArray.Select(field => field.ToString());
                sb.AppendLine("\"" + string.Join("\",\"", fields) + "\"");
            }

            File.WriteAllText(proc.ToLower() + " " + DateTime.Now.ToString("yyyy-MM-dd HH-mm-ss") + ".csv", sb.ToString());
        }
    }
}
