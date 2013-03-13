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

            string filename = proc.ToLower() + " " + DateTime.Now.ToString("yyyy-MM-dd HH-mm-ss") + ".csv";

            //RKLib.ExportData.Export objExport = new RKLib.ExportData.Export();
            //objExport.ExportDetails(dt, RKLib.ExportData.Export.ExportFormat.Excel, "Nametag.xls");

            StringBuilder sb = new StringBuilder();

            IEnumerable<string> columnNames = dt.Columns.Cast<DataColumn>().
                                              Select(column => column.ColumnName);
            sb.AppendLine(EncodeCsvLine(columnNames));

            foreach (DataRow row in dt.Rows)
            {
                IEnumerable<string> fields = row.ItemArray.Select(field => field.ToString());
                sb.AppendLine(EncodeCsvLine(fields));
            }

            File.WriteAllText(filename, sb.ToString());
        }

        static char DelimiterChar = ',';
        public static String EncodeCsvLine(IEnumerable<string> fields)
        {
            StringBuilder line = new StringBuilder();
            int i = 0;
            foreach (string field in fields)
            {
                if (i > 0) { line.Append(DelimiterChar); }
                String csvField = EncodeCsvField(field);
                line.Append(csvField);
                i++;
            }
            return line.ToString();
        }

        static String EncodeCsvField(String field)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append(field);

            // Some fields with special characters must be embedded in double quotes
            bool embedInQuotes = false;

            // Embed in quotes to preserve leading/tralining whitespace
            if (sb.Length > 0 &&
                (sb[0] == ' ' ||
                 sb[0] == '\t' ||
                 sb[sb.Length - 1] == ' ' ||
                 sb[sb.Length - 1] == '\t')) { embedInQuotes = true; }

            for (int i = 0; i < sb.Length; i++)
            {
                // Embed in quotes to preserve: commas, line-breaks etc.
                if (sb[i] == DelimiterChar ||
                    sb[i] == '\r' ||
                    sb[i] == '\n' ||
                    sb[i] == '"')
                {
                    embedInQuotes = true;
                    break;
                }
            }

            // If the field itself has quotes, they must each be represented 
            // by a pair of consecutive quotes.
            sb.Replace("\"", "\"\"");

            String rv = sb.ToString();
            if (embedInQuotes) { rv = "\"" + rv + "\""; }
            return rv;
        }
    }
}
