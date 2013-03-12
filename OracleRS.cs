using System;
using System.Data;
using System.Configuration;
using System.Data.OracleClient;
using System.Text;


/// <summary>
/// Summary description for OracleRS
/// </summary>
public class OracleRS
{
    private OracleDataReader myReader;
    private OracleConnection oConn;

    public static System.Data.DataSet GetDataSet(string queryString, string connectionProfileName)
    {
        string connectionString = ConfigurationSettings.AppSettings[connectionProfileName];
        System.Data.IDbConnection dbConnection = new System.Data.OracleClient.OracleConnection(connectionString);
        System.Data.IDbCommand dbCommand = new System.Data.OracleClient.OracleCommand();
        dbCommand.CommandText = queryString;
        dbCommand.Connection = dbConnection;
        System.Data.IDbDataAdapter dataAdapter = new System.Data.OracleClient.OracleDataAdapter();
        dataAdapter.SelectCommand = dbCommand;
        System.Data.DataSet dataSet = new System.Data.DataSet();
        dataAdapter.Fill(dataSet);
        dbConnection.Close();
        return dataSet;

    }

    public static System.Data.DataTable GetDataTable(string queryString, string connectionProfileName)
    {
        string connectionString = ConfigurationSettings.AppSettings[connectionProfileName];
        DataTable myDataTable = new DataTable();
        try
        {
            OracleCommand cmd = new OracleCommand(queryString, new OracleConnection(connectionString));
            OracleDataAdapter adapter = new OracleDataAdapter(cmd);
            adapter.Fill(myDataTable);
        }
        catch (Exception ex)
        {
            Console.Write("OracleRS.GetDataTable()", ex.Message + " - " + queryString, "", ex.StackTrace);
            throw ex;
        }
        return myDataTable;

    }

    public static DataTable GetDataTableFromProc(string procedureName, string connectionProfileName)
    {
        string connectionString = ConfigurationSettings.AppSettings[connectionProfileName];
        OracleConnection oConn = new OracleConnection(connectionString);
        OracleCommand cmd = new OracleCommand(procedureName, oConn);
        cmd.CommandType = CommandType.StoredProcedure;
        cmd.Parameters.Add("RC", OracleType.Cursor).Direction = ParameterDirection.Output;
        OracleDataAdapter ad = new OracleDataAdapter();
        ad.SelectCommand = cmd;
        DataTable dtAns = new DataTable();
        ad.Fill(dtAns);
        oConn.Close();
        return dtAns;
    }
    public static DataTable GetDataTableFromProc(string procedureName, string connectionProfileName, string[] variableName, string[] variableValue)
    {
        string connectionString = ConfigurationSettings.AppSettings[connectionProfileName];
        DataTable dtAns = new DataTable();
        OracleCommand cmd = new OracleCommand(procedureName, new OracleConnection(connectionString));
        cmd.CommandType = CommandType.StoredProcedure;
        cmd.Parameters.Add("RC", OracleType.Cursor).Direction = ParameterDirection.Output;

        int counter = -1;
        foreach (string var in variableValue)
        {
            counter++;
            cmd.Parameters.Add(variableName[counter], OracleType.VarChar).Value = var;
        }
        OracleDataAdapter ad = new OracleDataAdapter();
        ad.SelectCommand = cmd;
        ad.Fill(dtAns);
        return dtAns;
    }

    public OracleRS(string strSQL, string strConnection)
    {
        try
        {
            string connectionString = ConfigurationSettings.AppSettings[strConnection];
            oConn = new OracleConnection(connectionString);
            OracleCommand oComm = oConn.CreateCommand();
            OracleCommand myCommand = new OracleCommand(strSQL, oConn);
            oConn.Open();

            myReader = myCommand.ExecuteReader();
            myReader.Read();
        }
        catch (Exception Ex)
        {
            Console.Write("OracleRS", Ex.Message + " - " + strSQL + "; User: " + strConnection, "", Ex.StackTrace);
            throw Ex;
        }
    }

    public OracleRS(string strSQL, string strConnection, bool Execute)
    {
        try
        {
            if (!Execute)
            {
                string connectionString = ConfigurationSettings.AppSettings[strConnection];
                oConn = new OracleConnection(connectionString);
                OracleCommand oComm = oConn.CreateCommand();
                OracleCommand myCommand = new OracleCommand(strSQL, oConn);
                oConn.Open();

                myReader = myCommand.ExecuteReader();
                myReader.Read();
            }
            else
            {
                string connectionString = ConfigurationSettings.AppSettings[strConnection];
                oConn = new OracleConnection(connectionString);
                OracleCommand oComm = oConn.CreateCommand();
                OracleCommand myCommand = new OracleCommand(strSQL, oConn);
                oConn.Open();
                myCommand.ExecuteNonQuery();
                oConn.Close();
            }
        }
        catch (Exception Ex)
        {
            throw Ex;
        }
    }

    public OracleDataReader getRS()
    {
        return myReader;
    }

    public void CloseRS()
    {
        myReader.Close();
        oConn.Close();
        oConn.Dispose();
    }

    public void OracleRunOnce(string strSQL, string strConnection)
    {
        string connectionString = ConfigurationSettings.AppSettings[strConnection];
        oConn = new OracleConnection(connectionString);
        OracleCommand oComm = oConn.CreateCommand();
        OracleCommand myCommand = new OracleCommand(strSQL, oConn);
        oConn.Open();
        myCommand.ExecuteNonQuery();
        myCommand.Dispose();
        oConn.Close();
        oConn.Dispose();
    }

    public static void Execute(string strSQL, string strConnection)
    {
        OracleConnection oConn;
        string connectionString = ConfigurationSettings.AppSettings[strConnection];
        oConn = new OracleConnection(connectionString);
        oConn.Open();
        OracleTransaction transaction = oConn.BeginTransaction();
        OracleCommand oComm = oConn.CreateCommand();
        OracleCommand myCommand = new OracleCommand(strSQL, oConn, transaction);
        try
        {
            myCommand.ExecuteNonQuery();
            transaction.Commit();
            transaction.Dispose();
            myCommand.Dispose();
            oConn.Close();
            oConn.Dispose();
        }
        catch (Exception e)
        {
            oConn.Close();
            oConn.Dispose();
            throw e;
        }
    }

    /// <summary>
    /// Oracle Insert Statement that will also return the new value of the Primary Key.  Intended to be a single row insert statement.  Unsure what would happen with a multirow insert.
    /// </summary>
    /// <param name="strInsertSql">Base SQL statemetn</param>
    /// <param name="strConnection">Connection Name (i.e. "event")</param>
    /// <param name="pkFieldName">Name of the field that holds the primary key.</param>
    /// <returns></returns>
    public static int Insert(string strInsertSql, string strConnection, string pkFieldName)
    {
        string strSQL = strInsertSql + " RETURNING " + pkFieldName + " INTO :out_id";

        OracleConnection oConn;
        string connectionString = ConfigurationSettings.AppSettings[strConnection];
        oConn = new OracleConnection(connectionString);
        oConn.Open();
        OracleTransaction transaction = oConn.BeginTransaction();
        //OracleCommand oComm = oConn.CreateCommand();

        OracleDataAdapter oda = new OracleDataAdapter();
        oda.InsertCommand = new OracleCommand(strSQL, oConn, transaction);
        oda.InsertCommand.Parameters.Add("out_id", OracleType.Number);
        oda.InsertCommand.Parameters["out_id"].Direction = ParameterDirection.ReturnValue;

        int newId = -1;

        try
        {
            oda.InsertCommand.ExecuteNonQuery();
            transaction.Commit();
            int.TryParse(oda.InsertCommand.Parameters["out_id"].Value.ToString(), out newId);
            transaction.Dispose();
            oda.Dispose();
            oConn.Close();
            oConn.Dispose();
        }
        catch (Exception ex)
        {
            oConn.Close();
            oConn.Dispose();
            throw ex;
        }
        return newId;
    }

    public static string ExecuteScalar(string strSQL, string strConnection)
    {
        string ans = "";
        OracleConnection oConn;
        string connectionString = ConfigurationSettings.AppSettings[strConnection];
        oConn = new OracleConnection(connectionString);
        oConn.Open();
        OracleTransaction transaction = oConn.BeginTransaction();
        OracleCommand oComm = oConn.CreateCommand();
        OracleCommand myCommand = new OracleCommand(strSQL, oConn, transaction);
        try
        {
            ans = myCommand.ExecuteScalar().ToString();
            //Now, overdoit with shutdown.
            transaction.Dispose();
            myCommand.Dispose();
            oConn.Close();
            oConn.Dispose();
        }
        catch (Exception e)
        {
            oConn.Close();
            oConn.Dispose();
            throw e;
        }
        ans = ans.GetType().Equals(System.DBNull.Value) ? "" : ans;
        return ans;
    }

    /// <summary>
    /// Command Structure to execute an Oracle Proceedure
    /// </summary>
    public class Command
    {
        OracleCommand cmd;
        OracleConnection conn;

        public Command(string procedureName, string strConnection)
        {
            string connectionString = ConfigurationSettings.AppSettings[strConnection];
            conn = new OracleConnection(connectionString);
            cmd = new OracleCommand(procedureName, conn);
            cmd.CommandType = CommandType.StoredProcedure;
        }

        /// <summary>
        /// For Example:
        /// OracleRS.Command aCmd = new OracleRS.Command(sql, "event", System.Data.CommandType.Text);
        /// aCmd.AddParameterWithValue(":event_id", Request.QueryString["id"]);
        /// aCmd.AddParameterWithValue(":css", uxCSSTextBox.Text);
        /// aCmd.ExecuteNonQuery();
        /// </summary>
        /// <param name="procedureName"></param>
        /// <param name="connectionProfileName"></param>
        /// <param name="oraCommandType"></param>
        public Command(string procedureName, string strConnection, CommandType oraCommandType)
        {
            string connectionString = ConfigurationSettings.AppSettings[strConnection];
            conn = new OracleConnection(connectionString);
            cmd = new OracleCommand(procedureName, conn);
            cmd.CommandType = oraCommandType;
        }

        public void AddParameter(string parameterName, OracleType oType, object value)
        {
            cmd.Parameters.Add(parameterName, oType).Value = value;
        }

        public void AddParameterWithValue(string parameterName, object parameterValue)
        {
            cmd.Parameters.AddWithValue(parameterName, parameterValue);
        }

        public DataTable GetDataTable(bool AddCursorParameter)
        {
            try
            {
                DataTable dtAns = new DataTable();
                if (AddCursorParameter)
                    cmd.Parameters.Add("RC", OracleType.Cursor).Direction = ParameterDirection.Output;
                OracleDataAdapter ad = new OracleDataAdapter();
                ad.SelectCommand = cmd;
                ad.Fill(dtAns);
                ad.Dispose();
                cmd.Dispose();
                conn.Close();
                conn.Dispose();
                return dtAns;
            }
            catch (Exception Ex)
            {
                conn.Dispose();
                throw Ex;
            }
        }

        public DataTable GetDataTable()
        {
            return GetDataTable(true);
        }

        /// <summary>
        /// Inserts a parameter OracleType.Cursor "RC" then wrapps the command with
        /// necessary OracleDatatable wrappers to fill the dataset
        /// </summary>
        /// <returns>Output of the "RC" parameter from the stored proceedure</returns>
        public DataSet GetDataSet()
        {
            try
            {
                DataSet dtAns = new DataSet();

                cmd.Parameters.Add("RC", OracleType.Cursor).Direction = ParameterDirection.Output;
                OracleDataAdapter ad = new OracleDataAdapter();
                ad.SelectCommand = cmd;
                ad.Fill(dtAns);
                ad.Dispose();
                cmd.Dispose();
                conn.Close();
                conn.Dispose();
                return dtAns;
            }
            catch (Exception Ex)
            {
                conn.Dispose();
                throw Ex;
            }
        }
        /// <summary>
        /// Executes the command, after adding a parameter "ANS" for which it expects a return value for
        /// </summary>
        /// <returns>String ANS</returns>
        public string GetStringResponse()
        {
            cmd.Parameters.Add("ANS", OracleType.VarChar, 4000).Direction = ParameterDirection.Output;
            //cmd.Parameters["ANS"].Size = 255;
            conn.Open();
            cmd.ExecuteNonQuery();
            string ans = cmd.Parameters["ANS"].Value.ToString();
            this.Close();
            return ans;
        }

        /// <summary>
        /// Executes the command, after adding a parameter "ANS" for which it expects a return value for
        /// </summary>
        /// <returns>String ANS</returns>
        public string GetScalarResponse()
        {
            conn.Open();
            string ans = cmd.ExecuteScalar().ToString();
            this.Close();
            return ans;
        }



        /// <summary>
        /// Executes the command. 
        /// </summary>
        /// <returns>int Number of Rows affected</returns>
        public int ExecuteNonQuery()
        {
            conn.Open();
            string tmp = cmd.ToString();
            int rowsAffected = cmd.ExecuteNonQuery();
            this.Close();
            return rowsAffected;
        }

        void Close()
        {
            cmd.Connection.Close();
            cmd.Connection.Dispose();
            cmd.Dispose();
            conn.Close();
            conn.Dispose();
        }

        protected string GetCommandLogString(IDbCommand command)
        {
            string outputText;

            if (command.Parameters.Count == 0)
            {
                outputText = command.CommandText;
            }
            else
            {
                StringBuilder output = new StringBuilder();
                output.Append(command.CommandText);
                output.Append("; ");

                IDataParameter p;
                int count = command.Parameters.Count;
                for (int i = 0; i < count; i++)
                {
                    p = (IDataParameter)command.Parameters[i];
                    output.Append(string.Format("{0} = '{1}'", p.ParameterName, p.Value));

                    if (i + 1 < count)
                    {
                        output.Append(", ");
                    }
                }
                outputText = output.ToString();
            }
            return outputText;
        }

    }

}
