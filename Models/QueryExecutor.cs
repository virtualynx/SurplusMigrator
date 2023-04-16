using Microsoft.Data.SqlClient;
using Npgsql;
using SurplusMigrator.Libraries;
using System;
using System.IO;

namespace SurplusMigrator.Models {
    class QueryExecutor {
        private DbConnection_ _connection;
        public string tableName;
        public string[] columns;
        public string[] ids;

        public QueryExecutor(DbConnection_ connection) {
            this._connection = connection;
        }

        public void execute(string path) {
            FileAttributes attr = File.GetAttributes(path);

            if((attr & FileAttributes.Directory) == FileAttributes.Directory) {
                string[] files = Directory.GetFiles(path, "*.sql", SearchOption.AllDirectories);
                Array.Sort(files);
                foreach(string filename in files) {
                    execute(filename);
                }
            } else {
                runQuery(path);
            }
        }

        private int runQuery(string path) {
            int result = 0;
            string sql = File.ReadAllText(path);
            DateTime startAt = DateTime.Now;
            TimeSpan elapsed;
            MyConsole.Write("Executing " + path + " ... ");
            try {
                if(_connection.GetDbLoginInfo().type == DbTypes.MSSQL) {
                    sql = sql.Replace("<schema>", "["+_connection.GetDbLoginInfo().schema+"]");
                    SqlCommand command = new SqlCommand(sql, (SqlConnection)_connection.GetDbConnection());
                    command.CommandTimeout = 0;
                    result = command.ExecuteNonQuery();
                    command.Dispose();
                } else if(_connection.GetDbLoginInfo().type == DbTypes.POSTGRESQL) {
                    sql = sql.Replace("<schema>", "\"" + _connection.GetDbLoginInfo().schema + "\"");
                    NpgsqlCommand command = new NpgsqlCommand(sql, (NpgsqlConnection)_connection.GetDbConnection());
                    command.CommandTimeout = 0;
                    result = command.ExecuteNonQuery();
                    command.Dispose();
                }
                elapsed = DateTime.Now - startAt;
                MyConsole.WriteLine("Success("+elapsed+").", false);
            } catch(PostgresException e) {
                elapsed = DateTime.Now - startAt;
                MyConsole.WriteLine("", false);
                if(e.Message.Contains("duplicate key value violates unique constraint") && e.Message.Contains("already exists")) {
                    MyConsole.Warning("Skipping file "+ path + ", is already executed.");
                } else {
                    MyConsole.Error("Error(" + elapsed + "): " + e.Message, false);
                    throw;
                }
            } catch(Exception e) {
                elapsed = DateTime.Now - startAt;
                MyConsole.WriteLine("", false);
                MyConsole.Error(e, "Error("+elapsed+"): " + e.Message, false);
                throw;
            }

            return result;
        }
    }
}
