using Microsoft.Data.SqlClient;
using Npgsql;
using NpgsqlTypes;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text.RegularExpressions;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory.Database;

using ParamNotation = System.String;

namespace SurplusMigrator.Models
{
  class DbConnection_ {
        private DbConnection conn;
        private DbLoginInfo loginInfo;

        public DbConnection_(DbLoginInfo loginInfo) {
            this.loginInfo = loginInfo;
            if(loginInfo.type == DbTypes.MSSQL) {
                conn = new SqlConnection("Data Source=" + loginInfo.host + "," + loginInfo.port + ";Initial Catalog=" + loginInfo.dbname + ";User ID=" + loginInfo.username + ";Password=" + loginInfo.password);
            }else if(loginInfo.type == DbTypes.POSTGRESQL) {
                conn = new NpgsqlConnection("Server=" + loginInfo.host + ";Port=" + loginInfo.port + ";User ID=" + loginInfo.username + ";Password=" + loginInfo.password + ";Database=" + loginInfo.dbname + ";SearchPath=" + loginInfo.schema + ";Include Error Detail=true");
            }
        }
        ~DbConnection_() {
            if(conn.State == ConnectionState.Open) {
                conn.Close();
            }
        }

        public DbConnection GetDbConnection() {
            if(conn.State != ConnectionState.Open) {
                conn.Open();
            }
            return conn;
        }

        public DbLoginInfo GetDbLoginInfo() { 
            return loginInfo;
        }

        public void closeConnection() { 
            conn.Close();
        }
    }
}
