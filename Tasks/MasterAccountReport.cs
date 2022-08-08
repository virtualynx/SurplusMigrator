using Microsoft.Data.SqlClient;
using Npgsql;
using SurplusMigrator.Interfaces;
using SurplusMigrator.Tasks;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Models
{
  class MasterAccountReport : _BaseTask {
        public MasterAccountReport(DbConnection_[] connections) {
            source = new TableInfo() {
                connection = connections.Where(a => a.GetDbLoginInfo().dbname == "E_FRM").FirstOrDefault(),
                tableName = "master_accrpt",
                columns = new string[] { "accrpt_id", "accrpt_name" },
                ids = new string[] { "accrpt_id" },
            };
            destination = new TableInfo() {
                connection = connections.Where(a => a.GetDbLoginInfo().dbname == "insosys").FirstOrDefault(),
                tableName = "master_account_report",
                columns = new string[] { 
                    "accountreporttypeid", 
                    "name",
                    "created_date",
                    "created_by",
                    "is_disabled" 
                },
                ids = new string[] { "accountreporttypeid" },
            };
        }

        public override List<RowData<string, object>> mapData(List<RowData<string, object>> inputs) {
            List<RowData<string, object>> result = new List<RowData<string, object>>();

            foreach(RowData<string, object> data in inputs) {
                RowData<string, object> insertRow = new RowData<string, object>() {
                    { "accountreporttypeid",  data["accrpt_id"]},
                    { "name",  data["accrpt_name"]},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                };
                result.Add(insertRow);
            }

            return result;
        }
    }
}
