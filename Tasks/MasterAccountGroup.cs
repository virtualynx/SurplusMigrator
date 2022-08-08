using Microsoft.Data.SqlClient;
using Npgsql;
using SurplusMigrator.Interfaces;
using SurplusMigrator.Tasks;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Models
{
  class MasterAccountGroup : _BaseTask {
        public MasterAccountGroup(DbConnection_[] connections) {
            source = new TableInfo() {
                connection = connections.Where(a => a.GetDbLoginInfo().dbname == "E_FRM").FirstOrDefault(),
                tableName = "master_accgroup",
                columns = new string[] { "accgroup_id", "accgroup_name", "accgroup_position" , "accrpt_id" },
                ids = new string[] { "accgroup_id" }
            };
            destination = new TableInfo() {
                connection = connections.Where(a => a.GetDbLoginInfo().dbname == "insosys").FirstOrDefault(),
                tableName = "master_account_group",
                columns = new string[] { 
                    "accountgroupid", 
                    "name", 
                    "position",
                    "accountreporttypeid",
                    "created_date",
                    "created_by",
                    "is_disabled"
                },
                ids = new string[] { "accountgroupid" }
            };
        }

        public override List<RowData<string, object>> mapData(List<RowData<string, object>> inputs) {
            List<RowData<string, object>> result = new List<RowData<string, object>>();

            foreach(RowData<string, object> data in inputs) {
                RowData<string, object> insertRow = new RowData<string, object>() {
                    { "accountgroupid",  data["accgroup_id"]},
                    { "name",  data["accgroup_name"]},
                    { "position",  data["accgroup_position"]},
                    { "accountreporttypeid",  data["accrpt_id"]},
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
