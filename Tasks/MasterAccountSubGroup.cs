using Microsoft.Data.SqlClient;
using Npgsql;
using SurplusMigrator.Interfaces;
using SurplusMigrator.Tasks;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Models
{
  class MasterAccountSubGroup : _BaseTask {
        public MasterAccountSubGroup(DbConnection_[] connections) {
            source = new TableInfo() {
                connection = connections.Where(a => a.GetDbLoginInfo().dbname == "E_FRM").FirstOrDefault(),
                tableName = "master_accsubgroup",
                columns = new string[] { "accsubgroup_id", "accsubgroup_name", "accgroup_id" },
                ids = new string[] { "accsubgroup_id" }
            };
            destination = new TableInfo() {
                connection = connections.Where(a => a.GetDbLoginInfo().dbname == "insosys").FirstOrDefault(),
                tableName = "master_account_sub_group",
                columns = new string[] {
                    "accountsubgroupid", 
                    "name",
                    "accountgroupid",
                    "created_date",
                    //"created_by",
                    "is_disabled"
                },
                ids = new string[] { "accountsubgroupid" }
            };
        }

        public override List<RowData<string, object>> mapData(List<RowData<string, object>> inputs) {
            List<RowData<string, object>> result = new List<RowData<string, object>>();

            foreach(RowData<string, object> data in inputs) {
                RowData<string, object> insertRow = new RowData<string, object>() {
                    { "accountsubgroupid",  data["accsubgroup_id"]},
                    { "name",  data["accsubgroup_name"]},
                    { "accountgroupid",  data["accgroup_id"]},
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
