using Microsoft.Data.SqlClient;
using Npgsql;
using SurplusMigrator.Interfaces;
using SurplusMigrator.Tasks;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Models
{
  class MasterAccountCa : _BaseTask {
        public MasterAccountCa(DbConnection_[] connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "E_FRM").FirstOrDefault(),
                    tableName = "master_acc_ca",
                    columns = new string[] {
                        "acc_ca_id",
                        "acc_ca_name",
                        "acc_ca_shortname",
                        "acc_ca_type",
                        "acc_ca_mother",
                        "acc_ca_idx",
                        "acc_ca_active",
                        "acc_ca_entry_dt",
                        "acc_ca_entry_by",
                    },
                    ids = new string[] { "acc_ca_id" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "insosys").FirstOrDefault(),
                    tableName = "master_account_ca",
                    columns = new string[] {
                        "accountcaid",
                        "name",
                        "shortname",
                        "type",
                        "parent",
                        "idx",
                        "created_date",
                        "is_disabled",
                    },
                    ids = new string[] { "accountid" }
                }
            };
        }

        public override List<RowData<ColumnName, Data>> getSourceData(Table[] sourceTables) {
            return sourceTables.Where(a => a.tableName == "master_acc_ca").FirstOrDefault().getDatas();
        }

        public override MappedData mapData(List<RowData<ColumnName, Data>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, Data> data in inputs) {
                RowData<ColumnName, Data> insertRow = new RowData<ColumnName, Data>() {
                    { "accountcaid",  data["acc_ca_id"]},
                    { "name",  data["acc_ca_name"]},
                    { "shortname",  data["acc_ca_shortname"]},
                    { "type",  data["acc_ca_type"]},
                    { "parent",  data["acc_ca_mother"]!=null? data["acc_ca_mother"]: 0},
                    { "idx",  data["acc_ca_idx"]},
                    { "created_date",  data["acc_ca_entry_dt"]},
                    { "is_disabled",  !Utils.obj2bool(data["acc_ca_active"])},
                };
                result.addData("master_account_ca", insertRow);
            }

            return result;
        }

        public override MappedData additionalStaticData() {
            return new MappedData();
        }
    }
}
