using Microsoft.Data.SqlClient;
using Npgsql;
using SurplusMigrator.Interfaces;
using SurplusMigrator.Tasks;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Models
{
  class MasterAccountSubType : _BaseTask {
        public MasterAccountSubType(DbConnection_[] connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "E_FRM").FirstOrDefault(),
                    tableName = "master_acctypetype",
                    columns = new string[] { 
                        "acctypetype_id", 
                        "acctypetype_name" 
                    },
                    ids = new string[] { "acctypetype_id" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "insosys").FirstOrDefault(),
                    tableName = "master_account_sub_type",
                    columns = new string[] {
                        "accountsubtypeid",
                        "name",
                        "created_date",
                        //"created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "accountsubtypeid" }
                }
            };
        }

        public override List<RowData<ColumnName, Data>> getSourceData(Table[] sourceTables) {
            return sourceTables.Where(a => a.tableName == "master_acctypetype").FirstOrDefault().getDatas();
        }

        public override MappedData mapData(List<RowData<ColumnName, Data>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, Data> data in inputs) {
                RowData<ColumnName, Data> insertRow = new RowData<ColumnName, Data>() {
                    { "accountsubtypeid",  data["acctypetype_id"]},
                    { "name",  data["acctypetype_name"]},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                };
                result.addData("master_account_sub_type", insertRow);
            }

            return result;
        }

        public override MappedData additionalStaticData() {
            MappedData result = new MappedData();

            result.addData(
                "master_account_sub_type",
                new RowData<ColumnName, Data>() {
                    { "accountsubtypeid",  0},
                    { "name",  "Unknown"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );

            return result;
        }
    }
}
