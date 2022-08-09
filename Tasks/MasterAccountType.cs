using Microsoft.Data.SqlClient;
using Npgsql;
using SurplusMigrator.Interfaces;
using SurplusMigrator.Tasks;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Models
{
  class MasterAccountType : _BaseTask {
        public MasterAccountType(DbConnection_[] connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "E_FRM").FirstOrDefault(),
                    tableName = "master_acctype",
                    columns = new string[] {
                        "acctype_id",
                        "acctype_name",
                        "acctypetype_id",
                        "acctype_isactive"
                    },
                    ids = new string[] { "acctypetype_id" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "insosys").FirstOrDefault(),
                    tableName = "master_account_type",
                    columns = new string[] {
                        "accounttypeid",
                        "name",
                        "accountsubtypeid",
                        "created_date",
                        //"created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "accounttypeid" }
                }
            };
        }

        public override List<RowData<ColumnName, Data>> getSourceData(Table[] sourceTables) {
            return sourceTables.Where(a => a.tableName == "master_acctype").FirstOrDefault().getDatas();
        }

        public override MappedData mapData(List<RowData<ColumnName, Data>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, Data> data in inputs) {
                RowData<ColumnName, Data> insertRow = new RowData<ColumnName, Data>() {
                    { "accounttypeid",  data["acctype_id"]},
                    { "name",  data["acctype_name"]},
                    { "accountsubtypeid",  data["acctypetype_id"]},
                    { "created_date",  DateTime.Now},
                    //{ "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", !Utils.obj2bool(data["acctype_isactive"]) }
                };
                result.addData("master_account_type", insertRow);
            }

            return result;
        }

        public override MappedData additionalStaticData() {
            MappedData result = new MappedData();

            result.addData(
                "master_account_type",
                new RowData<ColumnName, Data>() {
                    { "accounttypeid",  0},
                    { "name",  "Unknown"},
                    { "accountsubtypeid",  0},
                    { "created_date",  DateTime.Now},
                    //{ "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );

            return result;
        }
    }
}
