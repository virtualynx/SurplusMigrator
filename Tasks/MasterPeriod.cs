using Microsoft.Data.SqlClient;
using Npgsql;
using SurplusMigrator.Interfaces;
using SurplusMigrator.Tasks;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Models
{
  class MasterPeriod : _BaseTask {
        public MasterPeriod(DbConnection_[] connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "E_FRM").FirstOrDefault(),
                    tableName = "master_periode",
                    columns = new string[] {
                        "periode_id",
                        "periode_name",
                        "periode_datestart",
                        "periode_dateend",
                        "periode_isclosed",
                        "periode_createby",
                        "periode_createdate",
                        "periode_allowsaldoawalentry",
                    },
                    ids = new string[] { "periode_id" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "insosys").FirstOrDefault(),
                    tableName = "master_period",
                    columns = new string[] {
                        "periodid",
                        "name",
                        "datestart",
                        "dateend",
                        "isclosed",
                        "allowsaldoawalentry",
                        //"closeddate",
                        //"closedby",
                        "created_date",
                        "created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "periodid" }
                }
            };
        }

        public override List<RowData<ColumnName, Data>> getSourceData(Table[] sourceTables, int batchSize = 5000) {
            return sourceTables.Where(a => a.tableName == "master_periode").FirstOrDefault().getDatas(batchSize);
        }

        public override MappedData mapData(List<RowData<ColumnName, Data>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, Data> data in inputs) {
                AuthInfo auth = new AuthInfo() {
                    FullName = data["periode_createby"]!=null? data["periode_createby"].ToString() : null
                };

                RowData<ColumnName, Data> insertRow = new RowData<ColumnName, Data>() {
                    { "periodid",  data["periode_id"]},
                    { "name",  data["periode_name"]},
                    { "datestart",  data["periode_datestart"]},
                    { "dateend",  data["periode_dateend"]},
                    { "isclosed",  Utils.obj2bool(data["periode_isclosed"])},
                    { "allowsaldoawalentry",  Utils.obj2bool(data["periode_allowsaldoawalentry"])},
                    { "created_date",  data["periode_createdate"]},
                    { "created_by",  auth},
                    { "is_disabled", false }
                };
                result.addData("master_period", insertRow);
            }

            return result;
        }

        public override MappedData additionalStaticData() {
            return new MappedData();
        }
    }
}
