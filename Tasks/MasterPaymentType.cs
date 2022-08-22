using Microsoft.Data.SqlClient;
using Npgsql;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using SurplusMigrator.Tasks;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
  class MasterPaymentType : _BaseTask {
        public MasterPaymentType(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "E_FRM").FirstOrDefault(),
                    tableName = "master_paymenttype",
                    columns = new string[] {
                        "paymenttype_id",
                        "paymenttype_name",
                    },
                    ids = new string[] { "paymenttype_id" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "insosys").FirstOrDefault(),
                    tableName = "master_payment_type",
                    columns = new string[] {
                        "paymenttypeid",
                        "name",
                        "is_disabled",
                    },
                    ids = new string[] { "paymenttypeid" }
                }
            };
        }

        public override List<RowData<ColumnName, Data>> getSourceData(Table[] sourceTables, int batchSize = 5000) {
            return sourceTables.Where(a => a.tableName == "master_paymenttype").FirstOrDefault().getDatas(batchSize);
        }

        public override MappedData mapData(List<RowData<ColumnName, Data>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, Data> data in inputs) {
                if(Utils.obj2int(data["paymenttype_id"]) == 0) continue;
                RowData<ColumnName, Data> insertRow = new RowData<ColumnName, Data>() {
                    { "paymenttypeid",  Int32.Parse(data["paymenttype_id"].ToString())},
                    { "name",  data["paymenttype_name"]},
                    { "is_disabled",  false},
                };
                result.addData("master_payment_type", insertRow);
            }

            return result;
        }

        public override MappedData additionalStaticData() {
            MappedData result = new MappedData();

            result.addData(
                "master_payment_type",
                new RowData<ColumnName, Data>() {
                    { "paymenttypeid",  0},
                    { "name",  "Unknown"},
                    { "is_disabled",  false},
                }
            );

            return result;
        }

        public override void runDependencies() {
        }
    }
}
