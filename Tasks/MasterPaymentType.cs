using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class MasterPaymentType : _BaseTask {
        public MasterPaymentType(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
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
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
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

        protected override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            return sourceTables.Where(a => a.tableName == "master_paymenttype").FirstOrDefault().getData(batchSize);
        }

        protected override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, object> data in inputs) {
                if(Utils.obj2int(data["paymenttype_id"]) == 0) continue;
                RowData<ColumnName, object> insertRow = new RowData<ColumnName, object>() {
                    { "paymenttypeid",  Int32.Parse(data["paymenttype_id"].ToString())},
                    { "name",  data["paymenttype_name"]},
                    { "is_disabled",  false},
                };
                result.addData("master_payment_type", insertRow);
            }

            return result;
        }

        protected override MappedData getStaticData() {
            MappedData result = new MappedData();

            result.addData(
                "master_payment_type",
                new RowData<ColumnName, object>() {
                    { "paymenttypeid",  0},
                    { "name",  "Unknown"},
                    { "is_disabled",  false},
                }
            );

            return result;
        }
    }
}
