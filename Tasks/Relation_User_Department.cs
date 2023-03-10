using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class Relation_User_Department : _BaseTask {
        public Relation_User_Department(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tableName = "AspNetUsers",
                    columns = new string[] {
                        "nik",
                        "departmentid",
                    },
                    ids = new string[] { "nik" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tableName = "relation_user_department",
                    columns = new string[] {
                        "nik",
                        "departmentid",
                        "created_date",
                        "created_by",
                        "is_disabled",
                    },
                    ids = new string[] { "nik", "departmentid" }
                }
            };
        }

        protected override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            return sourceTables.Where(a => a.tableName == "AspNetUsers").FirstOrDefault().getData(batchSize);
        }

        public override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, object> data in inputs) {
                string departmentid = Utils.obj2str(data["departmentid"]);

                if(departmentid == null) continue;

                result.addData(
                    "relation_user_department",
                    new RowData<ColumnName, object>() {
                        { "nik",  data["nik"]},
                        { "departmentid",  data["departmentid"]},
                        { "created_date",  DateTime.Now},
                        { "created_by",  DefaultValues.CREATED_BY},
                        { "is_disabled", false}
                    }
                );
            }

            return result;
        }

        protected override void runDependencies() {
            new AspNetUsers(connections).run();
        }
    }
}
