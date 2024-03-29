using SurplusMigrator.Models;
using System;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class MasterOccupation : _BaseTask {
        public MasterOccupation(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {};
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tablename = "master_occupation",
                    columns = new string[] {
                        "occupationid",
                        "name",
                        "departmentid",
                        "created_date",
                        "created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "occupationid" }
                }
            };
        }

        protected override void runDependencies() {
            new _Department(connections).run();
        }

        protected override MappedData getStaticData() {
            MappedData result = new MappedData();

            result.addData(
                "master_occupation",
                new RowData<ColumnName, object>() {
                    { "occupationid",  1},
                    { "name",  "Software Developer"},
                    { "departmentid",  "IT"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );

            return result;
        }
    }
}
