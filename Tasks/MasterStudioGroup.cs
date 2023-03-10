using SurplusMigrator.Models;
using System;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class MasterStudioGroup : _BaseTask {
        public MasterStudioGroup(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tablename = "master_studio_group",
                    columns = new string[] {
                        "studiogroupid",
                        "name",
                        "created_by",
                        "created_date",
                        "is_disabled"
                    },
                    ids = new string[] { "studiogroupid" }
                }
            };
        }

        protected override MappedData getStaticData() {
            MappedData result = new MappedData();

            result.addData(
                "master_studio_group",
                new RowData<ColumnName, object>() {
                    { "studiogroupid", 1},
                    { "name", "Studio 9 & 11"},
                    { "created_by", DefaultValues.CREATED_BY},
                    { "created_date", DateTime.Now},
                    { "is_disabled", false},
                }
            );
            result.addData(
                "master_studio_group",
                new RowData<ColumnName, object>() {
                    { "studiogroupid", 2},
                    { "name", "Studio Trans7"},
                    { "created_by", DefaultValues.CREATED_BY},
                    { "created_date", DateTime.Now},
                    { "is_disabled", false},
                }
            );

            return result;
        }
    }
}
