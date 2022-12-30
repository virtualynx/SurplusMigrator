using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class MasterProjectType : _BaseTask {
        public MasterProjectType(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "E_FRM").FirstOrDefault(),
                    tableName = "master_projecttype",
                    columns = new string[] {
                        "projecttype_id",
                        "projecttype_name",
                        "projecttype_nameshort",
                        "projecttype_isactive",
                        "projecttype_entry_by",
                        "projecttype_entry_dt",
                    },
                    ids = new string[] { "projecttype_id" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "insosys").FirstOrDefault(),
                    tableName = "master_project_type",
                    columns = new string[] {
                        "projecttypeid",
                        "name",
                        "created_date",
                        "created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "projecttypeid" }
                }
            };
        }

        protected override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            return sourceTables.Where(a => a.tableName == "master_projecttype").FirstOrDefault().getDatas(batchSize);
        }

        protected override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, object> data in inputs) {
                result.addData(
                    "master_project_type",
                    new RowData<ColumnName, object>() {
                        { "projecttypeid",  data["projecttype_id"]},
                        { "name",  data["projecttype_name"]},
                        { "created_date",  data["projecttype_entry_dt"]},
                        { "created_by", getAuthInfo(data["projecttype_entry_by"]) },
                        { "is_disabled", !Utils.obj2bool(data["projecttype_isactive"]) }
                    }
                );
            }

            return result;
        }
    }
}
