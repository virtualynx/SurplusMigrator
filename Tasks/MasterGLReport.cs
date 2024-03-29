using SurplusMigrator.Models;
using System.Collections.Generic;
using System.Linq;
using SurplusMigrator.Libraries;

namespace SurplusMigrator.Tasks {
    class MasterGLReport : _BaseTask {
        public MasterGLReport(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
                    tablename = "master_gl_report_row_h",
                    columns = new string[] {
                        "Code",
                        "Name",
                        "Create_by",
                        "Create_dt",
                    },
                    ids = new string[] { "Code" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tablename = "master_glreport",
                    columns = new string[] {
                        "glreportid",
                        "name",
                        "created_date",
                        "created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "glreportid" }
                }
            };
        }

        protected override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            return sourceTables.Where(a => a.tablename == "master_gl_report_row_h").FirstOrDefault().getData(batchSize);
        }

        public override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, object> data in inputs) {
                //string glreportid = "GLR"+Utils.obj2int(data["Code"]).ToString().PadLeft(5, '0');
                //IdRemapper.add("glreportid", data["Code"], glreportid);

                int glreportid = Utils.obj2int(data["Code"]);
                result.addData(
                    "master_glreport",
                    new RowData<ColumnName, object>() {
                        { "glreportid",  glreportid},
                        { "name",  data["Name"]},
                        { "created_date",  data["Create_dt"]},
                        { "created_by", getAuthInfo(data["Create_by"], true) },
                        { "is_disabled", false }
                    }
                );
            }

            return result;
        }

        //protected override void onFinished() {
        //    IdRemapper.saveMap();
        //}

        //public void clearRemappingCache() {
        //    IdRemapper.clearMapping("glreportid");
        //}
    }
}
