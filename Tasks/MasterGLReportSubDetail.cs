using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class MasterGLReportSubDetail : _BaseTask {
        public MasterGLReportSubDetail(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
                    tableName = "master_gl_report_row_acc",
                    columns = new string[] {
                        "code",
                        "row",
                        "line",
                        "descr",
                        "ac_start",
                        "ac_end",
                        "sign",
                        //"ytd",
                    },
                    ids = new string[] { "code", "row", "line" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tableName = "master_glreport_subdetail",
                    columns = new string[] {
                        "glreportsubdetailid",
                        "glreportdetailid",
                        "description",
                        "accountid_start",
                        "accountid_end",
                        "sign",
                    },
                    ids = new string[] { "glreportsubdetailid" }
                }
            };
        }

        protected override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            return sourceTables.Where(a => a.tableName == "master_gl_report_row_acc").FirstOrDefault().getDatas(batchSize);
        }

        protected override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, object> data in inputs) {
                string dummy = SequencerString.getId("DUMMY_GLRSD", DateTime.Now).Substring("DUMMY_GLRSD".Length + "yyMMdd".Length);
                int glreportsubdetailid = Utils.obj2int(dummy);

                string codeAndRowTag = data["code"].ToString() + "_" + data["row"].ToString();
                int glreportdetailid = IdRemapper.get("glreportdetailid", codeAndRowTag);

                result.addData(
                    "master_glreport_subdetail",
                    new RowData<ColumnName, object>() {
                        { "glreportsubdetailid",  glreportsubdetailid},
                        { "glreportdetailid",  glreportdetailid},
                        { "description",  data["descr"]},
                        { "accountid_start",  Utils.obj2str(data["ac_start"])},
                        { "accountid_end",  Utils.obj2str(data["ac_end"])},
                        { "sign",  Utils.obj2long(data["sign"])},
                    }
                );
            }

            return result;
        }
    }
}
