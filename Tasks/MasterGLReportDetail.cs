using SurplusMigrator.Interfaces;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class MasterGLReportDetail : _BaseTask, RemappableId {
        public MasterGLReportDetail(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "E_FRM").FirstOrDefault(),
                    tableName = "master_gl_report_row",
                    columns = new string[] {
                        "code",
                        "row",
                        "seq",
                        "descr",
                        "remark",
                        "fbold",
                        "fline",
                        "fitalic",
                    },
                    ids = new string[] { "code", "row" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "insosys").FirstOrDefault(),
                    tableName = "master_glreport_detail",
                    columns = new string[] {
                        "glreportdetailid",
                        "glreportid",
                        "sequence",
                        "description",
                        "isbold",
                        "isitalic",
                        "isunderline",
                    },
                    ids = new string[] { "glreportdetailid" }
                }
            };
        }

        protected override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            return sourceTables.Where(a => a.tableName == "master_gl_report_row").FirstOrDefault().getDatas(batchSize, false);
        }

        protected override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, object> data in inputs) {
                string dummy = SequencerString.getId("DUMMY_GLRD", DateTime.Now).Substring("DUMMY_GLRD".Length + "yyMMdd".Length);
                int glreportdetailid = Utils.obj2int(dummy);
                string codeAndRowTag = data["code"].ToString() + "_" + data["row"].ToString();
                IdRemapper.add("glreportdetailid", codeAndRowTag, glreportdetailid);

                int glreportid = Utils.obj2int(data["code"]);
                int sequence = Utils.obj2int(data["seq"]);
                if(glreportid == 3 && sequence == 480 && Utils.obj2str(data["descr"]) == "LABA (RUGI) SEBELUM PAJAK") {
                    sequence = 481;
                }

                result.addData(
                    "master_glreport_detail",
                    new RowData<ColumnName, object>() {
                        { "glreportdetailid",  glreportdetailid},
                        { "glreportid",  glreportid},
                        { "sequence",  sequence},
                        { "description",  data["descr"]},
                        { "isbold",  Utils.obj2bool(data["fbold"])},
                        { "isitalic",  Utils.obj2bool(data["fitalic"])},
                        { "isunderline",  Utils.obj2bool(data["fline"])},
                    }
                );
            }

            return result;
        }

        protected override void afterFinishedCallback() {
            IdRemapper.saveMap();
        }

        public void clearRemappingCache() {
            IdRemapper.clearMapping("glreportdetailid");
        }
    }
}
