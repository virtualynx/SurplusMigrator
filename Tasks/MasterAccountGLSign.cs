using SurplusMigrator.Models;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class MasterAccountGLSign : _BaseTask {
        public MasterAccountGLSign(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
                    tableName = "master_acc_gl_sign",
                    columns = new string[] {
                        "acc_id",
                        "mark",
                    },
                    ids = new string[] { "acc_id" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tableName = "master_account_general_ledger_sign",
                    columns = new string[] {
                        "accountid",
                        "sign",
                        "is_disabled",
                    },
                    ids = new string[] { "accountid" }
                }
            };
        }

        protected override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            return sourceTables.Where(a => a.tableName == "master_acc_gl_sign").FirstOrDefault().getDatas(batchSize);
        }

        protected override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, object> data in inputs) {
                result.addData(
                    "master_account_general_ledger_sign",
                    new RowData<ColumnName, object>() {
                        { "accountid",  data["acc_id"]},
                        { "sign",  data["mark"]},
                        { "is_disabled",  false},
                    }
                );
            }

            return result;
        }

        protected override void runDependencies() {
            new MasterAccount(connections).run();
        }
    }
}
