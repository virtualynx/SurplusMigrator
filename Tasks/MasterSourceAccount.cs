using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class MasterSourceAccount : _BaseTask {
        public MasterSourceAccount(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
                    tableName = "master_source_acc",
                    columns = new string[] {
                        "source_id",
                        "acc_id",
                        "dk",
                    },
                    ids = new string[] { "source_id", "acc_id", "dk" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tableName = "master_source_account",
                    columns = new string[] {
                        "sourceaccountid",
                        "sourceid",
                        "accountid",
                        "dk",
                        "created_date",
                        "created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "sourceid", "accountid", "dk" }
                }
            };
        }

        protected override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            DateTime createdDate = DateTime.Now;
            foreach(RowData<ColumnName, object> data in inputs) {
                string msaId = SequencerString.getId("MSA", createdDate);

                result.addData(
                    "master_source_account",
                    new RowData<ColumnName, object>() {
                        { "sourceaccountid", msaId},
                        { "sourceid", data["source_id"]},
                        { "accountid", data["acc_id"]},
                        { "dk", data["dk"]},
                        { "created_date", createdDate},
                        { "created_by",  DefaultValues.CREATED_BY},
                        { "is_disabled", false }
                    }
                );
            }
            var targetConnection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").First();
            SequencerString.updateMasterSequencer(targetConnection, "MSA", createdDate);

            return result;
        }

        protected override void runDependencies() {
            new MasterSource(connections).run();
            new MasterAccount(connections).run();
        }
    }
}
