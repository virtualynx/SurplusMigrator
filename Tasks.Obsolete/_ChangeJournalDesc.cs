using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class _ChangeJournalDesc : _BaseTask {
        private DbConnection_ targetConnection;

        private const int DEFAULT_BATCH_SIZE = 500;

        public _ChangeJournalDesc(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
            };
            destinations = new TableInfo[] {
            };

            targetConnection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").First();
            Console.WriteLine("\n");
            MyConsole.Information("Journal Description will be changed on table transaction_journal (schema " + targetConnection.GetDbLoginInfo().schema + ")");
            Console.WriteLine();
            Console.Write("Continue performing fix (Y/N)? ");
            string choice = Console.ReadLine();
            if(choice.ToLower() != "y") {
                throw new Exceptions.JobAbortedException();
            }
        }

        protected override void onFinished() {
            var newDescriptions = getDataFromExcel();

            newDescriptions = newDescriptions.Skip(1).ToArray(); //removes headers

            foreach(var newDesc in newDescriptions) {
                string tjournalid = Utils.obj2str(newDesc["tjournalid"]);
                string description = Utils.obj2str(newDesc["Ganti Desc"]);

                try {
                    string queryUpdate = @"
                            update transaction_journal 
                            set description = @newdesc
                            where tjournalid = @tjournalid;
                        "
                    ;

                    var updateResult = QueryUtils.executeQuery(
                        targetConnection,
                        queryUpdate,
                        new Dictionary<string, object> {
                            { "@tjournalid", tjournalid },
                            { "@newdesc", description }
                        }
                    );

                    MyConsole.Information(
                        "Desctiption on journal [<tjournalid>] has been updated ... "
                        .Replace("<tjournalid>", tjournalid)
                    );
                } catch(Exception) {
                    throw;
                }
            }
        }

        private RowData<ColumnName, object>[] getDataFromExcel() {
            ExcelColumn[] columns = new ExcelColumn[] {
                new ExcelColumn(){ name="tjournalid", ordinal=0 },
                new ExcelColumn(){ name="Ganti Desc", ordinal=2 },
            };

            return Utils.getDataFromExcel("Ganti descripsi.xlsx", columns).ToArray();
        }
    }
}
