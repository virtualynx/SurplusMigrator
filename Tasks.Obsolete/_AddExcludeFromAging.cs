using Npgsql;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class _AddExcludeFromAging : _BaseTask {
        private DbConnection_ targetConnection;

        public _AddExcludeFromAging(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
            };
            destinations = new TableInfo[] {
            };

            targetConnection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").First();
            Console.WriteLine("\n");
            MyConsole.Information("Add exclude from aging (schema " + targetConnection.GetDbLoginInfo().schema + ")");
            Console.WriteLine();
            Console.Write("Continue performing job (Y/N)? ");
            string choice = Console.ReadLine();
            if(choice.ToLower() != "y") {
                throw new Exceptions.JobAbortedException();
            }
        }

        protected override void onFinished() {
            var excelDatas = getDataFromExcel();

            string[] accountids = new string[] { "2015018" };

            string tjdQuery = @"
                select 
	                tjd.tjournalid,
	                tjd.tjournal_detailid,
                    tjd.idramount
                from 
	                transaction_journal_detail tjd 
	                join transaction_journal tj on tj.tjournalid = tjd.tjournalid 
                where
	                accountid in @accountids
                    and tjd.tjournalid in @tjournalids
                    and tjd.is_disabled = false and tj.is_disabled = false
                    and tjd.tjournal_detailid not in (
                        select tjournal_detailid from transaction_journal_excluded_fromaging where is_disabled = false
                    )
                ;
            ";
            
            var tjournalids = excelDatas.Select(a => Utils.obj2str(a["tjournalid"])).ToArray();

            var tjdRs = QueryUtils.executeQuery(targetConnection, tjdQuery, new Dictionary<string, object> {
                { "@accountids", accountids },
                { "@tjournalids", tjournalids }
            });

            //var excludedJournalIds = new List<RowData<string, object>>();
            var newExcludes = new List<RowData<string, object>>();
            DateTime createdDate = DateTime.Now;
            foreach(var row in tjdRs) {
                string tjournalid = Utils.obj2str(row["tjournalid"]);
                string tjournal_detailid = Utils.obj2str(row["tjournal_detailid"]);

                //decimal journalValue = Math.Abs(Utils.obj2decimal(row["sum"]));
                decimal journalValue = 0;

                var excelData = excelDatas.FirstOrDefault(a => Utils.obj2str(a["tjournalid"]) == tjournalid);
                //decimal excelValue = Math.Abs(Utils.obj2decimal(excelData["aging_amount"]));
                decimal excelValue = 0;

                try {
                    //journalValue = (int)Utils.obj2decimal(row["idramount"]);
                    journalValue = Decimal.Parse((Utils.obj2decimal(row["idramount"]).ToString().Where(Char.IsDigit).ToArray().AsSpan()));
                    if(Utils.obj2decimal(row["idramount"]) < 0) {
                        journalValue *= -1;
                    }
                } catch(Exception e) {

                }

                try {
                    //excelValue = (int)Utils.obj2decimal(excelData["aging_amount"]);
                    excelValue = Decimal.Parse((Utils.obj2decimal(excelData["aging_amount"]).ToString().Where(Char.IsDigit).ToArray().AsSpan()));
                    if(Utils.obj2decimal(excelData["aging_amount"]) < 0) {
                        excelValue *= -1;
                    }
                } catch(Exception e) { 
                
                }

                if(journalValue == excelValue) {
                    //excludedJournalIds.Add(new RowData<string, object> {
                    //    { "tjournal_detailid", tjournal_detailid },
                    //    { "idramount", row["idramount"] }
                    //});
                    newExcludes.Add(
                        new RowData<string, object> {
                        { "tjournal_detailid", row["tjournal_detailid"] },
                        { "created_date", createdDate },
                        { "created_by", DefaultValues.CREATED_BY },
                        { "is_disabled", false },
                        }
                    );
                }
            }

            Table excludeAgingTable = new Table() {
                connection = targetConnection,
                tablename = "transaction_journal_excluded_fromaging",
                columns = new string[] {
                        "tjournal_detailid",
                        "created_date",
                        "created_by",
                        "is_disabled",
                    },
                ids = new string[] {
                        "tjournal_detailid"
                    },
            };

            NpgsqlTransaction transaction = ((NpgsqlConnection)targetConnection.GetDbConnection()).BeginTransaction();
            try {
                excludeAgingTable.insertData(newExcludes, transaction);
                transaction.Commit();
            } catch(Exception e) {
                transaction.Rollback();
                throw;
            }
        }

        private RowData<ColumnName, object>[] getDataFromExcel() {
            ExcelColumn[] columns = new ExcelColumn[] {
                new ExcelColumn(){ name="tjournalid", ordinal=0 },
                new ExcelColumn(){ name="aging_amount", ordinal=7 },
            };

            return Utils.getDataFromExcel("add_exclude_fromaging.xlsx", columns).ToArray();
        }
    }
}
