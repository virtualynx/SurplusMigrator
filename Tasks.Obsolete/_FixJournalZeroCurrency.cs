using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class _FixJournalZeroCurrency : _BaseTask {
        private DbConnection_ _connection;

        public _FixJournalZeroCurrency(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tablename = "transaction_journal_detail",
                    columns = new string[] {
                        "tjournal_detailid",
                        "tjournalid",
                        "dk",
                        "description",
                        "foreignamount",
                        "foreignrate",
                        "ref_detail_id",
                        "ref_subdetail_id",
                        "vendorid",
                        "accountid",
                        "currencyid",
                        "departmentid",
                        "tbudgetid",
                        "tbudget_detailid",
                        "ref_id",
                        "bilyet_no",
                        "bilyet_date",
                        "bilyet_effectivedate",
                        "received_by",
                        "created_date",
                        "created_by",
                        "is_disabled",
                        "disabled_date",
                        "disabled_by",
                        "modified_date",
                        "modified_by",
                        //"budgetdetail_name", removed
                        "idramount",
                        "bankaccountid",
                        "paymenttypeid",
                        "journalreferencetypeid",
                        "subreference_id",
                    },
                    ids = new string[] { "tjournal_detailid" }
                },
            };

            _connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").First();
        }

        protected override void onFinished() {
            int count = 0;
            int total = 0;
            RowData<string, object>[] data;
            string query;

            total = Utils.obj2int(
                QueryUtils.executeQuery(
                    _connection,
                    @"
                        select count(1)
                        from transaction_journal_detail
                        where 
                            currencyid = 0
                        ;
                    "
                ).First()["count"].ToString()
            );

            if(total > 0) {
                MyConsole.Information(
                    "Found @total journaldetail with zero currencyid ..."
                    .Replace("@total", total.ToString())
                );

                query = @"
                    select tjournal_detailid
                    from transaction_journal_detail
                    where 
                        currencyid = 0
                    limit 2500
                    ;
                ";

                try {
                    while((data = QueryUtils.executeQuery(_connection, query)).Length > 0) {
                        string[] tjournal_detailids = data.Select(a => Utils.obj2str(a["tjournal_detailid"])).ToArray();
                        QueryUtils.executeQuery(
                            _connection,
                            @"
                            update transaction_journal_detail
                            set currencyid = 1
                            where 
                                currencyid = 0
                                and tjournal_detailid in @tjournal_detailids
                        ",
                            new Dictionary<string, object>() {
                            { "@tjournal_detailids", tjournal_detailids },
                            }
                        );

                        count += data.Length;
                        MyConsole.EraseLine();
                        MyConsole.Write(
                            "@count/@total updated ..."
                            .Replace("@count", count.ToString())
                            .Replace("@total", total.ToString())
                        );
                    }
                    Console.WriteLine();
                } catch(Exception ex) {
                    MyConsole.Error(ex, ex.Message);
                }
            }
        }
    }
}
