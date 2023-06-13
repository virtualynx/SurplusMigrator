using Microsoft.EntityFrameworkCore.Diagnostics;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class _RemapVendorJournalBymhd : _BaseTask {
        private DbConnection_ _connection;

        public _RemapVendorJournalBymhd(DbConnection_[] connections) : base(connections) {
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
            var bymhdAccounts = getAccruedBymhdAccounts();

            MyConsole.WriteLine("Get list of accrued-transaction-details ...");
            var accruedTransactionDetails = getAccruedTransactionDetails(bymhdAccounts);
            MyConsole.Information("Found(@count) list of accrued-transaction-details".Replace("@count", accruedTransactionDetails.Length.ToString()));

            const int batchSize = 2500;
            int counter = 0;
            int processedCount = 0;
            RowData<string, object>[] data;

            var trx = _connection.GetDbConnection().BeginTransaction();

            try {
                while((data = accruedTransactionDetails.Skip(counter * batchSize).Take(batchSize).ToArray()).Length > 0) {
                    var tjournal_detailids = data.Select(a => a["tjournal_detailid"].ToString()).ToArray();

                    QueryUtils.executeQuery(
                        _connection,
                        @"
                            update transaction_journal_detail
                            set vendorid = @vendorid
                            where 
                                tjournal_detailid in @tjournal_detailids
                                and vendorid <> @vendorid
                            ;
                        ",
                        new Dictionary<string, object> {
                            { "@tjournal_detailids", tjournal_detailids },
                            { "@vendorid", 927 }
                        },
                        trx
                    );

                    counter++;
                    processedCount += data.Length;
                    MyConsole.EraseLine();
                    MyConsole.Write(
                        "@count/@total data processed ..."
                        .Replace("@count", processedCount.ToString())
                        .Replace("@total", accruedTransactionDetails.Length.ToString())
                    );
                }
                Console.WriteLine();
                trx.Commit();
            } catch (Exception) {
                trx.Rollback();
                throw;
            }
        }

        private string[] getAccruedBymhdAccounts() {
            var result = QueryUtils.executeQuery(
                _connection,
                @"
                    select distinct 
	                    account_bymhd_id 
                    from 
	                    transaction_budget_detail tbd 
                    where 
	                    isaccrued = true
                    ;
                "
            );

            return result.Select(a => Utils.obj2str(a["account_bymhd_id"])).ToArray();
        }

        private RowData<string, object>[] getAccruedTransactionDetails(string[] bymhdAccounts) {
            DateTime fromDate = DateTime.MinValue;
            DateTime toDate = DateTime.MaxValue;

            if(getOptions("from") != null) {
                fromDate = DateTime.ParseExact(getOptions("from"), "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
            }
            if(getOptions("to") != null) {
                toDate = DateTime.ParseExact(getOptions("to"), "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
            }

            var result = QueryUtils.executeQuery(
                _connection,
                @"
                    select 
	                    tjd.tjournal_detailid
                    from 
	                    transaction_journal tj 
	                    join transaction_journal_detail tjd on tjd.tjournalid = tj.tjournalid 
                    where 
                        left(tjd.tjournalid, 2) = 'AP'
	                    and dk = 'D'
                        and accountid in @bymhdAccounts
	                    and tj.is_disabled = false and tjd.is_disabled = false
                        and (tj.bookdate between @from and @to)
	                    --and tjd.tjournalid in ('AP23020900078', 'AP23022800112')
                    ;
                ",
                new Dictionary<string, object> {
                    { "@bymhdAccounts", bymhdAccounts },
                    { "@from", fromDate.ToString("yyyy-MM-dd") + " 00:00:00" },
                    { "@to", toDate.ToString("yyyy-MM-dd") + " 23:59:59" }
                }
            );

            return result;
        }
    }
}
