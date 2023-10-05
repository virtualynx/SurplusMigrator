using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class _FixPayableAgingResponseVendor : _BaseTask {
        private DbConnection_ _connection;

        public _FixPayableAgingResponseVendor(DbConnection_[] connections) : base(connections) {
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
            var excelDatas = getDataFromExcel();

            List<string> tjournalidSources = new List<string>();

            //int rownum = 0;
            //while(rownum < excelDatas.Length) {
            //    string row_source = excelDatas[rownum]["row_source"].ToString();

            //    int nextRownum = rownum;
            //    do {
            //        nextRownum++;
            //    } while(nextRownum < excelDatas.Length && !isJournalId(excelDatas[nextRownum]["row_source"].ToString()));

            //    if((nextRownum - 1) - rownum > 1) {
            //        tjournalidSources.Add(row_source);
            //    }

            //    rownum = nextRownum;
            //}

            tjournalidSources = excelDatas.Select(a => a["row_source"].ToString()).ToList();

            //tjournalidSources = tjournalidSources.Where(a =>
            //    (new string[] {
            //        "AP0220010098"
            //    }).Contains(a)
            //).ToList();

            var journalDetailResponses = QueryUtils.executeQuery(
                _connection,
                @"
                    select 
                        tjd.tjournal_detailid ,
			            tjd.ref_detail_id
		            from 
			            transaction_journal_detail tjd 
			            join master_vendor mv on mv.vendorid = tjd.vendorid 
		            where 
			            tjd.ref_id in @tjournalid_sources 
			            and accountid in (
				            select 
					            distinct account_bymhd_id 
				            from 
					            transaction_budget_detail tbd 
				            where 
					            isaccrued = true
			            )
			            and mv.name = 'Trans 7'
                    ;
                ",
                new Dictionary<string, object> {
                    { "@tjournalid_sources", tjournalidSources.ToArray() },
                }
            );

            var tjournalDetailIdSources = journalDetailResponses.Select(a => Utils.obj2str(a["ref_detail_id"])).Distinct().ToArray();

            if(tjournalDetailIdSources.Length == 0) {
                MyConsole.Information("No data to fix");
                return;
            }

            var journalDetailSources = QueryUtils.executeQuery(
                _connection,
                @"
                    select 
			            tjournal_detailid ,
			            vendorid
		            from 
			            transaction_journal_detail 
		            where 
			            tjournal_detailid in @tjournaldetailid_sources
			            and accountid in (
				            select 
					            distinct account_bymhd_id 
				            from 
					            transaction_budget_detail tbd 
				            where 
					            isaccrued = true
			            )
                    ;
                ",
                new Dictionary<string, object> {
                    { "@tjournaldetailid_sources", tjournalDetailIdSources.ToArray() },
                }
            );

            var tjournalDetailIdResponses = journalDetailResponses.Select(a => Utils.obj2str(a["tjournal_detailid"])).Distinct().ToArray();

            var trx = _connection.GetDbConnection().BeginTransaction();
            try {
                foreach(var row in journalDetailSources) {
                    string ref_detail_id = Utils.obj2str(row["tjournal_detailid"]);
                    int vendorid = Utils.obj2int(row["vendorid"]);

                    QueryUtils.executeQuery(
                        _connection,
                        @"
                            update transaction_journal_detail
                            set vendorid = @vendorid
                            where 
                                ref_detail_id = @ref_detail_id
                                and tjournal_detailid in @tjournaldetailid_responses
                            ;
                        ",
                        new Dictionary<string, object> {
                            { "@vendorid", vendorid },
                            { "@ref_detail_id", ref_detail_id },
                            { "@tjournaldetailid_responses", tjournalDetailIdResponses }
                        },
                        trx
                    );

                    MyConsole.Information(
                        "Updated info for ref_detail_id: @ref_detail_id, set vendorid into @vendorid"
                        .Replace("@ref_detail_id", ref_detail_id)
                        .Replace("@vendorid", vendorid.ToString())
                    );
                }
                trx.Commit();
            } catch(Exception) {
                trx.Rollback();
                throw;
            }
        }

        private RowData<ColumnName, object>[] getDataFromExcel() {
            ExcelColumn[] columns = new ExcelColumn[] {
                new ExcelColumn(){ name="row_source", ordinal=0 },
                new ExcelColumn(){ name="sum", ordinal=1 },
            };

            return Utils.getDataFromExcel("FixPayableAgingResponseVendor-August2023-3.xlsx", columns).ToArray();
        }

        private bool isJournalId(string str) {
            if(str == null || str.Length <= 2) return false;
            return double.TryParse(str.Substring(2), out _);
        }
    }
}
