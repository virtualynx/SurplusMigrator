using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Npgsql;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using Table = SurplusMigrator.Models.Table;

namespace SurplusMigrator.Tasks {
    class _FixPaymentPlanning : _BaseTask {
        private DbConnection_ targetConnection;

        public _FixPaymentPlanning(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
            };
            destinations = new TableInfo[] {
            };

            targetConnection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").First();
            Console.WriteLine("\n");
            Console.Write("Continue performing fix-payment-planning on schema "+ targetConnection.GetDbLoginInfo().schema + " (Y/N)? ");
            string choice = Console.ReadLine();
            if(choice.ToLower() != "y") {
                throw new Exceptions.JobAbortedException();
            }
        }

        protected override void onFinished() {
            string departmentid = getOptions("departmentid").ToString();
            string[] pvFilters = new string[] { };

            if(getOptions("pvs") != null) {
                string[] pvArr = getOptions("pvs").Split(",");
                pvArr = pvArr.Distinct().ToArray();
                pvFilters = (from pv in pvArr select Utils.obj2str(pv)).Where(a => a != null).ToArray();
            }

            var sourceData = getExcelData();
            var excelHeaderPv = getExcelHeaderPv(pvFilters);

            //filter-out sourceData with empty accountid_advance
            var emptyAccountIdAdvanceSourceData = sourceData.Where(a => a["accountid_advance"] == null).ToArray();
            var emptyAccountIdAdvanceVqd = (from row in emptyAccountIdAdvanceSourceData select row["tadvance_detailid"].ToString()).ToArray();
            sourceData = sourceData.Where(a => a["accountid_advance"] != null).ToArray();
            if(emptyAccountIdAdvanceSourceData.Length > 0) {
                MyConsole.Warning("Empty accountid_advance: " + String.Join(",", emptyAccountIdAdvanceVqd));
            }

            //filter-out settled pv
            List<string> settledPvIds = new List<string>();
            RowData<string, object> unsettledExcelHeaderPv = new RowData<string, object>();
            foreach(var map in excelHeaderPv) {
                string pvId = map.Value.ToString();
                if(isPvAlreadySettled(pvId)) {
                    settledPvIds.Add(pvId);
                } else {
                    unsettledExcelHeaderPv[map.Key] = map.Value;
                }
            }
            excelHeaderPv = unsettledExcelHeaderPv;
            if(settledPvIds.Count > 0) {
                MyConsole.Warning("PV already settled: " + String.Join(",", settledPvIds));
            }

            MyConsole.WriteLine("-----------------------", false);

            //cleansing idr zero values
            foreach(var map in excelHeaderPv) {
                string pvSourceIdx = map.Key;
                string pvId = map.Value.ToString();

                foreach(var row in sourceData) {
                    try {
                        Utils.obj2decimal(row[pvSourceIdx]);
                    } catch(Exception e) {
                        if(e.Message == "Input string was not in a correct format.") {
                            row[pvSourceIdx] = (decimal)0;
                        }
                    }
                }
            }

            Table ppdTable = new Table() {
                connection = targetConnection,
                tableName = "transaction_payment_planning_detail",
                columns = new string[] {
                    "id",
                    "payment_planning_id",
                    "ref_id",
                    "amount_foreign",
                    "amount_foreign_rate",
                    "currencyid",
                    //"invoice_number",
                    "invoice_date",
                    "account_ca_id",
                    "media",
                    "created_date",
                    "created_by",
                    //"disabled_date",
                    "is_disabled",
                    //"disabled_by",
                    //"modified_date",
                    //"modified_by",
                    "accountid",
                    "description",
                    "vendorid",
                    "ref_detail_id"
                },
                ids = new string[] {
                    "id",
                    //"payment_planning_id",
                    //"ref_detail_id"
                },
            };
            Table pvdTable = new Table() {
                connection = targetConnection,
                tableName = "transaction_journal_detail",
                columns = new string[] {
                    "tjournal_detailid",
                    "tjournalid",
                    "dk",
                    "description",
                    "foreignamount",
                    "foreignrate",
                    "ref_detail_id",
                    "vendorid",
                    "accountid",
                    "currencyid",
                    "departmentid",
                    "tbudgetid",
                    "tbudget_detailid",
                    "ref_id",
                    //"bilyet_no",
                    //"bilyet_date",
                    //"bilyet_effectivedate",
                    //"received_by",
                    //"bankaccountid",
                    //"paymenttypeid",
                    "idramount",
                    "journalreferencetypeid",
                    "subreference_id",
                    "created_date",
                    "created_by",
                    //"disabled_date",
                    "is_disabled",
                    //"disabled_by",
                    //"modified_date",
                    //"modified_by",
                    "ref_subdetail_id"
                },
                ids = new string[] {
                    "tjournal_detailid",
                    //"tjournalid",
                    //"subreference_id"
                },
            };
            List<string> ppdIdsTobeDeleted = new List<string>();
            List<string> pvdIdsTobeDeleted = new List<string>();

            foreach(var map in excelHeaderPv) {
                string pvSourceIdx = map.Key;
                string pvId = map.Value.ToString();

                string pvdQuery = @"
                        select
                            *
                        from
                            transaction_journal_detail
                        where
                            tjournalid = @pvid
                            and subreference_id is not null
                            and is_disabled = false
                    ";
                //pvdQuery = pvdQuery.Replace("<schema>", targetConnection.GetDbLoginInfo().schema);
                var pvdRs = QueryUtils.executeQuery(targetConnection, pvdQuery, new Dictionary<string, object> {
                        { "@pvid", pvId }
                    });
                var ppdIds = (from rowPvdRs in pvdRs select Utils.obj2str(rowPvdRs["subreference_id"]))
                    .Where(a => a != null)
                    .Distinct()
                    .ToArray();
                ppdIdsTobeDeleted.AddRange(ppdIds);

                string ppdQuery = @"
                        select
                            *
                        from
                            transaction_payment_planning_detail
                        where
                            id in @ppdids
                            and is_disabled = false
                    ";
                //ppdQuery = ppdQuery.Replace("<schema>", targetConnection.GetDbLoginInfo().schema);
                var ppdRs = QueryUtils.executeQuery(targetConnection, ppdQuery, new Dictionary<string, object> {
                        { "@ppdids", ppdIds }
                    });

                if(ppdRs.Length == 0) {
                    throw new Exception("No data found in [transaction_payment_planning_detail] for PV: "+pvId);
                }

                string ppid = ppdRs[0]["payment_planning_id"].ToString();
                string account_ca_id = (from rowPpdRs in ppdRs select rowPpdRs["account_ca_id"].ToString()).First();
                string media = (from rowPpdRs in ppdRs select Utils.obj2str(rowPpdRs["media"])).First();
                DateTime invoiceDate = Utils.obj2datetime(ppdRs[0]["invoice_date"]);

                NpgsqlTransaction transaction = ((NpgsqlConnection)targetConnection.GetDbConnection()).BeginTransaction();
                try {
                    List<RowData<ColumnName, object>> newPpdDatas = new List<RowData<string, object>>();
                    Dictionary<string, Dictionary<string, string>> ppdBudgetMap = new Dictionary<string, Dictionary<string, string>>();
                    var pvSourceData = sourceData.Where(a => Utils.obj2decimal(a[pvSourceIdx]) != 0).ToArray();
                    DateTime now = DateTime.Now;
                    foreach(var rowSource in pvSourceData) {
                        string ppdId = SequencerString.getId(targetConnection, "PPD", now);
                        decimal amountForeign = Utils.obj2decimal(rowSource[pvSourceIdx].ToString());
                        newPpdDatas.Add(new RowData<string, object>() {
                            { "id", ppdId },
                            { "payment_planning_id", ppid },
                            { "ref_id", rowSource["tadvanceid"] },
                            { "amount_foreign", amountForeign },
                            { "amount_foreign_rate", (decimal)1 },
                            { "currencyid", 1 },
                            { "invoice_date", invoiceDate },
                            { "account_ca_id", account_ca_id },
                            { "media", media },
                            { "created_date", now },
                            { "created_by", DefaultValues.CREATED_BY },
                            { "is_disabled", false },
                            { "accountid", rowSource["accountid_advance"] },
                            { "description", rowSource["description"] },
                            { "vendorid", rowSource["vendorid"] },
                            { "ref_detail_id", rowSource["tadvance_detailid"] },
                        });
                        ppdBudgetMap[ppdId] = new Dictionary<string, string>() {
                            { "tbudgetid", Utils.obj2str(rowSource["tbudgetid"]) },
                            { "tbudget_detailid", Utils.obj2str(rowSource["tbudget_detailid"]) },
                        };
                    }
                    ppdTable.insertData(newPpdDatas, transaction, false);
                    SequencerString.updateMasterSequencer(targetConnection, "PPD", now, transaction);
                    MyConsole.Information(newPpdDatas.Count + " data inserted into [transaction_payment_planning_detail] for PV: " + pvId);

                    //get old pvd data
                    string advanceid = Utils.obj2str(newPpdDatas[0]["ref_id"]);
                    if(advanceid == null) {
                        throw new Exception("New PPD data has null advanceid");
                    }
                    string pvdQuerySelect = @"
                        select
                            tjournal_detailid
                        from
                            transaction_journal_detail
                        where
                            tjournalid = @pvid
                            and ref_id = @advanceid
                            and is_disabled = false
                    ";
                    var pvdRsSelect = QueryUtils.executeQuery(targetConnection, pvdQuerySelect, new Dictionary<string, object> {
                        { "@pvid", pvId },
                        { "@advanceid", advanceid }
                    });
                    pvdIdsTobeDeleted.AddRange((from row in pvdRsSelect select row["tjournal_detailid"].ToString()).ToList());

                    now = DateTime.Now;
                    List<RowData<ColumnName, object>> newPvdDatas = new List<RowData<string, object>>();
                    foreach(var ppd in newPpdDatas) {
                        string ppdId = Utils.obj2str(ppd["id"]);
                        newPvdDatas.Add(new RowData<string, object>() {
                            { "tjournal_detailid", SequencerString.getId(targetConnection, "PVD", now) },
                            { "tjournalid", pvId },
                            { "dk", "D" },
                            { "description", ppd["description"] },
                            { "foreignamount", ppd["amount_foreign"] },
                            { "foreignrate", ppd["amount_foreign_rate"] },
                            { "ref_detail_id", ppd["ref_detail_id"] },
                            { "vendorid", ppd["vendorid"] },
                            { "accountid", ppd["accountid"] },
                            { "currencyid", ppd["currencyid"] },
                            { "departmentid", departmentid },
                            { "tbudgetid", ppdBudgetMap[ppdId]["tbudgetid"] },
                            { "tbudget_detailid", ppdBudgetMap[ppdId]["tbudget_detailid"] },
                            { "ref_id", ppd["ref_id"] },
                            { "idramount", ppd["amount_foreign"] },
                            { "subreference_id", ppdId },
                            { "created_date", now },
                            { "created_by", DefaultValues.CREATED_BY },
                            { "is_disabled", false },
                            { "ref_subdetail_id", 0 },
                        });
                    }
                    pvdTable.insertData(newPvdDatas, transaction, false);
                    SequencerString.updateMasterSequencer(targetConnection, "PVD", now, transaction);
                    MyConsole.Information(newPpdDatas.Count + " data inserted into [transaction_journal_detail] for PV: " + pvId);
                    Console.WriteLine();
                    transaction.Commit();
                } catch(Exception) {
                    transaction.Rollback();
                    throw;
                }
            }

            //delete old pvd data
            string pvdQueryDelete = @"
                    delete from
                        transaction_journal_detail
                    where
                        tjournal_detailid in @pvids
                ";
            var pvdRsDelete = QueryUtils.executeQuery(targetConnection, pvdQueryDelete,
                new Dictionary<string, object> {
                    { "@pvids", pvdIdsTobeDeleted.ToArray() },
                });

            //delete old ppd data
            string ppdQueryDelete = @"
                    delete from
                        transaction_payment_planning_detail
                    where
                        id in @ppdids
                ";
            var ppdRsDelete = QueryUtils.executeQuery(targetConnection, ppdQueryDelete,
                new Dictionary<string, object> {
                    { "@ppdids", ppdIdsTobeDeleted.ToArray() }
                });
        }

        /**
         * -----------------------------------------------------------
         */
        private ExcelColumn[] _namedColumns = new ExcelColumn[] {
            new ExcelColumn(){ name="tadvanceid", ordinal=0 },
            new ExcelColumn(){ name="tadvance_detailid", ordinal=1 },
            new ExcelColumn(){ name="description", ordinal=2 },
            new ExcelColumn(){ name="tbudgetid", ordinal=3 },
            new ExcelColumn(){ name="tbudget_detailid", ordinal=4 },
            new ExcelColumn(){ name="accountid_advance", ordinal=5 },
            new ExcelColumn(){ name="vendorid", ordinal=6 },
            new ExcelColumn(){ name="budget_detail", ordinal=7 },
            new ExcelColumn(){ name="total_advance", ordinal=8 }
        };
        private RowData<ColumnName, object>[] _excelData;
        private Dictionary<string, object> getExcelHeader() {
            if(_excelData == null) {
                getExcelData();
            }

            return _excelData.Where(row => Utils.obj2str(row["tadvanceid"]) == "tadvanceid").First();
        }
        private RowData<ColumnName, object>[] getExcelData() {
            if(_excelData == null) {
                string excelFilename = getOptions("excel");
                string sheetName = null;
                if(getOptions("sheet") != null) {
                    sheetName = getOptions("sheet");
                }
                _excelData = Utils.getDataFromExcel(excelFilename, _namedColumns, sheetName).ToArray();

                //correcting kdata-type
                foreach(var row in _excelData) {
                    string tadvanceid = Utils.obj2str(row["tadvanceid"]);
                    if(tadvanceid == null || !tadvanceid.StartsWith("VQ")) continue;
                    string accountid_advance = Utils.obj2str(row["accountid_advance"]);
                    row["accountid_advance"] = accountid_advance;
                    if(accountid_advance != null) {
                        row["accountid_advance"] = Utils.obj2int(accountid_advance.Replace(",", ""));
                    }
                    row["vendorid"] = Utils.obj2int(row["vendorid"].ToString().Replace(",", ""));
                }
            }

            return _excelData.Where(a => 
                a["tadvanceid"].ToString().StartsWith("VQ")
                && Utils.obj2str(a["tadvanceid"]) != null
            ).ToArray();
        }
        private RowData<ColumnName, object> getExcelHeaderPv(string[] pvFilters) {
            RowData<ColumnName, object> result = new RowData<string, object>();

            var excelHeader = getExcelHeader();
            foreach(var map in excelHeader) {
                int idx;
                if(
                    Int32.TryParse(map.Key, out idx) 
                    && idx > 8 
                    && Utils.obj2str(map.Value) != null
                    && Utils.obj2str(map.Value).ToUpper().StartsWith("PV")
                ) {
                    string pvId = Utils.obj2str(map.Value);
                    if(pvFilters.Length > 0) {
                        if(pvFilters.Contains(pvId)) {
                            result[map.Key] = pvId;
                        }
                    } else {
                        result[map.Key] = pvId;
                    }
                }
            }

            return result;
        }
        private bool isPvAlreadySettled(string pvId) {
            string stQuerySelect = @"
                    select *
                    from
                        transaction_settlement_detail
                    where
                        pv_id = @pvid
                        and is_disabled = false
                ";
            var rs = QueryUtils.executeQuery(targetConnection, stQuerySelect,
                new Dictionary<string, object> {
                    { "@pvid", pvId }
                });

            return rs.Length > 0;
        }
    }
}
