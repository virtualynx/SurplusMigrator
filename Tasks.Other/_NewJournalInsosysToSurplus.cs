using Npgsql;
using SurplusMigrator.Interfaces;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Data.Entity.Core.Common.CommandTrees.ExpressionBuilder;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class _NewJournalInsosysToSurplus : _BaseTask, RemappableId {
        private string[] journalIds = null;
        private string filter = null;

        public _NewJournalInsosysToSurplus(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
                    tablename = "transaksi_jurnal",
                    columns = new string[] {
                        "jurnal_id",
                        "jurnal_bookdate",
                        "jurnal_duedate",
                        "jurnal_billdate",
                        "jurnal_descr",
                        "jurnal_invoice_id",
                        "jurnal_invoice_descr",
                        "jurnal_source",
                        "jurnaltype_id",
                        "rekanan_id",
                        "periode_id",
                        //"channel_id",
                        "budget_id",
                        "currency_id",
                        "currency_rate",
                        "strukturunit_id",
                        "acc_ca_id",
                        //"region_id",
                        //"branch_id",
                        "advertiser_id",
                        "brand_id",
                        "ae_id",
                        //"jurnal_iscreated",
                        //"jurnal_iscreatedby",
                        //"jurnal_iscreatedate",
                        "jurnal_isposted",
                        "jurnal_ispostedby",
                        "jurnal_isposteddate",
                        "jurnal_isdisabled",
                        "jurnal_isdisabledby",
                        "jurnal_isdisableddt",
                        "created_by",
                        "created_dt",
                        "modified_by",
                        "modified_dt",
                    },
                    ids = new string[] { "jurnal_id" }
                },
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
                    tablename = "transaksi_jurnaldetil",
                    columns = new string[] {
                        "jurnal_id",
                        "jurnaldetil_line",
                        "jurnaldetil_dk",
                        "jurnaldetil_descr",
                        "rekanan_id",
                        //"rekanan_name",
                        "acc_id",
                        "currency_id",
                        "jurnaldetil_foreign",
                        "jurnaldetil_foreignrate",
                        "jurnaldetil_idr",
                        //"channel_id",
                        "strukturunit_id",
                        "ref_id",
                        "ref_line",
                        "ref_budgetline",
                        //"region_id",
                        //"branch_id",
                        "budget_id",
                        //"budget_name",
                        "budgetdetil_id",
                        //"budgetdetil_name",
                    },
                    ids = new string[] { "jurnal_id", "jurnaldetil_line" }
                },
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
                    tablename = "transaksi_salesorder",
                    columns = new string[] {
                        "salesorder_id",
                        "salesorder_agency",
                        "salesorder_agency_addr",
                        "salesorder_ext_ref",
                        "salesorder_ext_ref2",
                        "salesorder_dt",
                        "salesorder_advertiser",
                        "salesorder_brand",
                        //"salesorder_product",
                        "salesorder_order_month",
                        "salesorder_bill_dt",
                        "salesorder_book_dt",
                        "salesorder_due",
                        "salesorder_ae",
                        "salesorder_recv_dt",
                        "salesorder_recv_by",
                        "salesorder_currency",
                        "salesorder_rate",
                        "salesorder_amount",
                        //"salesorder_amountidr",
                        "salesorder_amount_add",
                        "salesorder_amount_cancel",
                        "salesorder_comm",
                        "salesorder_buyer",
                        "salesorder_area",
                        "salesorder_traffic_id",
                        "salesorder_format_inv",
                        //"salesorder_format_log",
                        //"salesorder_invoice_reference",
                        "salesorder_ply_inv",
                        //"salesorder_ply_log",
                        "salesorder_inv_type",
                        "salesorder_direct",
                        "salesorder_descr",
                        "salesorder_entry_dt",
                        "salesorder_entry_by",
                        "salesorder_account",
                        "salesorder_mo_avail",
                        "salesorder_mo_add",
                        "salesorder_mo_memo",
                        "salesorder_mo_canc",
                        "salesorder_mo_date",
                        //"salesorder_pulled",v
                        //"salesorder_modifyby",
                        //"salesorder_modifydt",
                        //"channel_id",
                        "salesorder_isokay",
                        "salesorder_iscanceled",
                        "salesorder_jurnaltypeid"
                    },
                    ids = new string[] { "salesorder_id" }
                },
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
                    tablename = "transaksi_jurnalreference",
                    columns = new string[] {
                        "jurnal_id",
                        "jurnaldetil_line",
                        "jurnal_id_ref",
                        "jurnal_id_refline",
                        "jurnal_id_budgetline",
                        "referencetype"
                    },
                    ids = new string[] { }
                },
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
                    tablename = "transaksi_jurnal_tax",
                    columns = new string[] {
                        "jurnaltax_id",
                        "jurnaltax_fakturid",
                        "jurnaltax_date",
                        "jurnaltax_currency",
                        "jurnaltax_rate",
                        "jurnaltax_format",
                        "jurnaltax_pic",
                        "jurnaltax_jabatan",
                        "jurnaltax_hargajual",
                        "jurnaltax_discount",
                        "jurnaltax_uangmuka",
                        "jurnaltax_dasarpengenaan",
                        "jurnaltax_ppn",
                        "channel_id"
                    },
                    ids = new string[] { "jurnaltax_id" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tablename = "transaction_journal",
                    columns = new string[] {
                        "tjournalid",
                        "bookdate",
                        "duedate",
                        "billdate",
                        "description",
                        "invoiceid",
                        "invoicedescription",
                        "sourceid",
                        "currencyid",
                        "foreignrate",
                        "accountexecutive_nik",
                        "transactiontypeid",
                        "vendorid",
                        "periodid",
                        "tbudgetid",
                        "departmentid",
                        "accountcaid",
                        "advertiserid",
                        "advertiserbrandid",
                        "paymenttypeid",
                        "created_date",
                        "created_by",
                        "is_disabled",
                        "disabled_by",
                        "disabled_date",
                        "modified_by",
                        "modified_date",
                        "is_posted",
                        "posted_by",
                        "posted_date",
                        "journaltypeid"
                    },
                    ids = new string[] { "tjournalid" }
                },
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
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tablename = "transaction_sales_order",
                    columns = new string[] {
                        "tsalesorderid",
                        "vendorid",
                        "vendorbillid",
                        "mediaordernumber",
                        "jobid",
                        "date",
                        "advertiserid",
                        "advertiserbrandid",
                        "periodedate",
                        "billdate",
                        "bookdate",
                        "due",
                        "accountexecutive_nik",
                        "receivedby_nik",
                        "receiveddate",
                        "currencyid",
                        "foreignamount",
                        "foreignrate",
                        "additionalamount",
                        "cancelationamount",
                        "commision",
                        "buyer",
                        "salesareaid",
                        "contractnumber",
                        "invoiceformatid",
                        "invoiceply",
                        "invoicetypeid",
                        "isdirect",
                        "description",
                        "accountid",
                        "mo",
                        "moadd",
                        "momemo",
                        "mocancelation",
                        "modate",
                        "isapproved",
                        "approvedby",
                        "approveddate",
                        "transactiontypeid",
                        "created_date",
                        "created_by",
                        //"disabled_date",
                        "is_disabled",
                        //"disabled_by",
                        //"modified_date",
                        //"modified_by"
                    },
                    ids = new string[] { "tsalesorderid" }
                },
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tablename = "transaction_journal_tax",
                    columns = new string[] {
                        "tjournaltaxid",
                        "tjournalid",
                        "fakturid",
                        "date",
                        "currencyid",
                        "ppnamount",
                        "format",
                        "pic",
                        "position",
                        "sellprice",
                        "discount",
                        "downpayment",
                        "dasarpengenaan",
                        "ppnrate",
                        "created_date",
                        "created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "tjournaltaxid" }
                }
            };
        }

        protected override List<RowData<string, object>> getSourceData(Table[] sourceTables, int batchSize = 5000) {
            return new List<RowData<ColumnName, object>>();
        }

        protected override void runDependencies() {
            new _Department(connections).run();
            new MasterAccountCa(connections).run();
            //new _MasterAdvertiser(connections).run();
            //new _MasterAdvertiserBrand(connections).run();
            new MasterCurrency(connections).run();
            new MasterPaymentType(connections).run();
            new MasterPeriod(connections).run();
            new MasterTransactionTypeGroup(connections).run();
            new MasterTransactionType(connections).run();
            new MasterSource(connections).run();
            new MasterVendorCategory(connections).run();
            new MasterVendorType(connections).run();
            new MasterVendor(connections).run();
            new TransactionBudget(connections).run(true);

            new MasterBankAccount(connections).run();
            new MasterJournalReferenceType(connections).run();
            new TransactionBudgetDetail(connections).run(true);

            new MasterVendor(connections).run();
            new MasterCurrency(connections).run();
            new MasterAccount(connections).run();
            new MasterTransactionType(connections).run();
            new MasterInvoiceFormat(connections).run();
            new MasterInvoiceType(connections).run();

            //var surplusConn = connections.Where(a => a.GetDbLoginInfo().name == "surplus").First();
            //QueryUtils.toggleTrigger(surplusConn, "master_vendor_bill", false);
            //new MasterVendorBill(connections).run(true, new TaskTruncateOption() { truncateBeforeInsert = true });
            //QueryUtils.toggleTrigger(surplusConn, "master_vendor_bill", true);
        }

        public void clearRemappingCache() {
            IdRemapper.clearMapping("advertiserid");
            IdRemapper.clearMapping("advertiserbrandid");
        }

        protected override void onFinished() {
            loadConfig();
            string logFilename = "log_(" + this.GetType().Name + ")_" + _startedAt.ToString("yyyyMMdd_HHmmss") + ".json";

            var newJurnals = getNewJurnalFromInsosys();
            var journalIds = newJurnals.Select(a => a["jurnal_id"].ToString()).ToList();
            var surplusConn = connections.Where(a => a.GetDbLoginInfo().name == "surplus").First();
            NpgsqlTransaction transaction = ((NpgsqlConnection)surplusConn.GetDbConnection()).BeginTransaction();

            QueryUtils.toggleTrigger(surplusConn, "transaction_journal", false);
            QueryUtils.toggleTrigger(surplusConn, "transaction_journal_detail", false);
            try {
                if(newJurnals.Length > 0) {
                    insertJournal(newJurnals, transaction);
                    Utils.saveJson(logFilename, newJurnals.Select(a => Utils.obj2str(a["jurnal_id"])).ToArray());
                    transaction.Commit();
                }
            } catch(Exception) {
                transaction.Rollback();
                throw;
            } finally {
                transaction.Dispose();
                QueryUtils.toggleTrigger(surplusConn, "transaction_journal", true);
                QueryUtils.toggleTrigger(surplusConn, "transaction_journal_detail", true);
                IdRemapper.saveMap(); //saving mapped advertiser and brand triggered by Gen21Integration, and also vendorbill
            }
        }

        private void loadConfig() {
            if(getOptions("journalids") != null) {
                List<string> listOfJournal = new List<string>();
                var journalOptSplit = getOptions("journalids").Split(",");
                foreach(var journalId in journalOptSplit) {
                    if(journalId.Trim().Length > 0) {
                        listOfJournal.Add(journalId.Trim());
                    }
                }
                journalIds = listOfJournal.Count > 0 ? listOfJournal.ToArray() : null;
            }

            if(getOptions("filters") != null) {
                filter = getOptions("filters");
            }
        }

        private RowData<ColumnName, object>[] getNewJurnalFromInsosys() {
            var insosysConn = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault();
            var surplusConn = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault();

            List<RowData<ColumnName, object>> migratedJurnalInsosys = new List<RowData<ColumnName, object>>();

            Table tableJurnal = new Table(sources.First(a => a.tablename == "transaksi_jurnal"));
            if(journalIds != null) {
                string whereIn = "jurnal_id in @jurnal_ids".Replace("@jurnal_ids", QueryUtils.getInsertArg(journalIds));
                List<RowData<ColumnName, object>> batchData;
                while((batchData = tableJurnal.getData(250, whereIn)).Count > 0) {
                    migratedJurnalInsosys.AddRange(getNewDataOnly(batchData));
                }
            } else if(filter != null) {
                List<RowData<ColumnName, object>> batchData;
                while((batchData = tableJurnal.getData(2500, filter)).Count > 0) {
                    migratedJurnalInsosys.AddRange(getNewDataOnly(batchData));
                }
            } else {
                List<RowData<ColumnName, object>> batchDataInsosys;
                while((batchDataInsosys = tableJurnal.getData(2500)).Count > 0) {
                    migratedJurnalInsosys.AddRange(getNewDataOnly(batchDataInsosys));
                }
            }

            return migratedJurnalInsosys.ToArray();
        }

        private RowData<ColumnName, object>[] getNewDataOnly(List<RowData<ColumnName, object>> inputs) {
            List<RowData<ColumnName, object>> result = new List<RowData<string, object>>();

            Table tableJournal = new Table(destinations.First(a => a.tablename == "transaction_journal"));
            int readBatchSize = 250;
            for(int a=0; a<inputs.Count; a+=readBatchSize) {
                var batchDataInsosys = inputs.Skip(a).Take(readBatchSize).ToArray();

                string[] insosysJournalIds = batchDataInsosys.Select(a => Utils.obj2str(a["jurnal_id"]).ToUpper()).Distinct().ToArray();

                string whereInClause = "tjournalid in @jurnal_ids".Replace("@jurnal_ids", QueryUtils.getInsertArg(insosysJournalIds));
                
                List<RowData<ColumnName, object>> dataSurplus = tableJournal.getAllData(whereInClause, 5000, true, false);
                var new_jurnalids = insosysJournalIds.Where(jurnal_id => !dataSurplus.Any(surplusData => Utils.obj2str(surplusData["tjournalid"]) == jurnal_id)).ToList();
                if(new_jurnalids.Count > 0) {
                    result.AddRange(batchDataInsosys.Where(a => new_jurnalids.Contains(Utils.obj2str(a["jurnal_id"]))).ToList());
                }
            }

            return result.ToArray();
        }

        private RowData<ColumnName, object>[] getNewJurnalDetailFromInsosys(RowData<ColumnName, object>[] newJurnals) {
            var insosysConn = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault();

            var newJurnalDetails = new List<RowData<ColumnName, object>>();
            int jurnalDetailBatchSize = 100;
            for(int a = 0; a < newJurnals.Length; a += jurnalDetailBatchSize) {
                var batchJurnal = newJurnals.Skip(a).Take(jurnalDetailBatchSize).ToArray();
                var jurnalids = new List<string>();
                foreach(var jurnal in batchJurnal) {
                    string jurnal_id = Utils.obj2str(jurnal["jurnal_id"]);
                    if(!jurnalids.Contains(jurnal_id)) {
                        jurnalids.Add(jurnal_id);
                    }
                }

                string query = @"
                    select
                        <columns>
                    from
                        [<schema>].transaksi_jurnaldetil
                    where
                        jurnal_id in @jurnalids
                ";
                string[] selectColumns = sources.Where(a => a.tablename == "transaksi_jurnaldetil").First().columns;
                query = query.Replace("<columns>", "[" + String.Join("],[", selectColumns) + "]");
                query = query.Replace("<schema>", insosysConn.GetDbLoginInfo().schema);
                query = query.Replace("@jurnalids", QueryUtils.getInsertArg(jurnalids));
                var rs = QueryUtils.executeQuery(insosysConn, query);

                newJurnalDetails.AddRange(rs);
            }

            return newJurnalDetails.ToArray();
        }

        private void insertJournal(RowData<ColumnName, object>[] newJournals, NpgsqlTransaction transaction) {
            int insertBatchSize = 250;
            int insertedJournal = 0;

            TransactionJournal tjournalTask = new TransactionJournal(connections);
            TransactionJournalDetail tjournalDetailTask = new TransactionJournalDetail(connections);
            TransactionSalesOrder tsalesOrderTask = new TransactionSalesOrder(connections);

            for(int a = 0; a < newJournals.Length; a += insertBatchSize) {
                var batchJurnals = newJournals.Skip(a).Take(insertBatchSize).ToArray();

                Table tableJournal = new Table(destinations.First(a => a.tablename == "transaction_journal"));
                Table tableJournalDetail = new Table(destinations.First(a => a.tablename == "transaction_journal_detail"));

                var mappedJournalData = tjournalTask.mapData(batchJurnals.ToList()).getData("transaction_journal");
                tableJournal.insertData(mappedJournalData, transaction);
                Console.WriteLine();

                var saList = batchJurnals
                    .Where(a => Utils.obj2str(a["jurnal_id"]).ToUpper().StartsWith("SA"))
                    .Select(a => Utils.obj2str(a["jurnal_id"]).ToUpper()).ToArray();
                if(saList.Length > 0) {
                    Table tableJurnalReference = new Table(sources.First(a => a.tablename == "transaksi_jurnalreference"));
                    string whereInJurnalRef = "jurnal_id in @jurnal_ids".Replace("@jurnal_ids", QueryUtils.getInsertArg(saList));
                    var soIdList = tableJurnalReference.getAllData(whereInJurnalRef)
                        .Where(a => Utils.obj2str(a["jurnal_id_ref"]).ToUpper().StartsWith("SO"))
                        .Select(a => Utils.obj2str(a["jurnal_id_ref"]).ToUpper())
                        .Distinct()
                        .ToArray();

                    if(soIdList.Length > 0) {
                        Table tableSoSource = new Table(sources.First(a => a.tablename == "transaksi_salesorder"));
                        string whereInSo = "salesorder_id in @salesorder_ids".Replace("@salesorder_ids", QueryUtils.getInsertArg(soIdList));
                        var soDatas = tableSoSource.getAllData(whereInSo);
                        if(soDatas.Count > 0) {
                            Table tableSoDest = new Table(destinations.First(a => a.tablename == "transaction_sales_order"));
                            var mappedSoData = tsalesOrderTask.mapData(soDatas).getData("transaction_sales_order");
                            tableSoDest.insertData(mappedSoData, transaction);
                            Console.WriteLine();
                        }
                    }

                    Table tableJurnalTaxSource = new Table(sources.First(a => a.tablename == "transaksi_jurnal_tax"));
                    string whereInJurnalTax = "jurnaltax_id in @jurnal_ids".Replace("@jurnal_ids", QueryUtils.getInsertArg(saList));
                    var jurnalTaxList = tableJurnalTaxSource.getAllData(whereInJurnalTax);
                    if(jurnalTaxList.Count > 0) {
                        Table tableJournalTaxDest = new Table(destinations.First(a => a.tablename == "transaction_journal_tax"));
                        var mappedJournalTaxData = tsalesOrderTask.mapData(jurnalTaxList).getData("transaction_journal_tax");
                        tableJournalTaxDest.insertData(mappedJournalTaxData, transaction);
                        Console.WriteLine();
                    }
                }

                var batchJurnalDetails = getNewJurnalDetailFromInsosys(batchJurnals);
                if(batchJurnalDetails.Length > 0) {
                    var mappedJournalDetailData = tjournalDetailTask.mapData(batchJurnalDetails.ToList()).getData("transaction_journal_detail");
                    tableJournalDetail.insertData(mappedJournalDetailData, transaction);
                    Console.WriteLine();
                }

                insertedJournal += batchJurnals.Length;
            }
        }
    }
}
