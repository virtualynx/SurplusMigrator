using Microsoft.Data.SqlClient;
using Npgsql;
using SurplusMigrator.Exceptions.Gen21;
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
                    tableName = "transaksi_jurnal",
                    columns = new string[] {
                        "jurnal_id",
                        "jurnal_bookdate",
                        "jurnal_duedate",
                        "jurnal_billdate",
                        "jurnal_descr",
                        "jurnal_invoice_id",
                        "jurnal_invoice_descr",
                        "jurnal_source",
                        //"jurnaltype_id",
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
                    tableName = "transaksi_jurnaldetil",
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
                    tableName = "transaksi_salesorder",
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
                    tableName = "transaksi_jurnalreference",
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
                    tableName = "transaksi_jurnal_tax",
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
                    tableName = "transaction_journal",
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
                    },
                    ids = new string[] { "tjournalid" }
                },
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tableName = "transaction_journal_detail",
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
                    tableName = "transaction_sales_order",
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
                    tableName = "transaction_journal_tax",
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
            new MasterVendorBill(connections).run();
        }

        public void clearRemappingCache() {
            IdRemapper.clearMapping("advertiserid");
            IdRemapper.clearMapping("advertiserbrandid");
            IdRemapper.clearMapping("vendorbillid");
        }

        protected override void onFinished() {
            loadConfig();
            string logFilename = "log_(" + this.GetType().Name + ")_" + _startedAt.ToString("yyyyMMdd_HHmmss") + ".json";

            var newJurnals = getNewJurnalFromInsosys();
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

            Table tableJurnal = new Table(sources.First(a => a.tableName == "transaksi_jurnal"));
            if(journalIds != null) {
                string whereIn = "jurnal_id in (<jurnal_ids>)".Replace("<jurnal_ids>", "'" + String.Join("','", journalIds) + "'");
                List<RowData<ColumnName, object>> batchData;
                while((batchData = tableJurnal.getDatas(250, whereIn)).Count > 0) {
                    migratedJurnalInsosys.AddRange(getNewDataOnly(batchData));
                }
            } else if(filter != null) {
                List<RowData<ColumnName, object>> batchData;
                while((batchData = tableJurnal.getDatas(2500, filter)).Count > 0) {
                    migratedJurnalInsosys.AddRange(getNewDataOnly(batchData));
                }
            } else {
                List<RowData<ColumnName, object>> batchDataInsosys;
                while((batchDataInsosys = tableJurnal.getDatas(2500)).Count > 0) {
                    migratedJurnalInsosys.AddRange(getNewDataOnly(batchDataInsosys));
                }
            }

            return migratedJurnalInsosys.ToArray();
        }

        private RowData<ColumnName, object>[] getNewDataOnly(List<RowData<ColumnName, object>> inputs) {
            List<RowData<ColumnName, object>> result = new List<RowData<string, object>>();

            Table tableJournal = new Table(destinations.First(a => a.tableName == "transaction_journal"));
            int readBatchSize = 250;
            for(int a=0; a<inputs.Count; a+=readBatchSize) {
                var batchDataInsosys = inputs.Skip(a).Take(readBatchSize).ToArray();

                string[] insosysJournalIds = batchDataInsosys.Select(a => Utils.obj2str(a["jurnal_id"]).ToUpper()).Distinct().ToArray();

                string whereInClause = "tjournalid in (<jurnal_ids>)";
                whereInClause = whereInClause.Replace("<jurnal_ids>", "'" + String.Join("','", insosysJournalIds) + "'");

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
                        jurnal_id in (<jurnalids>)
                ";
                string[] selectColumns = sources.Where(a => a.tableName == "transaksi_jurnaldetil").First().columns;
                query = query.Replace("<columns>", "[" + String.Join("],[", selectColumns) + "]");
                query = query.Replace("<schema>", insosysConn.GetDbLoginInfo().schema);
                query = query.Replace("<jurnalids>", "'" + String.Join("','", jurnalids) + "'");
                var rs = QueryUtils.executeQuery(insosysConn, query);

                newJurnalDetails.AddRange(rs);
            }

            return newJurnalDetails.ToArray();
        }

        private void insertJournal(RowData<ColumnName, object>[] newJournals, NpgsqlTransaction transaction) {
            int insertBatchSize = 250;
            int insertedJournal = 0;

            for(int a = 0; a < newJournals.Length; a += insertBatchSize) {
                var batchJurnals = newJournals.Skip(a).Take(insertBatchSize).ToArray();

                Table tableJournal = new Table(destinations.First(a => a.tableName == "transaction_journal"));
                Table tableJournalDetail = new Table(destinations.First(a => a.tableName == "transaction_journal_detail"));

                tableJournal.insertData(getMappedJournalData(batchJurnals), false, true, transaction);
                Console.WriteLine();

                var saList = batchJurnals
                    .Where(a => Utils.obj2str(a["jurnal_id"]).ToUpper().StartsWith("SA"))
                    .Select(a => Utils.obj2str(a["jurnal_id"]).ToUpper()).ToArray();
                if(saList.Length > 0) {
                    Table tableJurnalReference = new Table(sources.First(a => a.tableName == "transaksi_jurnalreference"));
                    string whereInJurnalRef = "jurnal_id in ('<jurnal_ids>')".Replace("<jurnal_ids>", String.Join("','", saList));
                    var soIdList = tableJurnalReference.getAllData(whereInJurnalRef)
                        .Where(a => Utils.obj2str(a["jurnal_id_ref"]).ToUpper().StartsWith("SO"))
                        .Select(a => Utils.obj2str(a["jurnal_id_ref"]).ToUpper())
                        .Distinct()
                        .ToArray();

                    if(soIdList.Length > 0) {
                        Table tableSoSource = new Table(sources.First(a => a.tableName == "transaksi_salesorder"));
                        string whereInSo = "salesorder_id in ('<salesorder_ids>')".Replace("<salesorder_ids>", String.Join("','", soIdList));
                        var soDatas = tableSoSource.getAllData(whereInSo);
                        if(soDatas.Count > 0) {
                            Table tableSoDest = new Table(destinations.First(a => a.tableName == "transaction_sales_order"));
                            tableSoDest.insertData(getMappedSoData(soDatas), false, true, transaction);
                            Console.WriteLine();
                        }
                    }

                    Table tableJurnalTax = new Table(sources.First(a => a.tableName == "transaksi_jurnal_tax"));
                    string whereInJurnalTax = "jurnaltax_id in ('@jurnal_ids')".Replace("@jurnal_ids", String.Join("','", saList));
                }

                var batchJurnalDetails = getNewJurnalDetailFromInsosys(batchJurnals);
                if(batchJurnalDetails.Length > 0) {
                    tableJournalDetail.insertData(getMappedJournalDetailData(batchJurnalDetails), false, true, transaction);
                    Console.WriteLine();
                }

                insertedJournal += batchJurnals.Length;
            }
        }

        private List<RowData<ColumnName, object>> getMappedJournalDetailData(RowData<ColumnName, object>[] inputs) {
            var result = new List<RowData<ColumnName, object>>();
            var inputList = inputs.ToList();

            DataIntegration integration = new DataIntegration(connections);
            integration.fillJournalDetailTrackingFields(inputList);
            nullifyMissingReferences(
                "budget_id",
                "transaksi_budget",
                "budget_id",
                connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
                inputList
            );
            nullifyMissingReferences(
                "budgetdetil_id",
                "transaksi_budgetdetil",
                "budgetdetil_id",
                connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
                inputList
            );
            nullifyMissingReferences(
                "rekanan_id",
                "master_rekanan",
                "rekanan_id",
                connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
                inputList
            );
            nullifyMissingReferences(
                "acc_id",
                "master_acc",
                "acc_id",
                connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
                inputList
            );

            foreach(var data in inputList) {
                string tjournal_detailid = data["jurnal_id"].ToString().Substring(0, 2) + "D" + data["jurnal_id"].ToString().Substring(2) + data["jurnaldetil_line"].ToString();

                string tbudgetid = null;
                //if(Utils.obj2long(data["budget_id"]) > 0) {
                //    tbudgetid = IdRemapper.get("tbudgetid", data["budget_id"]).ToString();
                //}
                tbudgetid = Utils.obj2str(data["budget_id"]);
                tbudgetid = tbudgetid == "0" ? null : tbudgetid;

                string tbudget_detailid = null;
                //if(Utils.obj2long(data["budgetdetil_id"]) > 0) {
                //    tbudget_detailid = IdRemapper.get("tbudget_detailid", Utils.obj2long(data["budgetdetil_id"])).ToString();
                //}
                tbudget_detailid = Utils.obj2str(data["budgetdetil_id"]);
                tbudget_detailid = tbudget_detailid == "0" ? null : tbudget_detailid;

                string tjournalid = Utils.obj2str(data["jurnal_id"]).ToUpper();
                string accountid = Utils.obj2str(data["acc_id"]);
                accountid = accountid == "0" ? null : accountid;

                string ref_id = Utils.obj2str(data["ref_id"]);
                string ref_line = Utils.obj2str(data["ref_line"]);
                string ref_detail_id = null;
                if((ref_id != null && ref_id != "0") && ref_line != null) {
                    ref_detail_id = ref_id.Substring(0, 2) + "D" + ref_id.Substring(2) + ref_line;
                }

                string ref_budgetline = Utils.obj2str(data["ref_budgetline"]);
                string ref_subdetail_id = null;
                if(ref_detail_id != null && ref_budgetline != null) {
                    ref_subdetail_id = ref_detail_id + "_" + ref_budgetline;
                }

                string departmentId = Utils.obj2str(data["strukturunit_id"]);
                if(departmentId == "0") {
                    departmentId = null;
                } else {
                    departmentId = integration.getDepartmentFromStrukturUnit(departmentId);
                }

                result.Add(
                    new RowData<ColumnName, object>() {
                        { "tjournal_detailid",  tjournal_detailid},
                        { "tjournalid",  tjournalid},
                        { "dk",  data["jurnaldetil_dk"]},
                        { "description",  data["jurnaldetil_descr"]},
                        { "foreignamount",  Utils.obj2decimal(data["jurnaldetil_foreign"])},
                        { "foreignrate",  Utils.obj2decimal(data["jurnaldetil_foreignrate"])},
                        { "vendorid",  Utils.obj2long(data["rekanan_id"])==0? null: data["rekanan_id"]},
                        { "accountid",  accountid},
                        { "currencyid",  data["currency_id"]},
                        { "departmentid", departmentId},
                        { "tbudgetid",  tbudgetid},
                        { "tbudget_detailid",  tbudget_detailid},
                        { "ref_id",  data["ref_id"]},
                        { "ref_detail_id",  ref_detail_id},
                        { "ref_subdetail_id",  Utils.obj2int(data["ref_line"])},
                        { "bilyet_no",  null},
                        { "bilyet_date",  null},
                        { "bilyet_effectivedate",  null},
                        { "received_by",  null},
                        { "created_date",  data["created_dt"]},
                        { "created_by", getAuthInfo(data["created_by"], true) },
                        { "is_disabled", Utils.obj2bool(data["jurnal_isdisabled"]) },
                        { "disabled_date",  data["jurnal_isdisableddt"]},
                        { "disabled_by", getAuthInfo(data["jurnal_isdisabledby"]) },
                        { "modified_date",  null},
                        { "modified_by",  null},
                        //{ "budgetdetail_name",  data[""]},
                        { "idramount",  data["jurnaldetil_idr"]},
                        { "bankaccountid",  null},
                        { "paymenttypeid",  0},
                        { "journalreferencetypeid", integration.getJournalReferenceTypeId(tjournalid)},
                        { "subreference_id",  null},
                    }
                );
            }

            return result;
        }

        private List<RowData<ColumnName, object>> getMappedJournalData(RowData<ColumnName, object>[] inputs) {
            var result = new List<RowData<ColumnName, object>>();
            var inputList = inputs.ToList();

            nullifyMissingReferences("rekanan_id", "master_rekanan", "rekanan_id", connections.Where(a => a.GetDbLoginInfo().name == "e_frm").First(), inputList);

            DataIntegration integration = new DataIntegration(connections);
            Gen21Integration gen21 = new Gen21Integration(connections);

            foreach(RowData<ColumnName, object> data in inputList) {
                string tbudgetid = null;
                //if(Utils.obj2int(data["budget_id"]) > 0) {
                //    tbudgetid = IdRemapper.get("tbudgetid", data["budget_id"]).ToString();
                //}
                tbudgetid = Utils.obj2str(data["budget_id"]);
                tbudgetid = tbudgetid == "0" ? null : tbudgetid;

                string departmentId = Utils.obj2str(data["strukturunit_id"]);
                if(departmentId == "0") {
                    departmentId = null;
                } else {
                    departmentId = integration.getDepartmentFromStrukturUnit(departmentId);
                }

                string advertiserid = Utils.obj2str(data["advertiser_id"]);
                string advertiserbrandid = Utils.obj2str(data["brand_id"]);
                string advertisercode = null;
                string advertiserbrandcode = null;
                if(advertiserbrandid != null && advertiserid != null && advertiserbrandid != "0" && advertiserid != "0") {
                    try {
                        (advertisercode, advertiserbrandcode) = gen21.getAdvertiserBrandId(advertiserid, advertiserbrandid);
                    } catch(MissingAdvertiserBrandException e) {
                        advertisercode = advertiserid;
                        advertiserbrandcode = advertiserbrandid;
                    } catch(Exception) {
                        throw;
                    }
                }

                string transactionType = integration.getJournalIdPrefix(Utils.obj2str(data["jurnal_id"])).ToUpper();

                result.Add(
                    new RowData<ColumnName, object>() {
                        { "tjournalid",  Utils.obj2str(data["jurnal_id"]).ToUpper()},
                        { "bookdate",  data["jurnal_bookdate"]},
                        { "duedate",  data["jurnal_duedate"]},
                        { "billdate",  data["jurnal_billdate"]},
                        { "description",  data["jurnal_descr"]},
                        { "invoiceid",  data["jurnal_invoice_id"]},
                        { "invoicedescription",  data["jurnal_invoice_descr"]},
                        { "sourceid",  Utils.obj2str(data["jurnal_source"])},
                        { "currencyid",  data["currency_id"]==null? 0: data["currency_id"]},
                        { "foreignrate",  data["currency_rate"]},
                        { "accountexecutive_nik",  data["ae_id"]},
                        { "transactiontypeid", transactionType},
                        { "vendorid",  Utils.obj2int(data["rekanan_id"])==0? null: data["rekanan_id"]},
                        { "periodid",  data["periode_id"]},
                        { "tbudgetid",  tbudgetid},
                        { "departmentid",  departmentId},
                        { "accountcaid",  Utils.obj2int(data["acc_ca_id"])==0? null: data["acc_ca_id"]},
                        { "advertiserid", advertisercode},
                        { "advertiserbrandid", advertiserbrandcode},
                        { "paymenttypeid",  1},
                        { "created_by", getAuthInfo(data["created_by"], true) },
                        { "created_date",  data["created_dt"]},
                        { "is_disabled", Utils.obj2bool(data["jurnal_isdisabled"]) },
                        { "disabled_by", getAuthInfo(data["jurnal_isdisabledby"]) },
                        { "disabled_date",  data["jurnal_isdisableddt"] },
                        { "modified_by", getAuthInfo(data["modified_by"]) },
                        { "modified_date",  data["modified_dt"] },
                        { "is_posted", Utils.obj2bool(data["jurnal_isposted"]) },
                        { "posted_by",  data["jurnal_ispostedby"] },
                        { "posted_date",  data["jurnal_isposteddate"] },
                    }
                );
            }

            return result;
        }

        private List<RowData<ColumnName, object>> getMappedSoData(List<RowData<ColumnName, object>> inputs) {
            var result = new List<RowData<ColumnName, object>>();

            nullifyMissingReferences("salesorder_agency", "master_rekanan", "rekanan_id", connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(), inputs);

            Gen21Integration gen21 = new Gen21Integration(connections);

            foreach(RowData<ColumnName, object> data in inputs) {
                string advertiserid = Utils.obj2str(data["salesorder_advertiser"]);
                string advertiserbrandid = Utils.obj2str(data["salesorder_brand"]);
                string advertisercode = null;
                string advertiserbrandcode = null;
                if(advertiserbrandid != null && advertiserid != null && advertiserbrandid != "0" && advertiserid != "0") {
                    try {
                        (advertisercode, advertiserbrandcode) = gen21.getAdvertiserBrandId(advertiserid, advertiserbrandid);
                    } catch(MissingAdvertiserBrandException e) {
                        advertisercode = advertiserid;
                        advertiserbrandcode = advertiserbrandid;
                    } catch(Exception) {
                        throw;
                    }
                }

                string vendorbillidTag = Utils.obj2str(data["salesorder_agency"]) + "-" + Utils.obj2str(data["salesorder_agency_addr"]);
                int vendorbillid = 0;
                try {
                    vendorbillid = IdRemapper.get("vendorbillid", vendorbillidTag);
                } catch(Exception e) {
                    if(e.Message.StartsWith("RemappedId map does not have mapping for id-columnname")) {
                        throw;
                    }
                }

                result.Add(
                    new RowData<ColumnName, object>() {
                        { "tsalesorderid",  data["salesorder_id"]},
                        { "vendorid",  data["salesorder_agency"]},
                        { "vendorbillid",  vendorbillid},
                        { "mediaordernumber",  data["salesorder_ext_ref"]},
                        { "jobid",  data["salesorder_ext_ref2"]},
                        { "date",  data["salesorder_dt"]},
                        { "advertiserid",  advertisercode},
                        { "advertiserbrandid",  advertiserbrandcode},
                        { "periodedate",  data["salesorder_order_month"]},
                        { "billdate",  data["salesorder_bill_dt"]},
                        { "bookdate",  data["salesorder_book_dt"]},
                        { "due",  data["salesorder_due"]},
                        { "accountexecutive_nik",  data["salesorder_ae"]},
                        { "receivedby_nik",  data["salesorder_recv_by"]},
                        { "receiveddate",  data["salesorder_recv_dt"]},
                        { "currencyid",  data["salesorder_currency"]},
                        { "foreignamount",  data["salesorder_amount"]},
                        { "foreignrate",  data["salesorder_rate"]},
                        { "additionalamount", Utils.obj2decimal(data["salesorder_amount_add"])},
                        { "cancelationamount", Utils.obj2decimal(data["salesorder_amount_cancel"])},
                        { "commision",  data["salesorder_comm"]},
                        { "buyer", Utils.obj2decimal(data["salesorder_buyer"])},
                        { "salesareaid", Utils.obj2int(data["salesorder_area"])},
                        { "contractnumber",  data["salesorder_traffic_id"]},
                        { "invoiceformatid",  data["salesorder_format_inv"]},
                        { "invoiceply",  data["salesorder_ply_inv"]},
                        { "invoicetypeid",  data["salesorder_inv_type"]},
                        { "isdirect", Utils.obj2bool(Utils.obj2int(data["salesorder_direct"]))},
                        { "description",  data["salesorder_descr"]},
                        { "accountid",  data["salesorder_account"]},
                        { "mo",  data["salesorder_mo_avail"]},
                        { "moadd",  data["salesorder_mo_add"]},
                        { "momemo",  data["salesorder_mo_memo"]},
                        { "mocancelation",  data["salesorder_mo_canc"]},
                        { "modate",  data["salesorder_mo_date"]},
                        { "isapproved",  Utils.obj2bool(data["salesorder_isokay"])},
                        //{ "approvedby",  data[""]},
                        //{ "approveddate",  data[""]},
                        { "transactiontypeid",  data["salesorder_jurnaltypeid"]},

                        { "created_by", getAuthInfo(data["salesorder_entry_by"], true) },
                        { "created_date",  data["salesorder_entry_dt"]},
                        { "is_disabled", Utils.obj2bool(data["salesorder_iscanceled"]) },
                        //{ "disabled_by",  new AuthInfo(){ FullName = Utils.obj2str(data["jurnal_isdisabledby"]) } },
                        //{ "disabled_date",  data["jurnal_isdisableddt"] },
                        //{ "modified_by",  new AuthInfo(){ FullName = Utils.obj2str(data["modified_by"]) } },
                        //{ "modified_date",  data["modified_dt"] },
                    }
                );
            }

            return result;
        }
    }
}
