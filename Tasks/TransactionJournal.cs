using SurplusMigrator.Exceptions.Gen21;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace SurplusMigrator.Tasks {
    class TransactionJournal : _BaseTask {
        public TransactionJournal(DbConnection_[] connections) : base(connections) {
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
                        "is_posted",
                        "posted_by",
                        "posted_date",
                        "created_date",
                        "created_by",
                        "is_disabled",
                        "disabled_by",
                        "disabled_date",
                        "modified_by",
                        "modified_date",
                        "approveddate",
                        "isapproved",
                        "approvedby",
                        "journaltypeid",
                    },
                    ids = new string[] { "tjournalid" }
                }
            };
        }

        protected override List<RowData<string, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            string queryWhere = null;
            if(getOptions("journalids") != null) {
                string[] journalids = (
                    from id in getOptions("journalids").Split(",")
                    select id.Trim()
                ).ToArray();

                queryWhere = "WHERE jurnal_id in ('" + string.Join("','", journalids) + "')";
            }

            return sourceTables.Where(a => a.tableName == "transaksi_jurnal").FirstOrDefault().getDatas(batchSize, queryWhere);
        }

        protected override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            nullifyMissingReferences("rekanan_id", "master_rekanan", "rekanan_id", connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(), inputs);

            DataIntegration integration = new DataIntegration(connections);
            Gen21Integration gen21 = new Gen21Integration(connections);

            foreach(RowData<ColumnName, object> data in inputs) {
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

                result.addData(
                    "transaction_journal",
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
                        { "is_posted", Utils.obj2bool(data["jurnal_isposted"]) },
                        { "posted_by",  data["jurnal_ispostedby"] },
                        { "posted_date",  data["jurnal_isposteddate"] },
                        { "created_by", getAuthInfo(data["created_by"], true) },
                        { "created_date",  data["created_dt"]},
                        { "is_disabled", Utils.obj2bool(data["jurnal_isdisabled"]) },
                        { "disabled_by", getAuthInfo(data["jurnal_isdisabledby"]) },
                        { "disabled_date",  data["jurnal_isdisableddt"] },
                        { "modified_by", getAuthInfo(data["modified_by"]) },
                        { "modified_date",  data["modified_dt"] },
                        { "approveddate",  data["jurnal_isposteddate"] },
                        { "isapproved", Utils.obj2bool(data["jurnal_isposted"]) },
                        { "approvedby",  data["jurnal_ispostedby"] },
                        { "journaltypeid",  data["jurnaltype_id"] },
                    }
                );
            }

            return result;
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
        }
    }
}
