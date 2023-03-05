using SurplusMigrator.Exceptions;
using SurplusMigrator.Exceptions.Gen21;
using SurplusMigrator.Interfaces;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using SurplusMigrator.Models.Others;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class TransactionSalesOrder : _BaseTask, RemappableId {
        private Gen21Integration gen21;

        public TransactionSalesOrder(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
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
                }
            };
            destinations = new TableInfo[] {
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
                }
            };

            gen21 = new Gen21Integration(connections);
        }

        protected override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            string queryWhere = null;
            if(getOptions("salesorderids") != null) {
                string[] salesorderids = (
                    from id in getOptions("salesorderids").Split(",")
                    select id.Trim()
                ).ToArray();

                queryWhere = "WHERE salesorder_id in ('" + string.Join("','", salesorderids) + "')";
            }

            return sourceTables.Where(a => a.tableName == "transaksi_salesorder").FirstOrDefault().getData(batchSize, queryWhere);
        }

        public override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            MissingReference missingReferenceAdvertiser = new MissingReference() {
                foreignColumnName = "advertiserid",
                referencedTableName = "view_master_advertiser_temp",
                referencedColumnName = "name",
            };

            MissingReference missingReferenceBrand = new MissingReference() {
                foreignColumnName = "advertiserbrandid",
                referencedTableName = "view_master_brand_temp",
                referencedColumnName = "name",
            };

            string logFilenameMissingAdvertiser = "log_(" + this.GetType().Name + ")_nullified_missing_reference_to_(" + missingReferenceAdvertiser.referencedTableName + ")_" + _startedAt.ToString("yyyyMMdd_HHmmss") + ".json";
            string logFilenameMissingBrand = "log_(" + this.GetType().Name + ")_nullified_missing_reference_to_(" + missingReferenceBrand.referencedTableName + ")_" + _startedAt.ToString("yyyyMMdd_HHmmss") + ".json";

            try {
                missingReferenceAdvertiser = Utils.loadJson<MissingReference>(logFilenameMissingAdvertiser);
            } catch(FileNotFoundException) { }

            try {
                missingReferenceBrand = Utils.loadJson<MissingReference>(logFilenameMissingBrand);
            } catch(FileNotFoundException) { }

            nullifyMissingReferences("salesorder_agency", "master_rekanan", "rekanan_id", connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(), inputs);

            List<DbInsertFail> nullAdvertiserOrBrandErrors = new List<DbInsertFail>();
            foreach(RowData<ColumnName, object> data in inputs) {
                //string advertisercode = null;
                //int advertiserid = Utils.obj2int(data["salesorder_advertiser"]);
                //if(advertiserid != 0) {
                //    try {
                //        advertisercode = gen21.getAdvertiserId2(advertiserid);
                //    } catch(MissingDataException) {
                //        nullAdvertiserOrBrandErrors.Add(new DbInsertFail() {
                //            info = "Missing reference to table (" + missingReferenceAdvertiser.referencedTableName + ")",
                //            severity = DbInsertFail.DB_FAIL_SEVERITY_WARNING,
                //            type = DbInsertFail.DB_FAIL_TYPE_FOREIGNKEY_VIOLATION,
                //            loggedInFilename = logFilenameMissingAdvertiser
                //        });
                //        missingReferenceAdvertiser.referencedIds.Add(advertiserid);
                //    } catch(Exception) {
                //        throw;
                //    }
                //}

                //string advertiserbrandcode = null;
                //int advertiserbrandid = Utils.obj2int(data["salesorder_brand"]);
                //if(advertiserbrandid != 0) {
                //    try {
                //        advertiserbrandcode = gen21.getAdvertiserBrandId2(advertiserbrandid);
                //    } catch(MissingDataException) {
                //        nullAdvertiserOrBrandErrors.Add(new DbInsertFail() {
                //            info = "Missing reference to table (" + missingReferenceBrand.referencedTableName + ")",
                //            severity = DbInsertFail.DB_FAIL_SEVERITY_WARNING,
                //            type = DbInsertFail.DB_FAIL_TYPE_FOREIGNKEY_VIOLATION,
                //            loggedInFilename = logFilenameMissingBrand
                //        });
                //        missingReferenceBrand.referencedIds.Add(advertiserbrandid);
                //    } catch(Exception) {
                //        throw;
                //    }
                //}

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

                        nullAdvertiserOrBrandErrors.Add(new DbInsertFail() {
                            info = "Missing reference to table (" + missingReferenceAdvertiser.referencedTableName + ")",
                            severity = DbInsertFail.DB_FAIL_SEVERITY_WARNING,
                            type = DbInsertFail.DB_FAIL_TYPE_FOREIGNKEY_VIOLATION,
                            loggedInFilename = logFilenameMissingAdvertiser
                        });
                        missingReferenceAdvertiser.referencedIds.Add(advertiserid);

                        nullAdvertiserOrBrandErrors.Add(new DbInsertFail() {
                            info = "Missing reference to table (" + missingReferenceBrand.referencedTableName + ")",
                            severity = DbInsertFail.DB_FAIL_SEVERITY_WARNING,
                            type = DbInsertFail.DB_FAIL_TYPE_FOREIGNKEY_VIOLATION,
                            loggedInFilename = logFilenameMissingBrand
                        });
                        missingReferenceBrand.referencedIds.Add(advertiserbrandid);
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

                result.addData(
                    "transaction_sales_order",
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

            result.addErrors("transaction_sales_order", nullAdvertiserOrBrandErrors.ToArray());
            Utils.saveJson(logFilenameMissingAdvertiser, missingReferenceAdvertiser);
            Utils.saveJson(logFilenameMissingBrand, missingReferenceBrand);

            return result;
        }

        /**
         * Because Gen21Integration.getAdvertiserId() and Gen21Integration.getAdvertiserBrandId
         * is using IdRemapper.add()
         */
        protected override void onFinished() {
            IdRemapper.saveMap();
        }

        public void clearRemappingCache() {
            IdRemapper.clearMapping("advertiserid");
            IdRemapper.clearMapping("advertiserbrandid");
        }

        protected override void runDependencies() {
            new MasterVendor(connections).run();
            new MasterCurrency(connections).run();
            new MasterAccount(connections).run();
            new MasterTransactionType(connections).run();
            new MasterInvoiceFormat(connections).run();
            new MasterInvoiceType(connections).run();
            new MasterVendorBill(connections).run();
        }
    }
}
