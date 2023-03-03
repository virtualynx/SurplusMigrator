using SurplusMigrator.Interfaces;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class MasterVendorCheckout : _BaseTask, RemappableId {
        public MasterVendorCheckout(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
                    tableName = "master_rekanan_tandakeluar",
                    columns = new string[] {
                        "rekanan_id",
                        "rekanantk_line",
                        "name",
                        "addr1",
                        "addr2",
                        "addr3",
                        "up",
                        "jabatan",
                        "invoice",
                        "faktur",
                        "logpr",
                        "collector",
                        "fr_invoice",
                        "fr_debet",
                        "fr_credit",
                        "fr_logpr",
                        "active",
                        "salesperson",
                        "create_by",
                        "create_date",
                        "modify_by",
                        "modify_date"
                    },
                    ids = new string[] { "rekanan_id", "rekanantk_line" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tableName = "master_vendor_checkout",
                    columns = new string[] {
                        "vendorcheckoutid",
                        "vendorid",
                        "name",
                        "up",
                        "position",
                        "address",
                        "invoice",
                        "faktur",
                        "logpr",
                        "created_date",
                        "created_by",
                        "is_disabled",
                        "modified_date",
                        "modified_by"
                    },
                    ids = new string[] { "vendorid", "address" }
                }
            };
        }

        protected override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            return sourceTables.Where(a => a.tableName == "master_rekanan_tandakeluar").FirstOrDefault().getData(batchSize);
        }

        protected override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, object> data in inputs) {
                string addr1 = Utils.obj2str(data["addr1"]);
                string addr2 = Utils.obj2str(data["addr2"]);
                string addr3 = Utils.obj2str(data["addr3"]);
                string address = addr1?.ToString() + addr2?.ToString() + addr3?.ToString();
                if(address.Length == 0) {
                    address = null;
                }

                int vendorcheckoutid = Utils.obj2int(SequencerString.getId(null, "DUMMY_MVC", DateTime.Now, 14).Substring(("DUMMY_MVC" + "yyMMdd").Length));
                string vendorcheckoutidTag = Utils.obj2str(data["rekanan_id"]) + "-" + Utils.obj2str(data["rekanantk_line"]);
                IdRemapper.add("vendorcheckoutid", vendorcheckoutidTag, vendorcheckoutid);

                result.addData(
                    "master_vendor_checkout",
                    new RowData<ColumnName, object>() {
                        { "vendorcheckoutid",  vendorcheckoutid},
                        { "vendorid",  data["rekanan_id"]},
                        { "name",  data["name"]},
                        { "up",  data["up"]},
                        { "position",  data["jabatan"]},
                        { "address",  address},
                        { "invoice", Utils.obj2int(data["invoice"])},
                        { "faktur", Utils.obj2int(data["faktur"])},
                        { "logpr", Utils.obj2int(data["logpr"])},
                        { "created_date",  data["create_date"]},
                        { "created_by",  getAuthInfo(data["create_by"], true)},
                        { "is_disabled", !Utils.obj2bool(data["active"]) },
                        { "modified_date",  data["modify_date"]},
                        { "modified_by",  getAuthInfo(data["modify_by"])},
                    }
                );
            }

            return result;
        }

        protected override void onFinished() {
            IdRemapper.saveMap();
        }

        public void clearRemappingCache() {
            IdRemapper.clearMapping("vendorbillid");
        }

        //protected override MappedData getStaticData() {
        //    MappedData result = new MappedData();

        //    //found in TransactionSalesOrder's reference
        //    result.addData(
        //        "vendorcheckoutid",
        //        new RowData<ColumnName, object>() {
        //            { "vendorbillid", 0},
        //            { "vendorid", 0},
        //            { "name", "Unknown"},
        //            { "up",  null},
        //            { "position",  null},
        //            { "address",  null},
        //            { "collector",  null},
        //            { "invoice_ply", 0},
        //            { "faktur_ply", 0},
        //            { "logproof_ply", 0},
        //            { "salesperson", null},
        //            { "created_date",  DateTime.Now},
        //            { "created_by",  DefaultValues.CREATED_BY},
        //            { "is_disabled", false},
        //            { "modified_date",  null},
        //            { "modified_by",  null},
        //        }
        //    );

        //    return result;
        //}

        protected override void runDependencies() {
            new MasterVendor(connections).run();
        }
    }
}
