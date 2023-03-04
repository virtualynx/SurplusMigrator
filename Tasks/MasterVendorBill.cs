using SurplusMigrator.Interfaces;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class MasterVendorBill : _BaseTask, RemappableId {
        public MasterVendorBill(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
                    tableName = "master_rekananbill",
                    columns = new string[] {
                        "rekanan_id",
                        "rekananbill_line",
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
                        "active",
                        "salesperson",
                        "create_by",
                        "create_date",
                        "modify_by",
                        "modify_date"
                    },
                    ids = new string[] { "rekanan_id", "rekananbill_line" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tableName = "master_vendor_bill",
                    columns = new string[] {
                        "vendorbillid",
                        "vendorid",
                        "name",
                        "up",
                        "position",
                        "address",
                        "collector",
                        "invoice_ply",
                        "faktur_ply",
                        "logproof_ply",
                        "salesperson",
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
            return sourceTables.Where(a => a.tableName == "master_rekananbill").FirstOrDefault().getData(batchSize);
        }

        public override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, object> data in inputs) {
                string addr1 = Utils.obj2str(data["addr1"]);
                string addr2 = Utils.obj2str(data["addr2"]);
                string addr3 = Utils.obj2str(data["addr3"]);
                string address = addr1?.ToString() + addr2?.ToString() + addr3?.ToString();
                if(address.Length == 0) {
                    address = null;
                }

                int vendorbillid = Utils.obj2int(SequencerString.getId(null, "DUMMY_MVB", DateTime.Now).Substring(("DUMMY_MVB" + "yyMMdd").Length));
                string vendorbillidTag = Utils.obj2str(data["rekanan_id"]) + "-" + Utils.obj2str(data["rekananbill_line"]);
                IdRemapper.add("vendorbillid", vendorbillidTag, vendorbillid);

                result.addData(
                    "master_vendor_bill",
                    new RowData<ColumnName, object>() {
                        { "vendorbillid",  vendorbillid},
                        { "vendorid",  data["rekanan_id"]},
                        { "name",  data["name"]},
                        { "up",  data["up"]},
                        { "position",  data["jabatan"]},
                        { "address",  address},
                        { "collector",  data["collector"]},
                        { "invoice_ply", Utils.obj2int(data["invoice"])},
                        { "faktur_ply", Utils.obj2int(data["faktur"])},
                        { "logproof_ply", Utils.obj2int(data["logpr"])},
                        { "salesperson", Utils.obj2str(data["salesperson"])},
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

        protected override MappedData getStaticData() {
            MappedData result = new MappedData();

            //found in TransactionSalesOrder's reference
            result.addData(
                "master_vendor_bill",
                new RowData<ColumnName, object>() {
                    { "vendorbillid", 0},
                    { "vendorid", 0},
                    { "name", "Unknown"},
                    { "up",  null},
                    { "position",  null},
                    { "address",  null},
                    { "collector",  null},
                    { "invoice_ply", 0},
                    { "faktur_ply", 0},
                    { "logproof_ply", 0},
                    { "salesperson", null},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false},
                    { "modified_date",  null},
                    { "modified_by",  null},
                }
            );

            return result;
        }

        protected override void runDependencies() {
            new MasterVendor(connections).run();
        }
    }
}
