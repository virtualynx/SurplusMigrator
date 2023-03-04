using SurplusMigrator.Interfaces;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class MasterVendorTax : _BaseTask {
        public MasterVendorTax(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
                    tableName = "master_rekanantax",
                    columns = new string[] {
                        "rekanan_id",
                        //"rekanantax_line",
                        "rekanantax_name",
                        "rekanantax_addr1",
                        "rekanantax_addr2",
                        "rekanantax_addr3",
                        "rekanantax_up",
                        "rekanantax_jabatan"
                    },
                    ids = new string[] { "rekanan_id" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tableName = "master_vendor_tax",
                    columns = new string[] {
                        //"vendortaxid",
                        "vendorid",
                        "name",
                        "up",
                        "position",
                        "address",
                        "created_date",
                        "created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "vendorid" }
                }
            };
        }

        public override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, object> data in inputs) {
                string addr1 = Utils.obj2str(data["rekanantax_addr1"]);
                string addr2 = Utils.obj2str(data["rekanantax_addr2"]);
                string addr3 = Utils.obj2str(data["rekanantax_addr3"]);
                string address = addr1?.ToString() + addr2?.ToString() + addr3?.ToString();
                if(address.Length == 0) {
                    address = null;
                }

                result.addData(
                    "master_vendor_tax",
                    new RowData<ColumnName, object>() {
                        { "vendorid",  data["rekanan_id"]},
                        { "name",  data["rekanantax_name"]},
                        { "up",  data["rekanantax_up"]},
                        { "position",  data["rekanantax_jabatan"]},
                        { "address",  address},
                        { "created_date", DateTime.Now},
                        { "created_by", DefaultValues.CREATED_BY},
                        { "is_disabled", false}
                    }
                );
            }

            return result;
        }

        protected override void runDependencies() {
            new MasterVendor(connections).run();
        }
    }
}
