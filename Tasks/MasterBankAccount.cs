using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class MasterBankAccount : _BaseTask {
        public MasterBankAccount(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "E_FRM").FirstOrDefault(),
                    tableName = "master_bankacc",
                    columns = new string[] {
                        "bankacc_id",
                        "bankacc_ac",
                        "bankacc_name",
                        "bankacc_bank",
                        "currency_id",
                        "bankacc_descr",
                        "bankacc_telp",
                        "bankacc_fax",
                        "bankacc_branch",
                        "bankacc_address1",
                        "bankacc_address2",
                        "bankacc_active",
                        "bankacc_account",
                        "bankacc_createdt",
                        "bankacc_createby",
                        "bankacc_reportname",
                        "bankacc_formbg",
                        "bankacc_formcek",
                        "bankacc_formtrf",
                        "bankacc_formset",
                    },
                    ids = new string[] { "bankacc_id" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "insosys").FirstOrDefault(),
                    tableName = "master_bank_account",
                    columns = new string[] {
                        "bankaccountid",
                        "accountnumber",
                        "name",
                        "bankid",
                        "currencyid",
                        "description",
                        "telp",
                        "fax",
                        "branch",
                        "address",
                        "accountid",
                        "created_date",
                        "created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "bankaccountid" }
                }
            };
        }

        protected override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            return sourceTables.Where(a => a.tableName == "master_bankacc").FirstOrDefault().getDatas(batchSize);
        }

        protected override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, object> data in inputs) {
                string address1 = Utils.obj2str(data["bankacc_address1"]);
                string address2 = Utils.obj2str(data["bankacc_address2"]);
                List<string> addressList = new List<string>();

                if(address1 != null) {
                    addressList.Add(address1);
                }
                if(address2 != null) {
                    addressList.Add(address2);
                }

                result.addData(
                    "master_bank_account",
                    new RowData<ColumnName, object>() {
                        { "bankaccountid",  data["bankacc_id"]},
                        { "name",  data["bankacc_name"]},
                        { "accountnumber",  data["bankacc_ac"]},
                        { "bankid",  data["bankacc_bank"]},
                        { "currencyid",  data["currency_id"]},
                        { "description",  data["bankacc_descr"]},
                        { "telp",  data["bankacc_telp"]},
                        { "fax",  data["bankacc_fax"]},
                        { "branch",  data["bankacc_branch"]},
                        { "address",  String.Join(", ", addressList)},
                        { "accountid",  data["bankacc_account"]},
                        { "created_date",  data["bankacc_createdt"]},
                        { "created_by",  new AuthInfo(){ FullName = Utils.obj2str(data["bankacc_createby"]) } },
                        { "is_disabled", !Utils.obj2bool(data["bankacc_active"]) }
                    }
                );
            }

            return result;
        }

        protected override void runDependencies() {
            new MasterBank(connections).run();
        }
    }
}
