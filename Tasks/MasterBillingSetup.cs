using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class MasterBillingSetup : _BaseTask {
        public MasterBillingSetup(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault(),
                    tablename = "master_setup",
                    columns = new string[] {
                        "setup_id",
                        "setup_invoice",
                        "setup_debitnote",
                        "setup_creditnote",
                        "setup_delivery",
                        "setup_logform",
                        "setup_orderdt",
                        "setup_salesarea",
                        //"setup_sales_src",
                        "setup_bill_period_descr",
                        "setup_direct",
                        "setup_ag_comm",
                        "setup_descr",
                        "setup_billdt",
                        "setup_bookdt",
                        //"channel_id",
                        "setup_createby",
                        "setup_createdt",
                        "setup_modifyby",
                        "setup_modifydt"
                    },
                    ids = new string[] { "setup_id" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tablename = "master_billing_setup",
                    columns = new string[] {
                        "billingsetupid",
                        "invoiceformatid",
                        "debitnote_format_id",
                        "creditnote_format_id",
                        "delivery_format_id",
                        "logproof_format_id",
                        "orderdate",
                        "salesareaid",
                        "periodedescription",
                        "isdirect",
                        "commision",
                        "description",
                        "billdate",
                        "bookdate",
                        "created_date",
                        "created_by",
                        "modified_date",
                        "modified_by",
                        "is_disabled"
                    },
                    ids = new string[] { "billingsetupid" }
                }
            };
        }

        protected override List<RowData<ColumnName, object>> getSourceData(Table[] sourceTables, int batchSize = defaultReadBatchSize) {
            return sourceTables.Where(a => a.tablename == "master_setup").FirstOrDefault().getData(batchSize);
        }

        public override MappedData mapData(List<RowData<ColumnName, object>> inputs) {
            MappedData result = new MappedData();

            foreach(RowData<ColumnName, object> data in inputs) {
                result.addData(
                    "master_billing_setup",
                    new RowData<ColumnName, object>() {
                        { "billingsetupid",  data["setup_id"]},
                        { "invoiceformatid",  data["setup_invoice"]},
                        { "debitnote_format_id",  data["setup_debitnote"]},
                        { "creditnote_format_id",  data["setup_creditnote"]},
                        { "delivery_format_id",  data["setup_delivery"]},
                        { "logproof_format_id",  data["setup_logform"]},
                        { "orderdate",  data["setup_orderdt"]},
                        { "salesareaid",  data["setup_salesarea"]},
                        { "periodedescription",  data["setup_bill_period_descr"]},
                        { "isdirect", Utils.obj2bool(data["setup_direct"])},
                        { "commision",  data["setup_ag_comm"]},
                        { "description",  data["setup_descr"]},
                        { "billdate",  data["setup_billdt"]},
                        { "bookdate",  data["setup_bookdt"]},
                        { "created_date",  Utils.obj2datetime(data["setup_createdt"])},
                        { "created_by",  getAuthInfo(data["setup_createby"], true)},
                        { "modified_date",  Utils.obj2datetime(data["setup_modifydt"])},
                        { "modified_by",  getAuthInfo(data["setup_modifyby"])},
                        { "is_disabled", false }
                    }
                );
            }

            return result;
        }

        protected override void runDependencies() {
            new MasterInvoiceFormat(connections).run();
        }
    }
}
