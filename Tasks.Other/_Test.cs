using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class _Test : _BaseTask {
        public _Test(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
            };
            destinations = new TableInfo[] {
            };
        }

        protected override void runDependencies() {
            //var surplusConn = connections.Where(a => a.GetDbLoginInfo().name == "surplus").First();
            //QueryUtils.toggleTrigger(surplusConn, "master_vendor_bill", false);
            //new MasterVendorBill(connections).run(false, new TaskTruncateOption() { truncateBeforeInsert = true });
            //QueryUtils.toggleTrigger(surplusConn, "master_vendor_bill", true);

            new MasterVendorBill(connections).run(false);
        }

        protected override void onFinished() {
            Console.WriteLine("Finished");
        }
    }
}
