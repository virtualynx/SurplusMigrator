using Npgsql;
using SurplusMigrator.Interfaces;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Globalization;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class _FixReportDataV2 : _BaseTask, IRemappableId {
        private DbConnection_ _insosysConnection;
        private DbConnection_ _surplusConnection;

        public _FixReportDataV2(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
            };
            destinations = new TableInfo[] {
            };

            _insosysConnection = connections.First(a => a.GetDbLoginInfo().name == "e_frm");
            _surplusConnection = connections.First(a => a.GetDbLoginInfo().name == "surplus");
        }

        protected override void onFinished() {
            DateTime reportDate = DateTime.ParseExact(getOptions("date"), "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

            //var mirrorDbJob = new _MirrorDatabase(connections);
            //mirrorDbJob.run(false);

            NpgsqlTransaction transaction = ((NpgsqlConnection)_surplusConnection.GetDbConnection()).BeginTransaction();

            try {
                QueryUtils.executeQuery(
                    _surplusConnection,
                    "ALTER TABLE transaction_journal RENAME TO transaction_journal_;",
                    null,
                    transaction
                );
                QueryUtils.executeQuery(
                    _surplusConnection, 
                    "CREATE TABLE transaction_journal AS TABLE transaction_journal_ with no data;",
                    null,
                    transaction
                );
                QueryUtils.executeQuery(
                    _surplusConnection,
                    "ALTER TABLE transaction_journal_detail RENAME TO transaction_journal_detail_;",
                    null,
                    transaction
                );
                QueryUtils.executeQuery(
                    _surplusConnection,
                    "CREATE TABLE transaction_journal_detail AS TABLE transaction_journal_detail_ with no data;",
                    null,
                    transaction
                );
                transaction.Commit();
            } catch(Exception) {
                transaction.Rollback();
                throw;
            } finally {
                transaction.Dispose();
            }

            transaction = ((NpgsqlConnection)_surplusConnection.GetDbConnection()).BeginTransaction();
            QueryUtils.toggleTrigger(_surplusConnection, "transaction_journal", false);
            QueryUtils.toggleTrigger(_surplusConnection, "transaction_journal_detail", false);
            try {
                new MasterVendorBill(connections).run(); //populate vendorid map
                new TransactionJournal(connections).run();
                new TransactionJournalDetail(connections).run();
                transaction.Commit();
            } catch(Exception) {
                transaction.Rollback();
                throw;
            } finally {
                transaction.Dispose();
                QueryUtils.toggleTrigger(_surplusConnection, "transaction_journal", true);
                QueryUtils.toggleTrigger(_surplusConnection, "transaction_journal_detail", true);
            }


        }

        void IRemappableId.clearRemappingCache() {
            IdRemapper.clearMapping("advertiserid");
            IdRemapper.clearMapping("advertiserbrandid");
            IdRemapper.clearMapping("vendorbillid");
        }
    }
}
