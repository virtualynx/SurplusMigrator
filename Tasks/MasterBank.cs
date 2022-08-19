using SurplusMigrator.Interfaces;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class MasterBank : _BaseTask {
        public MasterBank(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "E_FRM").FirstOrDefault(),
                    tableName = "master_bank",
                    columns = new string[] {
                        "bank_id",
                        "bank_name",
                        "bank_active",
                    },
                    ids = new string[] { "bank_id" }
                }
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().dbname == "insosys").FirstOrDefault(),
                    tableName = "master_bank",
                    columns = new string[] {
                        "bankid",
                        "name",
                        "code",
                        "created_date",
                        "created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "bankid" }
                }
            };
        }

        public override List<RowData<ColumnName, Data>> getSourceData(Table[] sourceTables, int batchSize = defaultBatchSize) {
            return sourceTables.Where(a => a.tableName == "master_bank").FirstOrDefault().getDatas(batchSize);
        }

        public override MappedData mapData(List<RowData<ColumnName, Data>> inputs) {
            MappedData result = new MappedData();

            Dictionary<string, string> bankCodeMap = new Dictionary<string, string>() {
                { "Mandiri", "008" },
                { "BCA", "014" },
                { "BNI", "009" },
                { "Danamon", "011" },
                { "Bumi Putera", "458" },
                { "Mega", "426" },
                { "Citybank", "031" },
                { "Universal", "UNKNOWN01" },
                { "BTN", "200" },
                { "Akita", "525" },
                { "Global", "UNKNOWN02" },
                { "DKI", "111" },
                { "Bank Allo-Rp 2", "UNKNOWN03" },
                { "Mayora", "553" },
                { "Bangkok", "040" },
                { "BBI", "UNKNOWN04" },
                { "Argo", "UNKNOWN05" },
                { "Shinta", "UNKNOWN06" },
                { "Standart Chartered Bank", "050" },
                { "Lippo", "026" },
                { "Piko", "UNKNOWN07" },
                { "ABN", "052" },
                { "Niaga", "022" },
                { "BII", "016" },
                { "NISP", "028" },
                { "BRI", "002" },
                { "Haga", "089" },
                { "BNN", "UNKNOWN08" },
                { "IFI", "093" },
                { "Muamalat", "147" },
                { "PDFCI Bank", "UNKNOWN09" },
                { "American Bank", "030" },
                { "Bank of Tokyo", "042" },
                { "CIC", "UNKNOWN10" },
                { "DKB", "UNKNOWN11" },
                { "Bukopin", "441" },
                { "Tugu", "UNKNOWN12" },
                { "Bali", "129" },
                { "HSBC", "041" },
                { "Daiwa Perdania", "UNKNOWN13" },
                { "ANZ Panin Bank", "061" },
                { "Panin Bank", "019" },
                { "Mas (Multi Arta sentosa)", "548" },
                { "Media", "UNKNOWN14" },
                { "Kas-Rp", "UNKNOWN15" },
                { "Kas-Valas", "UNKNOWN16" },
                { "Buana Indonesia (BBI)", "UNKNOWN17" },
                { "IndoMonex", "UNKNOWN18" },
                { "Permata", "013" },
                { "Bank Mizuho Indonesia", "048" },
                { "Mizuho Indonesia", "UNKNOWN19" },
                { "Artha Graha", "037" },
                { "HS 1906 (Himpunan Saudara 1906)", "212" },
                { "Resona Perdania", "047" },
                { "Deutsche Bank", "067" },
                { "BPD Jateng", "113" },
                { "Arta Niaga Kencana", "020" },
                { "Syariah Mandiri", "UNKNOWN20" },
                { "INA", "513" },
                { "RHB Berhad", "UNKNOWN21" },
                { "Maspion", "157" },
                { "Harmoni", "166" },
                { "BSMI", "UNKNOWN22" },
                { "Ganesha", "161" },
                { "Sumitomo Mitsui Indonesia", "045" },
                { "Victoria", "566" },
                { "Nagari", "118" },
                { "Swadesi", "UNKNOWN23" },
                { "Bank Jasa Jakarta", "472" },
                { "Bank BJB", "110" },
                { "Bank Allo", "UNKNOWN24" },
                { "Bank BNP Paribas", "057" },
                { "Bank Sinarmas", "153" },
                { "KEB Hana Bank", "484" },
                { "Bank Sumut", "117" },
                { "Bank Syariah Indonesia", "451" },
                { "Allo Bank", "567" },
            };

            foreach(RowData<ColumnName, Data> data in inputs) {
                result.addData(
                    "master_bank",
                    new RowData<ColumnName, Data>() {
                        { "bankid",  data["bank_id"]},
                        { "name",  data["bank_name"]},
                        { "code",  bankCodeMap[Utils.obj2str(data["bank_name"])]},
                        { "created_date",  DateTime.Now},
                        { "created_by",  DefaultValues.CREATED_BY},
                        { "is_disabled", !Utils.obj2bool(data["bank_active"]) }
                    }
                );
            }

            return result;
        }

        public override MappedData additionalStaticData() {
            return null;
        }

        public override void runDependencies() {
        }
    }
}
