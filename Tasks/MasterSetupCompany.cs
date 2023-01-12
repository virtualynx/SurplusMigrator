using SurplusMigrator.Models;
using System;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class MasterSetupCompany : _BaseTask {
        public MasterSetupCompany(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
            };
            destinations = new TableInfo[] {
                new TableInfo() {
                    connection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault(),
                    tableName = "master_setup_company",
                    columns = new string[] {
                        "setupcompanyid",
                        "name",
                        "number",
                        "reportname",
                        "address",
                        "telp1",
                        "telp2",
                        "telp3",
                        "fax",
                        "logotitle",
                        "logopath",
                        "created_date",
                        "created_by",
                        "is_disabled"
                    },
                    ids = new string[] { "setupcompanyid" }
                }
            };
        }

        protected override MappedData getStaticData() {
            MappedData result = new MappedData();

            result.addData(
                "master_setup_company",
                new RowData<ColumnName, object>() {
                    { "setupcompanyid",  "TV7"},
                    { "name",  "TV7"},
                    { "number",  "02"},
                    { "reportname",  "PT. Duta Visual Nusantara Tivi Tujuh"},
                    { "address",  "Jln. Kapten P. Tendean Kav. 12-14 A Mampang Prapatan, Jakarta, 12790"},
                    { "telp1",  "(021) 79177000"},
                    { "telp2",  "6212 / 6233 / 6612 / 6755 / 6899"},
                    { "telp3",  "081221637723"},
                    { "fax",  "(021) 79187695"},
                    { "logotitle",  "TV7"},
                    { "logopath",  "\\\\172.16.20.179\\insosys_startservices\\live\\solutions\\images\\TV7.jpg"},
                    { "created_date",  DateTime.Now},
                    { "created_by",  DefaultValues.CREATED_BY},
                    { "is_disabled", false }
                }
            );

            return result;
        }
    }
}
