using Npgsql;
using SurplusMigrator.Models;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Libraries {
    class DataIntegration {
        private DbConnection_[] connections;
        private Dictionary<string, int> _currencyIdMaps = null;

        public DataIntegration(DbConnection_[] connections) {
            this.connections = connections;
        }

        public int getCurrencyIdFromShortname(string shortname) {
            int result = getCurrencyIdMaps()["UNKWN"];
            if(getCurrencyIdMaps().ContainsKey(shortname)) {
                result = getCurrencyIdMaps()[shortname];
            }

            return result;
        }

        private Dictionary<string, int> getCurrencyIdMaps() {
            if(_currencyIdMaps == null) {
                _currencyIdMaps = new Dictionary<string, int>();

                DbConnection_ connection_ = connections.Where(a => a.GetDbLoginInfo().dbname == "insosys").FirstOrDefault();
                NpgsqlConnection conn = (NpgsqlConnection)connection_.GetDbConnection();
                NpgsqlCommand command = new NpgsqlCommand("select currencyid, shortname from \"" + connection_.GetDbLoginInfo().schema + "\".\"master_currency\"", conn);
                NpgsqlDataReader dataReader = command.ExecuteReader();

                while(dataReader.Read()) {
                    int currencyid = Utils.obj2int(dataReader.GetValue(dataReader.GetOrdinal("currencyid")));
                    string shortname = Utils.obj2str(dataReader.GetValue(dataReader.GetOrdinal("shortname")));

                    _currencyIdMaps[shortname] = currencyid;
                }
                dataReader.Close();
                command.Dispose();
            }

            return _currencyIdMaps;
        }
    }
}
