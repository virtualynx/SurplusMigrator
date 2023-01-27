using Npgsql;
using SurplusMigrator.Exceptions;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Libraries {
    class Gen21Integration {
        private DbConnection_[] connections;

        public Gen21Integration(DbConnection_[] connections) {
            this.connections = connections;
        }

        public string getAdvertiserId(string oldAdvertiserId) {
            string idRemapperKey = "advertiserid";
            try {
                return IdRemapper.get(idRemapperKey, oldAdvertiserId).ToString();
            } catch(Exception e) {
                if(
                    e.Message != "RemappedId map does not have mapping for id-columnname: " + idRemapperKey
                    && e.Message != "RemappedId map for id-columnname: " + idRemapperKey + ", does not have mapping for old-value: " + oldAdvertiserId
                ) {
                    throw;
                }
            }

            var efrmConn = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").First();
            string queryGetAdvertiserNames = @"
                select
                    trafficadvertiser_name
                from
                    master_trafficadvertiser
                where
                    code = <old_advertiser_id>
                    and trafficadvertiser_isactive = 1
            ";
            queryGetAdvertiserNames = queryGetAdvertiserNames.Replace("<old_advertiser_id>", oldAdvertiserId);
            var trafficAdvertiserNamesRs = QueryUtils.executeQuery(efrmConn, queryGetAdvertiserNames);
            var trafficAdvertiserNames = (from row in trafficAdvertiserNamesRs select row["trafficadvertiser_name"].ToString()).ToArray();

            //if not found in master_trafficadvertiser, get the name from master_advertiser instead
            if(trafficAdvertiserNames.Length == 0) {
                queryGetAdvertiserNames = @"
                    select
                        [name]
                    from
                        master_advertiser
                    where
                        code = <old_advertiser_id>
                ";
                queryGetAdvertiserNames = queryGetAdvertiserNames.Replace("<old_advertiser_id>", oldAdvertiserId.ToString());
                trafficAdvertiserNamesRs = QueryUtils.executeQuery(efrmConn, queryGetAdvertiserNames);
                trafficAdvertiserNames = (from row in trafficAdvertiserNamesRs select row["name"].ToString()).ToArray();
            }

            if(trafficAdvertiserNames.Length == 0) {
                throw new MissingDataException("Unidentified AdvertiserId(" + oldAdvertiserId + ") in both [master_trafficadvertiser] and [master_advertiser]");
            }

            var surplusConn = connections.Where(a => a.GetDbLoginInfo().name == "surplus").First();
            //Array.Sort(trafficAdvertiserNames, (x, y) => y.Length.CompareTo(x.Length)); //sort descending by length
            RowData<ColumnName, object>[] gen21SearchRs = null;
            foreach(string trafficAdvertiserName in trafficAdvertiserNames) {
                gen21SearchRs = QueryUtils.searchSimilar(
                    surplusConn,
                    "view_master_advertiser_temp",
                    new string[] { "advertiserid" },
                    "name",
                    trafficAdvertiserName.ToUpper()
                );
                if(gen21SearchRs.Length > 0) break;
            }

            if(gen21SearchRs.Length > 0) {
                string advertiserId = gen21SearchRs[0]["advertiserid"].ToString();
                IdRemapper.add(idRemapperKey, oldAdvertiserId, advertiserId);

                return advertiserId;
            }

            return null;
        }

        public (string advertiserId, string brandId) getAdvertiserBrandId(string oldAdvertiserId, string oldAdvertiserBrandId) {
            string idRemapperKey = "advertiserbrandid";
            try {
                string brandId = IdRemapper.get(idRemapperKey, oldAdvertiserBrandId).ToString();
                return (getAdvertiserId(oldAdvertiserId), brandId);
            } catch(Exception e) {
                if(
                    e.Message != "RemappedId map does not have mapping for id-columnname: " + idRemapperKey
                    && e.Message != "RemappedId map for id-columnname: " + idRemapperKey + ", does not have mapping for old-value: " + oldAdvertiserBrandId
                ) {
                    throw;
                }
            }

            string advertiserId = getAdvertiserId(oldAdvertiserId);

            if(advertiserId == null) {
                throw new MissingDataException("Unidentified AdvertiserId(" + oldAdvertiserId + ") in Gen21");
            }

            var efrmConn = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").First();
            string queryGetBrandNames = @"
                select
                    trafficbrand_name
                from
                    master_trafficbrand
                where
                    advertiser_brand_id = <old_brand_id>
                    and trafficbrand_isactive = 1
            ";
            queryGetBrandNames = queryGetBrandNames.Replace("<old_brand_id>", oldAdvertiserBrandId.ToString());
            var trafficBrandNamesRs = QueryUtils.executeQuery(efrmConn, queryGetBrandNames);
            var trafficBrandNames = (from row in trafficBrandNamesRs select row["trafficbrand_name"].ToString()).ToArray();

            //if not found in master_trafficbrand, get the name from master_advertiserbrand instead
            if(trafficBrandNames.Length == 0) {
                queryGetBrandNames = @"
                    select
                        advertiser_brand_name
                    from
                        master_advertiserbrand
                    where
                        advertiser_brand_id = <old_brand_id>
                ";
                queryGetBrandNames = queryGetBrandNames.Replace("<old_brand_id>", oldAdvertiserBrandId.ToString());
                trafficBrandNamesRs = QueryUtils.executeQuery(efrmConn, queryGetBrandNames);
                trafficBrandNames = (from row in trafficBrandNamesRs select row["advertiser_brand_name"].ToString()).ToArray();
            }

            trafficBrandNames = (from name in trafficBrandNames select name.ToUpper()).ToArray();

            var surplusConn = connections.Where(a => a.GetDbLoginInfo().name == "surplus").First();
            //Array.Sort(trafficAdvertiserNames, (x, y) => y.Length.CompareTo(x.Length)); //sort descending by length
            RowData<ColumnName, object>[] gen21SearchRs = null;
            foreach(string trafficBrandName in trafficBrandNames) {
                gen21SearchRs = QueryUtils.searchSimilar(
                    surplusConn,
                    "view_master_brand_temp",
                    new string[] { "advertiserid", "advertiserbrandid", "name" },
                    "name",
                    trafficBrandName.ToUpper()
                );
                if(gen21SearchRs.Length > 0) break;
            }

            if(gen21SearchRs.Length > 0) {
                gen21SearchRs = gen21SearchRs.Where(
                    row => 
                        row["advertiserid"].ToString() == advertiserId
                        && trafficBrandNames.Contains(row["name"].ToString().ToUpper())
                ).ToArray();
                string brandId = gen21SearchRs[0]["advertiserbrandid"].ToString();
                IdRemapper.add(idRemapperKey, oldAdvertiserBrandId, brandId);

                return (advertiserId, brandId);
            }

            return (null, null);
        }

        public string getAdvertiserId2(int oldAdvertiserId) {
            string idRemapperKey = "advertiserid";
            try {
                return IdRemapper.get(idRemapperKey, oldAdvertiserId).ToString();
            } catch(Exception e) {
                if(
                    e.Message != "RemappedId map does not have mapping for id-columnname: " + idRemapperKey
                    && e.Message != "RemappedId map for id-columnname: " + idRemapperKey + ", does not have mapping for old-value: " + oldAdvertiserId
                ) {
                    throw;
                }
            }

            string[] trafficAdvertiserNames = searchAtInsosys2(
                "master_trafficadvertiser", 
                new string[] { "trafficadvertiser_name" }, 
                new Dictionary<string, dynamic>() {
                    { "code", oldAdvertiserId },
                    { "trafficadvertiser_isactive", 1 }
                }
            );

            if(trafficAdvertiserNames.Length == 0) {
                throw new MissingDataException("Unidentified AdvertiserId(" + oldAdvertiserId + ") in [master_trafficadvertiser]");
            }

            string[] gen21AdvertiserIds = new string[] { };
            
            //using exact-search first
            foreach(string trafficAdvertiserName in trafficAdvertiserNames) {
                gen21AdvertiserIds = searchAtGen21(
                    "view_master_advertiser_temp",
                    new string[] { "advertiserid" },
                    new Dictionary<string, dynamic>() {
                        { "name", trafficAdvertiserName.ToUpper() }
                    }
                );
                if(gen21AdvertiserIds.Length > 0) break;
            }

            if(gen21AdvertiserIds.Length == 0) {
                //using similarity-search
                foreach(string trafficAdvertiserName in trafficAdvertiserNames) {
                    gen21AdvertiserIds = searchAtGen21(
                        "view_master_advertiser_temp",
                        new string[] { "advertiserid" },
                        new Dictionary<string, dynamic>() {
                            { "name", trafficAdvertiserName }
                        },
                        false
                    );
                    if(gen21AdvertiserIds.Length > 0) break;
                }
            }

            string result = null;
            if(gen21AdvertiserIds.Length > 0) {
                result = gen21AdvertiserIds[0];
                IdRemapper.add(idRemapperKey, oldAdvertiserId, result);
            }

            return result;
        }

        public string getAdvertiserBrandId2(int oldAdvertiserBrandId) {
            string idRemapperKey = "advertiserbrandid";
            try {
                return IdRemapper.get(idRemapperKey, oldAdvertiserBrandId).ToString();
            } catch(Exception e) {
                if(
                    e.Message != "RemappedId map does not have mapping for id-columnname: " + idRemapperKey
                    && e.Message != "RemappedId map for id-columnname: " + idRemapperKey + ", does not have mapping for old-value: " + oldAdvertiserBrandId
                ) {
                    throw;
                }
            }

            string[] trafficAdvertiserBrandNames = searchAtInsosys2(
                "master_trafficbrand",
                new string[] { "trafficbrand_name" },
                new Dictionary<string, dynamic>() {
                    { "advertiser_brand_id", oldAdvertiserBrandId },
                    { "trafficbrand_isactive", 1 }
                }
            );

            if(trafficAdvertiserBrandNames.Length == 0) {
                throw new MissingDataException("Unidentified AdvertiserBrandId(" + oldAdvertiserBrandId + ") in [master_trafficbrand]");
            }

            string[] gen21AdvertiserBrandIds = new string[] { };

            //using exact-search first
            foreach(string trafficAdvertiserBrandName in trafficAdvertiserBrandNames) {
                gen21AdvertiserBrandIds = searchAtGen21(
                    "view_master_brand_temp",
                    new string[] { "advertiserbrandid" },
                    new Dictionary<string, dynamic>() {
                        { "name", trafficAdvertiserBrandName.ToUpper() }
                    }
                );
                if(gen21AdvertiserBrandIds.Length > 0) break;
            }

            if(gen21AdvertiserBrandIds.Length == 0) {
                //using similarity-search
                foreach(string trafficAdvertiserBrandName in trafficAdvertiserBrandNames) {
                    gen21AdvertiserBrandIds = searchAtGen21(
                        "view_master_brand_temp",
                        new string[] { "advertiserbrandid" },
                        new Dictionary<string, dynamic>() {
                            { "name", trafficAdvertiserBrandName }
                        },
                        false
                    );
                    if(gen21AdvertiserBrandIds.Length > 0) break;
                }
            }

            string result = null;
            if(gen21AdvertiserBrandIds.Length > 0) {
                result = gen21AdvertiserBrandIds[0];
                IdRemapper.add(idRemapperKey, oldAdvertiserBrandId, result);
            }

            return result;
        }

        //private string[] searchAtInsosys(string table, string[] columns, Dictionary<string, dynamic> filters) {
        //    string sql = @"
        //        select
        //            [column]
        //        from
        //            [table]
        //    ";

        //    List<string> filtersForSql = new List<string>();
        //    foreach(KeyValuePair<string, dynamic> entry in filters) {
        //        filtersForSql.Add(entry.Key + " = @" + entry.Key);
        //    }

        //    if(filtersForSql.Count > 0) {
        //        sql += " where " + String.Join(" and ", filtersForSql);
        //    }

        //    sql = sql.Replace("[column]", String.Join(",", columns));
        //    sql = sql.Replace("[table]", table);

        //    var dbConn = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").FirstOrDefault();

        //    SqlConnection conn = (SqlConnection)dbConn.GetDbConnection();
        //    SqlCommand command = new SqlCommand(sql, conn);

        //    foreach(KeyValuePair<string, dynamic> entry in filters) {
        //        command.Parameters.AddWithValue("@"+ entry.Key, entry.Value);
        //    }

        //    List<string> results = new List<string>();
        //    SqlDataReader reader = command.ExecuteReader();
        //    while(reader.Read()) {
        //        dynamic data = reader.GetValue(0);
        //        if(data.GetType() == typeof(System.DBNull)) {
        //            data = null;
        //        } else if(data.GetType() == typeof(string)) {
        //            data = data.ToString().Trim();
        //        }
        //        results.Add(data);
        //    }
        //    reader.Close();
        //    command.Dispose();

        //    return results.ToArray();
        //}

        private string searchAtInsosys(string table, string[] columns, string word) {
            DbConnection_ conn = connections.Where(a => a.GetDbLoginInfo().name == "e_frm").First();
            var similar = QueryUtils.searchSimilar(
                conn,
                "master_trafficadvertiser",
                new string[] {
                    "trafficadvertiser_name"
                },
                "trafficadvertiser_name",
                word
            );

            return similar.Length > 0? similar[0]["trafficadvertiser_name"].ToString(): null;
        }

        private string[] searchAtInsosys2(string table, string[] columns, Dictionary<string, dynamic> filters, bool exact = true) {
            string sql = @"
                select
                    [column]
                from
                    [table]
            ";

            List<string> columnsTemp = new List<string>(columns);
            List<string> filtersForSql = new List<string>();
            List<string> orderForSql = new List<string>();
            foreach(KeyValuePair<string, dynamic> entry in filters) {
                if(exact) {
                    filtersForSql.Add(entry.Key + " = @" + entry.Key);
                } else {
                    filtersForSql.Add(entry.Key + " % @" + entry.Key);
                    columnsTemp.Add(entry.Key + " % @" + entry.Key + " as " + entry.Key + "_similarity");
                    orderForSql.Add(entry.Key + "_similarity desc");
                }
            }
            columns = columnsTemp.ToArray();

            if(filtersForSql.Count > 0) {
                sql += " where " + String.Join(" and ", filtersForSql);
                if(!exact && orderForSql.Count > 0) {
                    sql += " order by " + String.Join(",", orderForSql);
                }
            }

            sql = sql.Replace("[column]", String.Join(",", columns));
            sql = sql.Replace("[table]", table);

            NpgsqlConnection conn = (NpgsqlConnection)connections.Where(a => a.GetDbLoginInfo().name == "e_frm_integration").FirstOrDefault().GetDbConnection();
            NpgsqlCommand command = new NpgsqlCommand(sql, conn);

            foreach(KeyValuePair<string, dynamic> entry in filters) {
                command.Parameters.AddWithValue("@" + entry.Key, entry.Value);
            }

            List<string> results = new List<string>();
            NpgsqlDataReader reader = command.ExecuteReader();
            while(reader.Read()) {
                dynamic data = reader.GetValue(0);
                if(data.GetType() == typeof(System.DBNull)) {
                    data = null;
                } else if(data.GetType() == typeof(string)) {
                    data = data.ToString().Trim();
                }
                results.Add(data);
            }
            reader.Close();
            command.Dispose();

            return results.ToArray();
        }

        /// <summary>
        /// Search data at gen21 database, if the exact value not found, it will returns multiple data with the most-similar result at the first
        /// </summary>
        /// <param name="table"></param>
        /// <param name="columns"></param>
        /// <param name="filters"></param>
        /// <param name="exact"></param>
        /// <returns></returns>
        private string[] searchAtGen21(string table, string[] columns, Dictionary<string, dynamic> filters, bool exact = true) {
            string sql = @"
                select
                    [column]
                from
                    [table]
            ";

            List<string> columnsTemp = new List<string>(columns);
            List<string> filtersForSql = new List<string>();
            List<string> orderForSql = new List<string>();
            foreach(KeyValuePair<string, dynamic> entry in filters) {
                if(exact) {
                    filtersForSql.Add(entry.Key + " = @" + entry.Key);
                } else {
                    filtersForSql.Add(entry.Key + " % @" + entry.Key);
                    columnsTemp.Add(entry.Key + " <-> @" + entry.Key + " as " + entry.Key + "_similarity");
                    orderForSql.Add(entry.Key + "_similarity desc");
                }
            }
            columns = columnsTemp.ToArray();

            if(filtersForSql.Count > 0) {
                sql += " where " + String.Join(" and ", filtersForSql);
                if(!exact && orderForSql.Count > 0) {
                    sql += " order by " + String.Join(",", orderForSql);
                }
            }

            sql = sql.Replace("[column]", String.Join(",", columns));
            sql = sql.Replace("[table]", table);

            NpgsqlConnection conn = (NpgsqlConnection)connections.Where(a => a.GetDbLoginInfo().name == "surplus").FirstOrDefault().GetDbConnection();
            NpgsqlCommand command = new NpgsqlCommand(sql, conn);

            foreach(KeyValuePair<string, dynamic> entry in filters) {
                command.Parameters.AddWithValue("@" + entry.Key, entry.Value);
            }

            List<string> results = new List<string>();
            NpgsqlDataReader reader = command.ExecuteReader();
            while(reader.Read()) {
                dynamic data = reader.GetValue(0);
                if(data.GetType() == typeof(System.DBNull)) {
                    data = null;
                } else if(data.GetType() == typeof(string)) {
                    data = data.ToString().Trim();
                }
                results.Add(data);
            }
            reader.Close();
            command.Dispose();

            return results.ToArray();
        }
    }
}
