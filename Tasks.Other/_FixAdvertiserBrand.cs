using SurplusMigrator.Exceptions.Gen21;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class _FixAdvertiserBrand : _BaseTask {
        private DbConnection_ targetConnection;
        private Gen21Integration gen21;

        private const int DEFAULT_BATCH_SIZE = 500;

        private string[] tableToFix = new string[] {
            "transaction_journal",
            "transaction_sales_order"
        };

        public _FixAdvertiserBrand(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
            };
            destinations = new TableInfo[] {
            };

            if(getOptions("tables") != null) {
                string[] tableList = getOptions("tables").Split(",");
                if(tableList.Length > 0) {
                    tableToFix = (from table in tableList select table.Trim()).ToArray();
                }
            }

            gen21 = new Gen21Integration(connections);
            targetConnection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").First();
            Console.WriteLine("\n");
            MyConsole.Information("Advertiser and Brand will be fixed on table (schema "+targetConnection.GetDbLoginInfo().schema+"): " + String.Join(",", tableToFix));
            Console.WriteLine();
            Console.Write("Continue performing fix (Y/N)? ");
            string choice = Console.ReadLine();
            if(choice.ToLower() != "y") {
                throw new Exceptions.JobAbortedException();
            }
        }

        protected override void onFinished() {
            foreach(var tablename in tableToFix) {
                try {
                    MyConsole.Information("Fixing advertiserid and advertiserbrandid in " + tablename + " ... ");

                    string[] primaryKeys = QueryUtils.getPrimaryKeys(targetConnection, tablename);
                    if(primaryKeys.Length == 0 || primaryKeys.Length > 1) {
                        throw new Exception("Invalid primary-key number: " + primaryKeys.Length);
                    }

                    var distinctedBrand = QueryUtils.executeQuery(
                        targetConnection,
                        //@"
                        //    select distinct(advertiserbrandid) 
                        //    from transaction_journal 
                        //    where 
                        //        (advertiserid is not null and advertiserid <> '0')
                        //        and (advertiserbrandid is not null and advertiserbrandid <> '0' and advertiserbrandid ~ '^([0-9]+\.?[0-9]*|\.[0-9]+)$')
                        //"
                        @"
                            select 
	                            distinct advertiserid, advertiserbrandid, count(1)
                            from ""<tablename>""
                            where 
                                advertiserid is not null and advertiserid <> '0'
                                and advertiserbrandid is not null and advertiserbrandid <> '0'
                                and advertiserbrandid ~ '^([0-9]+\.?[0-9]*|\.[0-9]+)$'
                            group by advertiserid, advertiserbrandid
                        "
                        .Replace("<tablename>", tablename)
                    );

                    int updatedCount = 0;
                    foreach(var dbrow in distinctedBrand) {
                        string advertiserid = dbrow["advertiserid"].ToString();
                        string advertiserbrandid = dbrow["advertiserbrandid"].ToString();

                        string queryCount = @"
                                select count(1) from ""<tablename>""
                                where 
                                    advertiserid = @old_advertiserid
                                    and advertiserbrandid = @old_advertiserbrandid
                                ;
                            "
                            .Replace("<tablename>", tablename)
                        ;

                        var parameters = new Dictionary<string, object> {
                            { "@old_advertiserid", advertiserid },
                            { "@old_advertiserbrandid", advertiserbrandid }
                        };

                        var rsCount = QueryUtils.executeQuery(targetConnection, queryCount, parameters);
                        int dataCount = Utils.obj2int(rsCount.First()["count"]);

                        string querySelect = @"
                                select ""<select_column>"" from ""<tablename>""
                                where 
                                    advertiserid = @old_advertiserid
                                    and advertiserbrandid = @old_advertiserbrandid
                                ;
                            "
                            .Replace("<select_column>", primaryKeys[0])
                            .Replace("<tablename>", tablename)
                        ;

                        string newAdvertiserId;
                        string newBrandId;
                        try {
                            (newAdvertiserId, newBrandId) = gen21.getAdvertiserBrandId(advertiserid, advertiserbrandid);
                        } catch(MissingAdvertiserBrandException) {
                            MyConsole.Warning("Missing Advertiser-Brand: AdvertiserId["+ advertiserid + "] BrandId["+ advertiserbrandid + "]");
                            continue;
                        }

                        var data = QueryUtils.executeQuery(targetConnection, querySelect, parameters);
                        if(data.Length > 0) {
                            string[] filterValues = data.Select(a => Utils.obj2str(a[primaryKeys[0]])).ToArray();

                            string queryUpdate = @"
                                    update ""<tablename>"" 
                                    set 
                                        advertiserid = @newadvertiserid, advertiserbrandid = @newbrandid
                                    where ""<filter_column>"" IN @filter_values;
                                "
                                .Replace("<tablename>", tablename)
                                .Replace("<filter_column>", primaryKeys[0])
                            ;

                            var updateResult = QueryUtils.executeQuery(
                                targetConnection, 
                                queryUpdate, 
                                new Dictionary<string, object> {
                                    { "@newadvertiserid", newAdvertiserId },
                                    { "@newbrandid", newBrandId },
                                    { "@filter_values", filterValues }
                                }
                            );

                            updatedCount += data.Length;

                            MyConsole.Information(
                                "Update [advertiser, advertiserbrand]: [@old_advertiserid, @old_advertiserbrandid] => [@newadvertiserid, @newbrandid] (@datacount data)"
                                .Replace("@old_advertiserid", advertiserid)
                                .Replace("@old_advertiserbrandid", advertiserbrandid)
                                .Replace("@newadvertiserid", newAdvertiserId)
                                .Replace("@newbrandid", newBrandId)
                                .Replace("@datacount", data.Length.ToString())
                            );
                        }
                    }

                    MyConsole.EraseLine();
                    MyConsole.Information("Successfully fixing advertiserid and advertiserbrandid in table " + tablename);
                    MyConsole.WriteLine("", false);
                } catch(Exception) {
                    throw;
                }
            }
        }
    }
}
