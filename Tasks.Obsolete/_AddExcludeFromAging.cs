using Npgsql;
using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class _AddExcludeFromAging : _BaseTask {
        private DbConnection_ targetConnection;
		private const string _temp_tablename = "_temp_grouped_response";
        private const string _query = @"
			do $$
				--declare _date timestamp = '2023-05-31 23:59:59';
				declare _date timestamp = @date;
				declare _group_refs varchar[] = array[<group_refs>];
			begin
				drop table if exists temp_accaging;
				CREATE TEMP TABLE IF NOT EXISTS temp_accaging AS
					SELECT DISTINCT 
						mac.accountcategoryid ,
						macd.accountid
					FROM 
						master_account_category_detail macd 
						LEFT OUTER JOIN master_account_category mac ON mac.accountcategoryid  = macd.accountcategoryid  
					WHERE 
						mac.name = 'AGING AP' 
						AND mac.type = 'AP'
						and macd.is_disabled = false
					;
				CREATE INDEX temp_accaging_accid ON temp_accaging (accountid);
		
				drop table if exists temp_journal_piutang;
				CREATE TEMP TABLE IF NOT EXISTS temp_journal_piutang AS
					SELECT
						tjd.tjournalid, 
						tjd.tjournal_detailid, 
						tjd.dk, 
						tjd.currencyid, 
						tjd.idramount, 
						tjd.foreignamount,
						tjd.accountid,			
						(
							CASE 
							WHEN ISFINITE(bookdate) = FALSE OR bookdate  = '0001-01-01' THEN
								tjd.created_date
							ELSE 
								bookdate 
							end
						) as bookdate,
						tj.description,
						tj.sourceid, 
						mv.vendorid as vendorid,
						mv.name AS vendor_name,
						tjd.ref_id AS group_ref
					FROM 
						transaction_journal_detail tjd
						JOIN transaction_journal tj ON tjd.tjournalid = tj.tjournalid
						JOIN temp_accaging ta ON tjd.accountid = ta.accountid
						LEFT JOIN master_vendor mv on mv.vendorid = coalesce(
							(
								case tjd.vendorid 
								when 0 then tj.vendorid
								else tjd.vendorid
								end
							)
							, tj.vendorid
						)
					where
						tj.bookdate::DATE <= _date
						and tj.is_posted = true 
						and tj.is_disabled = false
						and tjd.is_disabled = false
						and tjournal_detailid not in (select tjournal_detailid from transaction_journal_excluded_fromaging where is_disabled=false)
					;
				CREATE INDEX temp_journal_piutang_tjournalid_idx ON temp_journal_piutang (tjournalid);

				drop table if exists temp_preaging1;
				CREATE TEMP TABLE IF NOT EXISTS temp_preaging1 AS
					SELECT
						tjournalid,
						tjournal_detailid,
						description,
						dk,
						vendorid,
						vendor_name,
						accountid,
						bookdate,
						currencyid,
						idramount,
						sourceid,
						(
							CASE coalesce(LEFT(group_ref,2),'')
							WHEN 'Mi' THEN tjournalid
							WHEN 'TR' THEN tjournalid
							WHEN 'CQ' THEN tjournalid
							WHEN 'SO' THEN tjournalid
							WHEN 'PO' THEN tjournalid
							WHEN 'RO' THEN tjournalid
							WHEN 'MO' THEN tjournalid
							WHEN 'TO' THEN tjournalid
							WHEN 'NO' THEN tjournalid
							WHEN 'EO' THEN tjournalid
							WHEN 'RI' THEN tjournalid
							WHEN 'GR' THEN tjournalid
							WHEN 'VQ' THEN tjournalid
							WHEN 'BQ' THEN tjournalid
							WHEN '' THEN (
								CASE LEFT(tjournalid,2)
								WHEN 'PV' THEN (
									CASE WHEN (
										SELECT 
											COUNT(1) 
										FROM temp_journal_piutang 
										WHERE 
											tjournalid = tjp.tjournalid
											and dk='D'
									) > 0 
									THEN(
										SELECT group_ref 
										FROM temp_journal_piutang 
										WHERE 
											tjournalid = tjp.tjournalid
											and dk='D'
										LIMIT 1
									) ELSE
										tjournalid 
									END 
								) ELSE 
									tjournalid 
								END 
							)
							ELSE 
								group_ref 
							END
						) AS group_ref
					FROM 
						temp_journal_piutang tjp
					;
				CREATE INDEX temp_preaging1_idx ON temp_preaging1 (tjournalid);

				drop table if exists temp_preaging2;
				CREATE TEMP TABLE IF NOT EXISTS temp_preaging2 AS
					SELECT 
						temp_preaging1.tjournalid,
						(
							CASE coalesce(temp_preaging1.group_ref, '') 
							WHEN '' THEN 
								temp_preaging1.tjournalid 
							ELSE 
								temp_preaging1.group_ref 
							end
						) AS group_ref,
						temp_preaging1.tjournal_detailid,
						temp_preaging1.description,
						temp_preaging1.vendorid,
						temp_preaging1.vendor_name,
						temp_preaging1.accountid,
						(
							CASE  
							WHEN group_ref='' THEN (
								SELECT bookdate FROM transaction_journal WHERE tjournalid = temp_preaging1.tjournalid
							) 
							WHEN group_ref is NULL THEN (
								SELECT bookdate FROM transaction_journal WHERE tjournalid = temp_preaging1.tjournalid
							)
							ELSE (
								SELECT bookdate FROM transaction_journal WHERE tjournalid = temp_preaging1.group_ref
							)
							END
						) AS bookdate,
						temp_preaging1.idramount,
						(
							CASE WHEN temp_preaging1.sourceid not in ('JV-Manual', 'AP-ListManual')
							THEN 
								temp_preaging1.currencyid
							ELSE (
								SELECT currencyid FROM transaction_journal WHERE tjournalid = temp_preaging1.group_ref
							)
							END
						) AS currencyid
					FROM 
						temp_preaging1
					;
				CREATE INDEX temp_preaging2_idx ON temp_preaging2 (tjournalid);
				CREATE INDEX temp_preaging2_idx2 ON temp_preaging2 (tjournal_detailid);
				CREATE INDEX temp_preaging2_idx3 ON temp_preaging2 (tjournalid, tjournal_detailid);

				drop table if exists temp_grouped_response;
				CREATE TEMP TABLE IF NOT EXISTS temp_grouped_response as
					SELECT 
						tjournal_detailid,
						tjournalid,
						group_ref, 
						idramount,
						accountid,
						vendor_name,
						description as header_desc,
						(select description from transaction_journal_detail where tjournal_detailid = tp2.tjournal_detailid) as desc
					FROM 
						temp_preaging2 tp2
					where 
						accountid in @accountids
						and group_ref = any(_group_refs)
					;
			end$$;

			--select * from temp_grouped_response;

			select 
				vendor_name,
				group_ref,
				sum(idramount) as idramount,
				(
					select string_agg(tjournal_detailid, ', ')
					from temp_grouped_response
					where 
						group_ref = tgr.group_ref
				) as tjournal_detailid
			from 
				temp_grouped_response tgr 
			group by 
				vendor_name, group_ref
			--order by vendor_name
			;
        ";

        public _AddExcludeFromAging(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
            };
            destinations = new TableInfo[] {
            };

            targetConnection = connections.Where(a => a.GetDbLoginInfo().name == "surplus").First();
            Console.WriteLine("\n");
            MyConsole.Information("Add exclude from aging (schema " + targetConnection.GetDbLoginInfo().schema + ")");
            Console.WriteLine();
            Console.Write("Continue performing job (Y/N)? ");
            string choice = Console.ReadLine();
            if(choice.ToLower() != "y") {
                throw new Exceptions.JobAbortedException();
            }
        }

        protected override void onFinished() {
            string[] accountids = new string[] { "2015018" };

            var excelDatas = getDataFromExcel();

            excelDatas = excelDatas.Skip(7).ToArray();

            excelDatas = excelDatas.Where(a => Utils.obj2str(a["first_column"]) != "Total").ToArray();

            var excelDatasGroupedByVendor = groupedByVendor(excelDatas);

			var tjournalids = new List<string>();
			foreach(var map in excelDatasGroupedByVendor) {
				tjournalids.AddRange(map.Value.Select(a => a["first_column"].ToString()).ToList());
            }
            tjournalids = tjournalids.Distinct().ToList();

            var rs = QueryUtils.executeQuery(
				targetConnection, 
				_query.Replace("<group_refs>", "'"+String.Join("','", tjournalids) +"'"), 
				new Dictionary<string, object> {
					{ "@date", getOptions("date") },
                    { "@accountids", accountids }
                },
				null,
				120
			);

            Table excludeAgingTable = new Table() {
                connection = targetConnection,
                tablename = "transaction_journal_excluded_fromaging",
                columns = new string[] {
                        "tjournal_detailid",
                        "description",
                        "created_date",
                        "created_by",
                        "is_disabled",
                    },
                ids = new string[] {
                    "tjournal_detailid"
                },
            };

			var allExcluded = excludeAgingTable.getAllData(null, 5000, false, false);
			var allExcludedTjournalDetailId = allExcluded.Select(a => a["tjournal_detailid"].ToString()).ToArray();
            var newExcludes = new List<RowData<string, object>>();
			List<string> disabledExclude = new List<string>();
            DateTime createdDate = DateTime.Now;

            foreach(var excelRow in excelDatasGroupedByVendor) {
				string excelVendorName = excelRow.Key;
				RowData<string, object>[] excelDataArr = excelRow.Value;

                var data = rs.Where(a => Utils.obj2str(a["vendor_name"]) == excelVendorName).ToArray();

				if(data.Length > 0) {
                    foreach(var excelData in excelDataArr) {
                        var matchingData = data.FirstOrDefault(a => a["group_ref"].ToString() == excelData["first_column"].ToString());
                        if(matchingData != null) {
							if(Utils.obj2decimal(matchingData["idramount"]) == Utils.obj2decimal(excelData["amount"])) {
								string[] tjournalDetailIds = matchingData["tjournal_detailid"].ToString().Split(',').Select(a => a.Trim()).ToArray();
								string[] alreadyExist = tjournalDetailIds.Where(a => allExcludedTjournalDetailId.Contains(a)).ToArray();

								if(alreadyExist.Length > 0) {
									foreach(string tjournalDetailId in alreadyExist) {
										var excludedData = allExcluded.First(a => a["tjournal_detailid"].ToString() == tjournalDetailId);
										string isDisabledClause = "";
										if(Utils.obj2bool(excludedData["is_disabled"])) {
											isDisabledClause = " and is disabled";
											disabledExclude.Add(tjournalDetailId);
                                        }
                                        MyConsole.Warning(
                                            @"Exclude data for vendor: <vendor>, group_ref: <tjournalid>, tjournal_detailid: <tjournal_detailid> is already exists <isDisabledClause>"
                                            .Replace("<vendor>", excelVendorName)
                                            .Replace("<tjournalid>", excelData["first_column"].ToString())
                                            .Replace("<tjournal_detailid>", tjournalDetailId)
                                            .Replace("<isDisabledClause>", isDisabledClause)
                                        );
                                    }
                                }

                                tjournalDetailIds = tjournalDetailIds.Where(a => !alreadyExist.Contains(a)).ToArray();

                                foreach(string tjournalDetailId in tjournalDetailIds) {
                                    newExcludes.Add(new RowData<string, object> {
										{ "tjournal_detailid", tjournalDetailId },
                                        { "description", matchingData["group_ref"].ToString() },
                                        { "created_date", createdDate },
                                        { "created_by", getAuthInfo("SYSTEM") },
                                        { "is_disabled", false }
                                    });
                                }
                            } else {
                                MyConsole.Warning(
                                    @"Different amount for vendor: <vendor>, group_ref: <tjournalid>"
                                    .Replace("<vendor>", excelVendorName)
                                    .Replace("<tjournalid>", excelData["first_column"].ToString())
                                );
                            }
                        } else {
                            MyConsole.Warning(
                                @"No matching data for vendor: <vendor>, group_ref: <tjournalid>"
                                .Replace("<vendor>", excelVendorName)
                                .Replace("<tjournalid>", excelData["first_column"].ToString())
                            );
                        }
                    }
                } else {
                    MyConsole.Warning(
                        @"No data found for vendor: <vendor>"
                        .Replace("<vendor>", excelVendorName)
                    );
                }
			}

            NpgsqlTransaction transaction = ((NpgsqlConnection)targetConnection.GetDbConnection()).BeginTransaction();
            try {
                excludeAgingTable.insertData(newExcludes, transaction);

				if(disabledExclude.Count > 0) {
					QueryUtils.executeQuery(
						targetConnection,
						"update transaction_journal_excluded_fromaging set is_disabled = false where tjournal_detailid in @tjournal_detailids",
						new Dictionary<string, object> {
							{ "@tjournal_detailids", disabledExclude.ToArray() }
						},
                        transaction
					);
					MyConsole.Information(@"Enabling @tjournal_detailids".Replace("@tjournal_detailids", String.Join(", ", disabledExclude)));
                }

                transaction.Commit();
            } catch(Exception e) {
                transaction.Rollback();
                throw;
            }
        }

        private RowData<ColumnName, object>[] getDataFromExcel() {
            ExcelColumn[] columns = new ExcelColumn[] {
                new ExcelColumn(){ name="first_column", ordinal=0 },
                new ExcelColumn(){ name="desc", ordinal=1 },
                new ExcelColumn(){ name="due", ordinal=2 },
                new ExcelColumn(){ name="amount", ordinal=8 },
            };

            return Utils.getDataFromExcel("Aging AP USD.xlsx", columns).ToArray();
        }

		private Dictionary<string, RowData<string, object>[]> groupedByVendor(RowData<ColumnName, object>[] data) {
            var rsGroupedByVendor = new Dictionary<string, RowData<string, object>[]>();

            string currVendorName = null;
            List<RowData<string, object>> currRowdata = new List<RowData<string, object>>();
            foreach(var row in data) {
                string first_column = Utils.obj2str(row["first_column"]);
                string desc = Utils.obj2str(row["desc"]);
                string due = Utils.obj2str(row["due"]);

				if(first_column == null) {

				} else if(desc == null && due == null) { //vendor line
                    if(currVendorName != null) {
                        rsGroupedByVendor[currVendorName] = currRowdata.ToArray();
                    }
                    currVendorName = first_column;
                    currRowdata.Clear();
                } else if(desc != null && due == null) { //account line
                } else if(desc != null && due != null) { //journal line
                    string amountStr = Utils.obj2str(row["amount"])?.Trim();
                    if(amountStr.StartsWith("(")) {
                        amountStr = amountStr.Replace("(", "");
                        amountStr = amountStr.Replace(")", "");
                        amountStr = "-" + amountStr;
                    } else if(amountStr == "-") {
                        amountStr = "0";
                    }

                    try {
                        decimal amount = Utils.obj2decimal(amountStr);

                        currRowdata.Add(new RowData<string, object> {
							{ "first_column", first_column },
							{ "desc", desc },
							{ "due", Utils.obj2int(row["due"]) },
							{ "amount", amount }
						});
                    } catch(Exception e) {
						MyConsole.Warning(
							@"Cannot get ""amount"" value for vendor: <vendor>, group_ref: <tjournalid>"
							.Replace("<vendor>", currVendorName)
                            .Replace("<tjournalid>", first_column)
                        );
                    }
                } else {
                    throw new Exception("Unidentified line type");
                }
            }

            //append last rowdatas
            if(currVendorName != null && currRowdata.Count > 0) {
                rsGroupedByVendor[currVendorName] = currRowdata.ToArray();
            }

			return rsGroupedByVendor;
        }
    }
}
