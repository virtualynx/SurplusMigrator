using SurplusMigrator.Libraries;
using SurplusMigrator.Models;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace SurplusMigrator.Tasks {
    class _FixReportData : _BaseTask {
        private DbConnection_ _insosysConnection;
        private DbConnection_ _surplusConnection;

        public _FixReportData(DbConnection_[] connections) : base(connections) {
            sources = new TableInfo[] {
            };
            destinations = new TableInfo[] {
            };

            _insosysConnection = connections.First(a => a.GetDbLoginInfo().name == "e_frm");
            _surplusConnection = connections.First(a => a.GetDbLoginInfo().name == "surplus");
        }

        protected override void onFinished() {
            DateTime reportDate = DateTime.ParseExact(getOptions("date"), "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

			var addNewJournalJob = new _NewJournalInsosysToSurplus(connections);
			addNewJournalJob.setOptions("journalids", null);
			addNewJournalJob.setOptions("filters", "('" + reportDate.ToString("yyyy") + "-01-01 00:00:00' <= jurnal_bookdate and jurnal_bookdate <= '" + reportDate.ToString("yyyy") + "-12-31 23:59:59')");
			addNewJournalJob.run(false);

			var updateJournalJob = new _UpdateJournalInsosysToSurplus(connections);
			updateJournalJob.setOptions("journalids", null);
			updateJournalJob.setOptions("filters", "modified_dt >= '2023-01-02 00:00:00'"); //cut-off date of the first migrations
			updateJournalJob.run(false);

			var reportInsosys = get_report_insosys(reportDate);
            var reportSurplus = get_report_surplus(reportDate);

			var reportAccIdsInsosys = reportInsosys.Select(a => Utils.obj2str(a["acc_id"])).ToArray();
            var reportAccIdsSurplus = reportSurplus.Select(a => Utils.obj2str(a["accountid"])).ToArray();

			var missingAccIds_in_surplus = reportAccIdsInsosys.Where(a => !reportAccIdsSurplus.Contains(a)).ToArray();
            var missingAccIds_in_insosys = reportAccIdsSurplus.Where(a => !reportAccIdsInsosys.Contains(a)).ToArray();

			var accIdsDiffBalance = reportInsosys.Where(iRep =>
                reportSurplus.Any(sRep =>
                    iRep["acc_id"].ToString() == sRep["accountid"].ToString()
					&& (
                        Utils.obj2decimal(iRep["begbal"]) != Utils.obj2decimal(sRep["begbal"])
                        || Utils.obj2decimal(iRep["debit"]) != Utils.obj2decimal(sRep["debit"])
                        || Utils.obj2decimal(iRep["kredit"]) != Utils.obj2decimal(sRep["kredit"])
                    )
                )
            ).ToArray();

			var accIdsDiffBalance_surplus = reportSurplus.Where(sRep =>
                reportInsosys.Any(iRep =>
                    iRep["acc_id"].ToString() == sRep["accountid"].ToString()
                    && (
                        Utils.obj2decimal(iRep["begbal"]) != Utils.obj2decimal(sRep["begbal"])
                        || Utils.obj2decimal(iRep["debit"]) != Utils.obj2decimal(sRep["debit"])
                        || Utils.obj2decimal(iRep["kredit"]) != Utils.obj2decimal(sRep["kredit"])
                    )
                )
            ).ToArray();

			foreach(var row in accIdsDiffBalance) {
				var row_surplus = accIdsDiffBalance_surplus.First(a => a["accountid"].ToString() == row["acc_id"].ToString());

				//string dk;
				//if()
				//var insosysJurnals = listJurnalByAcc_insosys(reportDate, row["acc_id"].ToString(), );
            }
        }

        private RowData<ColumnName, object>[] get_report_insosys(DateTime reportDate) {
            var result = QueryUtils.executeQuery(
				_insosysConnection,
                @"
					SELECT DISTINCT acc_id INTO #temp_acc_1 FROM transaksi_jurnalsaldo  
					UNION 
					SELECT DISTINCT acc_id FROM transaksi_jurnaldetil ORDER BY acc_id;

					SELECT DISTINCT acc_id, isnull((select mark from INSOSYS1.E_FRM.dbo.master_acc_gl_sign where acc_id = #temp_acc_1.acc_id), 1) as mark
					INTO #temp_acc
					FROM #temp_acc_1
					ORDER BY acc_id;

					SELECT transaksi_jurnaldetil.jurnal_id,  transaksi_jurnaldetil.jurnaldetil_dk,
						transaksi_jurnaldetil.acc_id, 
						(case #temp_acc.mark when 1 then transaksi_jurnaldetil.jurnaldetil_foreign else transaksi_jurnaldetil.jurnaldetil_foreign*-1 end) as jurnaldetil_foreign, 
						(case #temp_acc.mark when 1 then transaksi_jurnaldetil.jurnaldetil_idr else transaksi_jurnaldetil.jurnaldetil_idr*-1 end) as jurnaldetil_idr, 
						transaksi_jurnal.jurnal_bookdate, transaksi_jurnal.jurnal_descr,(month(jurnal_bookdate)) as bulan,(year(jurnal_bookdate)) as tahun 
					INTO #temp_gl1
					FROM 
						transaksi_jurnaldetil 
						INNER JOIN transaksi_jurnal ON transaksi_jurnaldetil.jurnal_id = transaksi_jurnal.jurnal_id 
						INNER JOIN #temp_acc ON transaksi_jurnaldetil.acc_id = #temp_acc.acc_id
					WHERE (transaksi_jurnal.jurnal_isposted = 1 AND  transaksi_jurnal.jurnal_isdisabled = 0) 
						AND year(transaksi_jurnal.jurnal_bookdate) = year(@date)
					UNION ALL
					SELECT 'BEG_BAL',' ', a.acc_id, 
						(case #temp_acc.mark when 1 then a.jurnalsaldo_foreign else a.jurnalsaldo_foreign*-1 end) as jurnalsaldo_foreign, 
						(case #temp_acc.mark when 1 then a.jurnalsaldo_idr else a.jurnalsaldo_idr*-1 end) as jurnalsaldo_idr, 
						jurnalsaldo_createdate as jurnal_bookdate, 'Beginning Balance',(month(jurnalsaldo_createdate)) as bulan,(year(jurnalsaldo_createdate)) as tahun 
					FROM 
						transaksi_jurnalsaldo a
						INNER JOIN #temp_acc ON a.acc_id = #temp_acc.acc_id
					WHERE year(a.jurnalsaldo_createdate) = year(@date)
					AND a.jurnalsaldo_idr <> 0;

					SELECT #temp_acc.acc_id, b.acc_name,
						isnull ((select sum(jurnaldetil_idr) from #temp_gl1 where (jurnal_id = 'BEG_BAL' and acc_id = #temp_acc.acc_id) or
						(acc_id = #temp_acc.acc_id and bulan < month(@date) and tahun = year(@date))   ), 0) 
						as begbal,

						isnull ((select sum(jurnaldetil_idr) from transaksi_jurnaldetil  INNER JOIN
						transaksi_jurnal ON transaksi_jurnaldetil.jurnal_id = transaksi_jurnal.jurnal_id 
						where (transaksi_jurnal.jurnal_isposted = 1 AND  transaksi_jurnal.jurnal_isdisabled = 0) and
						transaksi_jurnaldetil.jurnaldetil_dk = 'D' and transaksi_jurnaldetil.acc_id = #temp_acc.acc_id
						and month(transaksi_jurnal.jurnal_bookdate) = month(@date) and year(transaksi_jurnal.jurnal_bookdate) = year(@date)), 0) as debit, 

						isnull ((select (sum(jurnaldetil_idr)*-1) from transaksi_jurnaldetil  INNER JOIN
						transaksi_jurnal ON transaksi_jurnaldetil.jurnal_id = transaksi_jurnal.jurnal_id 
						where (transaksi_jurnal.jurnal_isposted = 1 AND  transaksi_jurnal.jurnal_isdisabled = 0) and
						transaksi_jurnaldetil.jurnaldetil_dk = 'K' and transaksi_jurnaldetil.acc_id = #temp_acc.acc_id
						and month(transaksi_jurnal.jurnal_bookdate) = month(@date) and year(transaksi_jurnal.jurnal_bookdate) = year(@date)), 0) as kredit
					INTO #temp_gl2
					FROM 
						#temp_acc 
						LEFT JOIN master_acc b ON #temp_acc.acc_id = b.acc_id
					GROUP BY #temp_acc.acc_id, b.acc_name
					ORDER BY #temp_acc.acc_id;

					SELECT 
						acc_id, acc_name, begbal, debit, kredit, endbal = (begbal + debit - kredit), total = (debit - kredit)
					INTO #report_result
					FROM 
						#temp_gl2
					WHERE 
						begbal <> 0 and debit <> 0 and kredit <> 0
					order by 
						acc_id
					;
					
					SELECT
						acc_id, acc_name,
						ROUND(begbal, 4) as begbal,
						ROUND(debit, 4) as debit,
						ROUND(kredit, 4) as kredit,
						ROUND(endbal, 4) as endbal,
						ROUND(total, 4) as total
					FROM #report_result;
				",
                new Dictionary<string, object> { { "@date", reportDate.ToString("yyyy-MM-dd HH:mm:ss") } }
            );

            return result;
        }

        private RowData<ColumnName, object>[] get_report_surplus(DateTime reportDate) {
            var result = QueryUtils.executeQuery(
				_surplusConnection,
                @"
					CREATE TEMP TABLE IF NOT EXISTS temp_acc_1 AS
						(SELECT DISTINCT tjs.accountid FROM transaction_journal_saldo tjs)
						UNION
						(SELECT DISTINCT tjd.accountid FROM transaction_journal_detail tjd ORDER BY tjd.accountid)
					;
	
					CREATE TEMP TABLE IF NOT EXISTS temp_acc AS
						SELECT DISTINCT accountid, coalesce((select sign from master_account_general_ledger_sign where accountid = temp_acc_1.accountid), 1) AS sign
						FROM temp_acc_1
						ORDER BY accountid
					;

					CREATE TEMP TABLE IF NOT EXISTS temp_gl1 AS
						SELECT
							tjd.tjournalid,
							tjd.dk,
							tjd.accountid,
							(case ta.sign when 1 then tjd.foreignamount else tjd.foreignamount*-1 end) as foreignamount,
							(case ta.sign when 1 then tjd.idramount else tjd.idramount*-1 end) as idramount,
							tj.bookdate, tj.description,(date_part('month', tj.bookdate)) as bulan,(date_part('year', tj.bookdate)) as tahun
						FROM
							transaction_journal_detail tjd
							INNER join transaction_journal tj ON tjd.tjournalid  = tj.tjournalid
							INNER JOIN temp_acc ta ON tjd.accountid = ta.accountid
						WHERE
							(tj.is_posted = true AND  tj.is_disabled = false)
							AND (date_part('year', tj.bookdate)) = @param_year
						UNION ALL
						SELECT
							'BEG_BAL',
							' ',
							tjs.accountid,
							(CASE tacc.sign WHEN 1 THEN tjs.foreignamount ELSE tjs.foreignamount*-1 END) AS foreignamount,
							(CASE tacc.sign WHEN 1 THEN tjs.idramount ELSE tjs.idramount*-1 END) AS idramount,
							tjs.created_date as bookdate,
							'Beginning Balance',
							(date_part('month', tjs.created_date)) AS bulan,
							(date_part('year', tjs.created_date)) AS tahun
						FROM
							transaction_journal_saldo tjs
							INNER JOIN temp_acc tacc ON tjs.accountid = tacc.accountid
						WHERE
							(date_part('year', tjs.created_date)) = @param_year
							AND tjs.idramount <> 0
					;
					
					CREATE TEMP TABLE IF NOT EXISTS temp_journal_total_D AS
						SELECT
							tjd.accountid
							,(date_part('month', tj.bookdate)) AS month
							,(date_part('year', tj.bookdate)) AS year
							,SUM(idramount) AS idramount
						from
							transaction_journal_detail tjd
							INNER join transaction_journal tj ON tjd.tjournalid = tj.tjournalid
						WHERE
							(tj.is_posted= true AND  tj.is_disabled = false)
							AND tjd.dk = 'D'
						group by
							tjd.accountid
							,(date_part('month', tj.bookdate))
							,(date_part('year', tj.bookdate))
					;
	
					CREATE TEMP TABLE IF NOT EXISTS temp_journal_total_K AS
						SELECT
							tjd.accountid
							,(date_part('month', tj.bookdate)) AS month
							,(date_part('year', tj.bookdate)) AS year
							,(SUM(idramount) * -1) AS idramount
						FROM
							transaction_journal_detail tjd
							INNER join transaction_journal tj ON tjd.tjournalid = tj.tjournalid
						WHERE
							(tj.is_posted= true AND  tj.is_disabled = false)
							AND tjd.dk = 'K'
						GROUP BY
							tjd.accountid
							,(date_part('month', tj.bookdate))
							,(date_part('year', tj.bookdate))
					;
	
					CREATE TEMP TABLE IF NOT EXISTS beg_bal AS
						SELECT
							tmp.accountid
							,COALESCE(
								(
								   SUM(gl.idramount)
								)
							, 0) AS begbal
							FROM 
	        					temp_acc tmp
							INNER JOIN temp_gl1 gl ON gl.accountid=tmp.accountid
							WHERE gl.tjournalid = 'BEG_BAL' OR
									(
										gl.bulan < @param_month
										and gl.tahun = @param_year
									)
							GROUP BY 
	        					tmp.accountid
							ORDER BY tmp.accountid
					;
	
					CREATE TEMP TABLE IF NOT EXISTS temp_gl2_try AS
						SELECT
							tmp.accountid
							,COALESCE(
								(
									SELECT
										SUM(idramount)
									FROM
										temp_journal_total_D
									WHERE
										temp_journal_total_D.accountid = tmp.accountid
										AND temp_journal_total_D.month = @param_month
										AND temp_journal_total_D.year = @param_year
								)
							, 0) AS debit
							,COALESCE(
							(
									SELECT
										SUM(idramount)
									FROM
										temp_journal_total_K
									WHERE
										temp_journal_total_K.accountid = tmp.accountid
										AND temp_journal_total_K.month = @param_month
										AND temp_journal_total_K.year = @param_year
							)
							, 0) AS kredit
						FROM
							temp_acc tmp
						ORDER BY tmp.accountid
					;
	
					CREATE TEMP TABLE IF NOT EXISTS temp_gl2 AS
						SELECT
							tmp.accountid,
							acc.name,
							bg.begbal,
							ty.kredit,
							ty.debit                            
						FROM
							temp_acc tmp
	   					INNER JOIN master_account acc ON tmp.accountid=acc.accountid
						LEFT JOIN beg_bal bg ON tmp.accountid = bg.accountid
						LEFT JOIN temp_gl2_try ty ON tmp.accountid = ty.accountid
						ORDER BY tmp.accountid
					;

					SELECT 
						accountid, 
						name, 
						cast(begbal as numeric(19,4)) as begbal, 
						cast(debit as numeric(19,4)) as debit, 
						cast(kredit as numeric(19,4)) as kredit, 
						cast((begbal + debit - kredit) as numeric(19,4)) as endbal, 
						cast((debit - kredit) as numeric(19,4)) as total
					FROM temp_gl2
					where 
						begbal <> 0 and debit <> 0 and kredit <> 0
					order by accountid
					;
				", 
				new Dictionary<string, object> {
                    { "@param_month", reportDate.ToString("MM") },
                    { "@param_year", reportDate.ToString("yyyy") } 
				}
			);

            return result;
        }

        private Dictionary<string, object>[] listJurnalByAcc_insosys(DateTime reportDate, string accountId, string dk) {
            var result = QueryUtils.executeQuery(
                _insosysConnection,
                @"
					select 
						tj.jurnal_id ,
						SUBSTRING(tj.jurnal_id,1,2) + 'D' + SUBSTRING(tj.jurnal_id,3,999) + CONVERT(varchar(10), tjd.jurnaldetil_line) as jurnal_id_detail,
						tjd.acc_id ,
						tjd.jurnaldetil_dk ,
						tjd.jurnaldetil_idr ,
						tj.jurnal_bookdate
					--	,tj.created_dt
					--	,tj.jurnal_iscreatedate
						,tj.modified_dt
						,tj.jurnal_isposted 
						,tj.jurnal_isdisableddt
					from
						transaksi_jurnal tj 
						join transaksi_jurnaldetil tjd on tjd.jurnal_id = tj.jurnal_id 
					WHERE 
						1=1
						and ('2022-01-01 00:00:00' <= tj.jurnal_bookdate and tj.jurnal_bookdate <= '2022-12-31 23:59:59') 
						and (tj.jurnal_isposted = 1 and tj.jurnal_isdisabled = 0)
						and tjd.jurnaldetil_dk = @dk
						and acc_id = @acc_id
					order by tj.jurnal_id, jurnal_id_detail  
					;
				",
                new Dictionary<string, object> { 
					{ "@bookdate_from", reportDate.ToString("yyyy") + "-01-01 00:00:00" } ,
                    { "@bookdate_to", reportDate.ToString("yyyy-MM-dd") + " 23:59:59" } ,
                    { "@dk", dk } ,
                    { "@acc_id", accountId }
                }
            );

            return result;
        }
    }
}
