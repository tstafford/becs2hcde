#!/bin/bash
bbcs_data=$(mktemp)
mysql_data=$(mktemp)

# check that the config file exists
if [ -f "$(dirname $0)/config.sh" ] ; then
        . $(dirname $0)/config.sh
else
        echo "config.sh does not exists, see config.sh.example"
        exit 1
fi

# called to exit with a friendly message
quit() {
        [[ "$locked" == "true" ]] && rmdir "$lock_file"
	rm "$bbcs_data"
	rm "$mysql_data"
        echo $*
        exit
}

#pre-flight check
if [ -z "$bbcs_dsn" ]
then
	quit "bbcs_dsn variable not set, check config.sh"
fi
if [ -z "$mysql_dsn" ]
then
	quit "mysql_dsn variable not set, check config.sh"
fi
if [ -z "$lock_file" ]
then
	quit "lock_file variable not set, check config.sh"
fi
if [ -z "$pdid_wb" ]
then
	quit "pdid_wb not set, check config.sh"
fi
if [ -z "$pdid_2rbc" ]
then
	quit "pdid_2rbc not set, check config.sh"
fi
if [ -z "$pdid_plt" ]
then
	quit "pdid_plt not set, check config.sh"
fi
if [ -z "$pdid_pls" ]
then
	quit "pdid_pls not set, check config.sh"
fi

# if everything is ok, the last thing we do is lock
if mkdir "$lock_file"
then
        locked="true"
        trap "quit" EXIT
else
	quit "$lock_file exists"
fi
#end pre-flight check


# query the iseries and then load them into the incoming table on the interim database
# query to get product eligibility
# ProductEligibility pe, Deferrals d
echo "fetching becs data"
cat << EOM | tr '\n' ' ' | isql iseries -v -b -x0x09 > "$bbcs_data"
SELECT final.actno,
       final.prod,
       CASE
              WHEN final.defer_date >= final.elig_date THEN final.defer_date
              ELSE final.elig_date
       END
FROM   (
                 SELECT    p.actno,
                           p.prod,
                           COALESCE(Varchar_format(pe.elig_date,'YYYY-MM-DD'),'0000-00-00') elig_date,
                           p.dadfdate                              defer_date
                 FROM      (
                                  SELECT actno,
                                         hack.prod prod,
                                         case when digits(dadfcn)||digits(dadfyr)||'-'||digits(dadfmo)||'-'||digits(dadfda) = '9999-99-99' then '5000-01-01' else digits(dadfcn)||digits(dadfyr)||'-'||digits(dadfmo)||'-'||digits(dadfda) end dadfdate
                                  FROM   dacntsl1,
                                         (
                                                SELECT 'WB' prod
                                                FROM   sysibm.sysdummy1
                                                UNION
                                                SELECT '2RBC' prod
                                                FROM   sysibm.sysdummy1
                                                UNION
                                                SELECT 'PLT' prod
                                                FROM   sysibm.sysdummy1
                                                UNION
                                                SELECT 'PLS' prod
                                                FROM   sysibm.sysdummy1 ) hack ) p
                 LEFT JOIN
                           (
                                    SELECT   ddacct actno,
                                             CASE dntprd
                                                      WHEN '$pdid_wb' THEN 'WB'
                                                      WHEN '$pdid_2rbc' THEN '2RBC'
                                                      WHEN '$pdid_plt' THEN 'PLT'
                                                      WHEN '$pdid_pls' THEN 'PLS'
                                                      ELSE dntprd
                                                                        || ' is not defined'
                                             END PROC,
                                             max(Date(Digits(ddldcn)
                                                      ||Digits(ddldyr)
                                                      ||'-'
                                                      ||Digits(ddldmo)
                                                      ||'-'
                                                      ||Digits(ddldda)) + dndays days) elig_date
                                    FROM     dondtal1
                                    JOIN     dndmstl5 on
                                    (dddpcd=dnfprd and ddspcd=dnfspc and ddtucd='') 
                                    or 
                                    (dnfprd='' and dnfspc='' and ddtucd=dnftuc)
                                    WHERE    dntprd IN ('$pdid_wb',
                                                        '$pdid_2rbc',
                                                        '$pdid_plt',
                                                        '$pdid_pls')
                                    and dntspc=''
                                    AND      ddldyr BETWEEN 0 AND      99
                                    AND      ddldmo BETWEEN 0 AND      12
                                    AND      ( (
                                                               ddldmo IN (1,3,5,7,8,10,12)
                                                      AND      ddldda BETWEEN 0 AND      31)
                                             OR       (
                                                               ddldmo IN (4,6,9,11)
                                                      AND      ddldda BETWEEN 0 AND      30)
                                             OR       (
                                                               ddldmo=2
                                                      AND      ddldda BETWEEN 0 AND      29) )
                                    GROUP BY ddacct,
                                             dntprd) pe
                 ON        p.actno = pe.actno
                 AND       pe.PROC=p.prod
) final
WHERE  (
              final.elig_date!='0000-00-00'
       AND    final.defer_date!='0000-00-00')
OR     (
              final.elig_date='0000-00-00'
       AND    final.defer_date != '0000-00-00')
OR     (
              final.elig_date!='0000-00-00'
       AND    final.defer_date = '0000-00-00')
EOM

echo "updating idb"
mysql --local-infile=1 -h"$mysql_host" -u"$mysql_username" -p"$mysql_password" "$mysql_database" -e "LOAD DATA LOCAL INFILE '$bbcs_data' replace INTO TABLE Incoming;
insert into DonorEligibility (becs_id,\`procedure\`,eligible_date) select i.becs_id, i.\`procedure\`, i.eligible_date from Incoming i left join DonorEligibility de on i.becs_id=de.becs_id and i.\`procedure\`=de.\`procedure\` where i.eligible_date != de. eligible_date or de.eligible_date is null on duplicate key update becs_id=i.becs_id, \`procedure\`=i.\`procedure\`, eligible_date=i.eligible_date, updated=CURRENT_TIMESTAMP;
"

if ! [ -z "$save_file" ] 
then
	echo "save file was set" 
	cp -v "$bbcs_data" "$save_file"
fi