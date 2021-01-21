## Questions

1. Tests last week (current date 21/01/2021)
```sql
  select count(*)
    from hearing_tests ht 
   where submission_timestamp > (now() - interval '7 DAY')
```
In the last week were made zero tests
```sql
  select count(*)
    from hearing_tests ht 
   where submission_timestamp > (now() - interval '31 DAY')
```
and in the last month 9 tests were made.

2. Hearing loss level per app  

```sql
  select temp.source_app,
  		 temp.hearing_loss,
  		 count(*)
    from ( select 
 		 distinct ht.source_app,
		   		  'no loss' as hearing_loss,
		  		  ht.user_id
		     from hearing_tests ht 
		    inner join insights i 
		       on ht.id = i.hearing_test_id
		    where round(i.pta4) < 26
		 union
		   select
		 distinct ht.source_app,
		  		  'mild loss' as hearing_loss,
		  		  ht.user_id
		     from hearing_tests ht 
	 	    inner join insights i 
		       on ht.id = i.hearing_test_id
		    where round(i.pta4) between 26 and 40
		 union
		   select
		 distinct ht.source_app,
		  	 	  'moderate loss' as hearing_loss,
		  		  ht.user_id
		     from hearing_tests ht 
		    inner join insights i 
		       on ht.id = i.hearing_test_id
		    where round(i.pta4) between 41 and 60
		 union
		   select
		 distinct ht.source_app,
		  		  'severe loss' as hearing_loss,
		  		 ht.user_id
		     from hearing_tests ht 
		    inner join insights i 
		       on ht.id = i.hearing_test_id
		    where round(i.pta4) between 61 and 80
		 union
		   select
		 distinct ht.source_app,
		  		  'profound loss' as hearing_loss,
		  		  ht.user_id
		     from hearing_tests ht 
		    inner join insights i 
		       on ht.id = i.hearing_test_id
		    where round(i.pta4) > 80 ) temp 
group by temp.source_app,
  		 temp.hearing_loss
order by temp.source_app,
  		 temp.hearing_loss
```

|source_app|hearing_loss|count|
|----------|------------|-----|
|app-06MTI08AF9|mild loss|36|
|app-06MTI08AF9|no loss|326|
|app-0K9RI0E20K|mild loss|29|
|app-0K9RI0E20K|no loss|292|
|app-136HQQ4STV|mild loss|40|
|app-136HQQ4STV|no loss|313|
|app-1H8W6H5LAP|mild loss|34|
|app-1H8W6H5LAP|no loss|332|
|app-2AWWMK8DSK|mild loss|36|
|app-2AWWMK8DSK|no loss|328|
|app-2S3JMNU2B8|mild loss|25|
|app-2S3JMNU2B8|no loss|323|
|app-2VB8LJD0IG|mild loss|28|
|app-2VB8LJD0IG|no loss|326|
|app-3EXAXYSXVN|mild loss|35|
|app-3EXAXYSXVN|no loss|322|
|app-3N277OVC8A|mild loss|37|
|app-3N277OVC8A|no loss|329|
|app-3P50LD7JKA|mild loss|30|
|app-3P50LD7JKA|no loss|300|
|app-3XKKKIPCWS|mild loss|33|
|app-3XKKKIPCWS|no loss|336|
|app-4B1BP45AQJ|mild loss|27|
|app-4B1BP45AQJ|no loss|318|
|app-4F0AISBKIZ|mild loss|43|
|app-4F0AISBKIZ|no loss|312|
|app-4NERQ3SU54|mild loss|31|
|app-4NERQ3SU54|no loss|313|
|app-4Q25IMGECS|mild loss|35|
|app-4Q25IMGECS|no loss|325|
|app-4RAX62KSU5|mild loss|25|
|app-4RAX62KSU5|no loss|333|
|app-50JCF9NP5L|mild loss|43|
|app-50JCF9NP5L|no loss|336|
|app-573YU7W3P3|mild loss|36|
|app-573YU7W3P3|no loss|314|
|app-57R16TS4VJ|mild loss|37|
|app-57R16TS4VJ|no loss|307|
|app-5JVRCHFCSN|mild loss|34|
|app-5JVRCHFCSN|no loss|294|
|app-5PP335S3Z8|mild loss|27|
|app-5PP335S3Z8|no loss|285|
|app-5WQGTW2P6B|mild loss|31|
|app-5WQGTW2P6B|no loss|331|
|app-65TE0PQKE4|mild loss|32|
|app-65TE0PQKE4|no loss|311|
|app-6JWSZXAB13|mild loss|34|
|app-6JWSZXAB13|no loss|301|
|app-6Q6GJ4VCB9|mild loss|27|
|app-6Q6GJ4VCB9|no loss|311|
|app-71KHI2C3LG|mild loss|25|
|app-71KHI2C3LG|no loss|305|
|app-75Y5PZJ9WL|mild loss|27|
|app-75Y5PZJ9WL|no loss|347|
|app-7DSN1GV3M0|mild loss|28|
|app-7DSN1GV3M0|no loss|289|
|app-7L1DX8F67V|mild loss|26|
|app-7L1DX8F67V|no loss|305|
|app-88GTEQU1Z2|mild loss|33|
|app-88GTEQU1Z2|no loss|329|
|app-8T6OAGNMRW|mild loss|33|
|app-8T6OAGNMRW|no loss|321|
|app-980KD5AB6F|mild loss|33|
|app-980KD5AB6F|no loss|335|
|app-99PBYKQ4Y5|mild loss|26|
|app-99PBYKQ4Y5|no loss|310|
|app-9TIL0A9RWZ|mild loss|39|
|app-9TIL0A9RWZ|no loss|318|
|app-AD8SPM7O01|mild loss|31|
|app-AD8SPM7O01|no loss|279|
|app-AFP6HF1XIV|mild loss|36|
|app-AFP6HF1XIV|no loss|298|
|app-AINBVN5R0B|mild loss|28|
|app-AINBVN5R0B|no loss|303|
|app-ANZUPUB7I4|mild loss|31|
|app-ANZUPUB7I4|no loss|342|
|app-ARCYU2PRSO|mild loss|22|
|app-ARCYU2PRSO|no loss|294|
|app-B3XP94AW41|mild loss|37|
|app-B3XP94AW41|no loss|331|
|app-BH1SDLSDLG|mild loss|35|
|app-BH1SDLSDLG|no loss|331|
|app-BIDHYAHWAM|mild loss|45|
|app-BIDHYAHWAM|no loss|311|
|app-BRBDG9NRT4|mild loss|38|
|app-BRBDG9NRT4|no loss|319|
|app-C8SA2G22KY|mild loss|36|
|app-C8SA2G22KY|no loss|313|
|app-CER340PH38|mild loss|36|
|app-CER340PH38|no loss|303|
|app-CRPRFW95S9|mild loss|54|
|app-CRPRFW95S9|no loss|319|
|app-D9I85YSH02|mild loss|43|
|app-D9I85YSH02|no loss|333|
|app-DB7HU70YJC|mild loss|31|
|app-DB7HU70YJC|no loss|297|
|app-DIMH39YXRR|mild loss|33|
|app-DIMH39YXRR|no loss|298|
|app-DMARLC21PV|mild loss|38|
|app-DMARLC21PV|no loss|316|
|app-DW9EF24ZC1|mild loss|36|
|app-DW9EF24ZC1|no loss|309|
|app-DYM10ZG2EM|mild loss|42|
|app-DYM10ZG2EM|no loss|306|
|app-DZKQPHWVJI|mild loss|34|
|app-DZKQPHWVJI|no loss|339|
|app-E030FK5AZ2|mild loss|31|
|app-E030FK5AZ2|no loss|310|
|app-E16TLISTQ6|mild loss|31|
|app-E16TLISTQ6|no loss|309|
|app-E9FMYFYYMA|mild loss|34|
|app-E9FMYFYYMA|no loss|293|
|app-EQ3PAL506F|mild loss|33|
|app-EQ3PAL506F|no loss|303|
|app-ESQOH7WCHN|mild loss|32|
|app-ESQOH7WCHN|no loss|293|
|app-F5HY10F68L|mild loss|33|
|app-F5HY10F68L|no loss|301|
|app-F8J71I0ARD|mild loss|35|
|app-F8J71I0ARD|no loss|276|
|app-FGWTZNBJ4C|mild loss|34|
|app-FGWTZNBJ4C|no loss|332|
|app-FN1PKGKCXZ|mild loss|42|
|app-FN1PKGKCXZ|no loss|287|
|app-FQ1WB2BM02|mild loss|36|
|app-FQ1WB2BM02|no loss|316|
|app-FVTOSS1JY5|mild loss|38|
|app-FVTOSS1JY5|no loss|293|
|app-G1UFL9I44Q|mild loss|27|
|app-G1UFL9I44Q|no loss|296|
|app-G8O1ELLGE6|mild loss|45|
|app-G8O1ELLGE6|no loss|328|
|app-GIRTL1NTKV|mild loss|30|
|app-GIRTL1NTKV|no loss|359|
|app-H2FTRJ0CGD|mild loss|29|
|app-H2FTRJ0CGD|no loss|300|
|app-H8AJQ7BNXS|mild loss|28|
|app-H8AJQ7BNXS|no loss|314|
|app-HOS4683LKX|mild loss|30|
|app-HOS4683LKX|no loss|351|
|app-I5M07SBATN|mild loss|29|
|app-I5M07SBATN|no loss|298|
|app-I5RQFJHY7Y|mild loss|32|
|app-I5RQFJHY7Y|no loss|324|
|app-IBWPYA7LCB|mild loss|34|
|app-IBWPYA7LCB|no loss|299|
|app-IG6UYBBB4W|mild loss|25|
|app-IG6UYBBB4W|no loss|307|
|app-IGACF2IU62|mild loss|33|
|app-IGACF2IU62|no loss|317|
|app-IZEQII7AS1|mild loss|33|
|app-IZEQII7AS1|no loss|306|
|app-JLABT2DS30|mild loss|26|
|app-JLABT2DS30|no loss|302|
|app-JNGPHKKMHX|mild loss|27|
|app-JNGPHKKMHX|no loss|308|
|app-JQ0602CSXD|mild loss|31|
|app-JQ0602CSXD|no loss|317|
|app-KI27ZMDG0Z|mild loss|37|
|app-KI27ZMDG0Z|no loss|327|
|app-KJVZCT24Y8|mild loss|40|
|app-KJVZCT24Y8|no loss|289|
|app-KMK9SWSYDS|mild loss|24|
|app-KMK9SWSYDS|no loss|290|
|app-KPFOEO3NF4|mild loss|30|
|app-KPFOEO3NF4|no loss|291|
|app-KX0NI7R2RO|mild loss|45|
|app-KX0NI7R2RO|no loss|330|
|app-KY4F26DX97|mild loss|33|
|app-KY4F26DX97|no loss|318|
|app-LEOYI71KX0|mild loss|39|
|app-LEOYI71KX0|no loss|314|
|app-M73T6BGP58|mild loss|31|
|app-M73T6BGP58|no loss|297|
|app-M7LQ5LSVLJ|mild loss|41|
|app-M7LQ5LSVLJ|no loss|284|
|app-MFAUJX2DU9|mild loss|45|
|app-MFAUJX2DU9|no loss|318|
|app-MFAWSBVEDY|mild loss|30|
|app-MFAWSBVEDY|no loss|332|
|app-MW3B2FKBB1|mild loss|24|
|app-MW3B2FKBB1|no loss|315|
|app-N54OHV32IQ|mild loss|35|
|app-N54OHV32IQ|no loss|276|
|app-NN7AJEN6RD|mild loss|26|
|app-NN7AJEN6RD|no loss|272|
|app-NRORKAM71X|mild loss|35|
|app-NRORKAM71X|no loss|322|
|app-NT6T08MN59|mild loss|36|
|app-NT6T08MN59|no loss|333|
|app-O7SQL6AZZW|mild loss|31|
|app-O7SQL6AZZW|no loss|304|
|app-OBQ1MOEYB3|mild loss|36|
|app-OBQ1MOEYB3|no loss|326|
|app-OGHT22Y6YT|mild loss|34|
|app-OGHT22Y6YT|no loss|346|
|app-OOWVYQHMLE|mild loss|24|
|app-OOWVYQHMLE|no loss|285|
|app-P7APAVSHN1|mild loss|40|
|app-P7APAVSHN1|no loss|307|
|app-PB629VNMH3|mild loss|32|
|app-PB629VNMH3|no loss|332|
|app-PC5H128DU9|mild loss|38|
|app-PC5H128DU9|no loss|300|
|app-PEZLCE0MIV|mild loss|29|
|app-PEZLCE0MIV|no loss|319|
|app-Q33MJS499H|mild loss|35|
|app-Q33MJS499H|no loss|306|
|app-QC0XC9FWIU|mild loss|41|
|app-QC0XC9FWIU|no loss|334|
|app-QMZYEU1MQS|mild loss|36|
|app-QMZYEU1MQS|no loss|307|
|app-QOL0A6GBEO|mild loss|44|
|app-QOL0A6GBEO|no loss|306|
|app-QRPKTZC8S8|mild loss|35|
|app-QRPKTZC8S8|no loss|304|
|app-QYJ1SJ48XK|mild loss|35|
|app-QYJ1SJ48XK|no loss|328|
|app-QZZSHEYQMM|mild loss|26|
|app-QZZSHEYQMM|no loss|306|
|app-R2UBW2WKOL|mild loss|24|
|app-R2UBW2WKOL|no loss|303|
|app-RLX42TH31C|mild loss|31|
|app-RLX42TH31C|no loss|302|
|app-ROOM69NRFQ|mild loss|30|
|app-ROOM69NRFQ|no loss|296|
|app-RYF38NFRZ4|mild loss|30|
|app-RYF38NFRZ4|no loss|296|
|app-S5AQZIB0EJ|mild loss|30|
|app-S5AQZIB0EJ|no loss|306|
|app-S6GE1YB1B9|mild loss|39|
|app-S6GE1YB1B9|no loss|294|
|app-T51AFPV2FC|mild loss|35|
|app-T51AFPV2FC|no loss|315|
|app-T5TLU9N44U|mild loss|36|
|app-T5TLU9N44U|no loss|279|
|app-TDBPG7UKXH|mild loss|34|
|app-TDBPG7UKXH|no loss|318|
|app-TIVWWG53J8|mild loss|37|
|app-TIVWWG53J8|no loss|306|
|app-TRP5M0Y3PI|mild loss|33|
|app-TRP5M0Y3PI|no loss|325|
|app-U0SWLAT577|mild loss|37|
|app-U0SWLAT577|no loss|334|
|app-U7YJ3QSJJQ|mild loss|46|
|app-U7YJ3QSJJQ|no loss|321|
|app-US97E6I577|mild loss|34|
|app-US97E6I577|no loss|307|
|app-UTVZ31JOKX|mild loss|47|
|app-UTVZ31JOKX|no loss|322|
|app-V6V1YBYWRA|mild loss|38|
|app-V6V1YBYWRA|no loss|304|
|app-VBH16J6GG8|mild loss|30|
|app-VBH16J6GG8|no loss|319|
|app-VC5U49919T|mild loss|35|
|app-VC5U49919T|no loss|299|
|app-VFSFHWRXEZ|mild loss|32|
|app-VFSFHWRXEZ|no loss|269|
|app-VK52H9S6TM|mild loss|34|
|app-VK52H9S6TM|no loss|286|
|app-VLWWPT8ENO|mild loss|39|
|app-VLWWPT8ENO|no loss|331|
|app-W0AQCF5PL8|mild loss|32|
|app-W0AQCF5PL8|no loss|320|
|app-W2U1OFNFNA|mild loss|29|
|app-W2U1OFNFNA|no loss|323|
|app-WCZD8DSSVL|mild loss|27|
|app-WCZD8DSSVL|no loss|294|
|app-WL5R9ONE1E|mild loss|56|
|app-WL5R9ONE1E|no loss|295|
|app-X2HWV7H9PB|mild loss|41|
|app-X2HWV7H9PB|no loss|334|
|app-XBPOIE370F|mild loss|43|
|app-XBPOIE370F|no loss|317|
|app-XJ6GPUSF8R|mild loss|35|
|app-XJ6GPUSF8R|no loss|315|
|app-XPAJXI3FLF|mild loss|37|
|app-XPAJXI3FLF|no loss|336|
|app-XV6MN90LH7|mild loss|39|
|app-XV6MN90LH7|no loss|324|
|app-Y1VJRO0OJK|mild loss|25|
|app-Y1VJRO0OJK|no loss|322|
|app-Y3H7KR87JE|mild loss|36|
|app-Y3H7KR87JE|no loss|295|
|app-Y3TZVFFV9V|mild loss|29|
|app-Y3TZVFFV9V|no loss|319|
|app-Y484TMLMNN|mild loss|33|
|app-Y484TMLMNN|no loss|297|
|app-Y8QIZV20C4|mild loss|32|
|app-Y8QIZV20C4|no loss|320|
|app-YBQJOFP3CJ|mild loss|33|
|app-YBQJOFP3CJ|no loss|304|
|app-YMPFJ9MPQI|mild loss|32|
|app-YMPFJ9MPQI|no loss|291|
|app-Z0M8KLRNRF|mild loss|32|
|app-Z0M8KLRNRF|no loss|306|
|app-ZBMRWJT9EN|mild loss|30|
|app-ZBMRWJT9EN|no loss|300|
|app-ZG2HYA4ARA|mild loss|24|
|app-ZG2HYA4ARA|no loss|303|
   
- How many first tests do we get through each partnership? A first test is the first hearing test that a user takes upon their registration.
    Context: A user can have several devices from different partners, for example, a TV from one partner and headphones from another. And they can do multiple tests in the respective companion apps from these manufacturers. But it is important for us to know what was the first touchpoint - which partner has brought a user to us.
  - One partner could have multiple applications that use the SDK.
  - This count should even include deleted tests
  
3. First tests per partner
```sql
  select first_tests.partner_id,
  		 count(*)
    from ( select ht.partner_id,
		          ht.id 
		     from hearing_tests ht 
		    where not exists ( select 1
		                         from hearing_tests ht2 
		                        where ht.user_id = ht2.user_id
		                          and ht.submission_timestamp > ht2.submission_timestamp ) ) first_tests
group by first_tests.partner_id
order by count(*) desc
```

|partner_id|count|
|----------|-----|
|api-partner-0L11KKPP44|230|
|api-partner-FQDR1M8OOA|227|
|api-partner-1ISXKM40K1|224|
|api-partner-GECMQ2CNZB|221|
|api-partner-5NINYD4LS5|219|
|api-partner-UHKM0WO59T|218|
|api-partner-3MKX33Z732|216|
|api-partner-DLAXYR0Y1F|214|
|api-partner-BHM05SUU39|212|
|api-partner-6JEEU828OW|211|
|api-partner-YK8M79K056|211|
|api-partner-Y0XUSRGSVM|208|
|api-partner-5RN8IS4G8K|205|
|api-partner-USQCW8MBHT|205|
|api-partner-HRAQUTK3II|205|
|api-partner-RTFWJIBXZ7|204|
|api-partner-E6B1PM1GYL|204|
|api-partner-910EYT9JR0|204|
|api-partner-FOVBBZ929M|204|
|api-partner-8BLC96YI7L|202|
|api-partner-ZQ3YG9RT0D|201|
|api-partner-21G7QKJ6HF|201|
|api-partner-7WAEYUAIKR|200|
|api-partner-9A7OW8PI65|200|
|api-partner-M6EY253O78|200|
|api-partner-7U1IG46VEL|200|
|api-partner-ECKK7V0I7A|199|
|api-partner-XWXYKVRQVZ|199|
|api-partner-4ZRJSVRY9I|199|
|api-partner-PS0WJT3TK5|199|
|api-partner-7XW43MX5DW|199|
|api-partner-YHGTROHGOC|194|
|api-partner-GKXNM27BE5|194|
|api-partner-4UUNLZ82ZU|193|
|api-partner-POVPNYKSQ9|192|
|api-partner-WF357GJF35|190|
|api-partner-Y4P06ZU4WV|190|
|api-partner-IYUPBTNFT5|190|
|api-partner-QA1GTNXKE6|187|
|api-partner-7J90FLDYIY|186|
|api-partner-PPSDWTINBH|183|
|api-partner-74WX622UZG|181|
|api-partner-T4JTBYSW4G|181|
|api-partner-VBKNGO7WAC|181|
|api-partner-KHDETM3IAY|181|
|api-partner-XTN6RY4J5S|180|
|api-partner-RL0G4ED1Z8|179|
|api-partner-1AB38R510M|179|
|api-partner-QLK3OEU9TX|177|
|api-partner-528YGRYZQG|164|

4. The following query can be used to retrieve the audiograms of users who made at least 2 hearing tests:

```sql
  select ht.user_id,
  		 a.*
    from hearing_tests ht
   inner join audiograms a
      on ht.id = a.hearing_test_id 
   where exists ( select 1
   		 	        from hearing_tests ht2 
   				   where ht.user_id = ht2.user_id
   				     and ht.id != ht2.id )
order by ht.user_id 
```

- We know that people’s hearing is getting worse with age. There are published standards that outline what hearing ability is expected depending on age. We would like to compare this data to ours so we would like to know what degree of hearing loss our users have per age group?
5. The query bellow points the average of pta4 hearing loss indicator by age
```sql
  select date_part('year', CURRENT_DATE) - u.year_of_birth as age,
  		 avg(i.pta4)
    from insights i
   inner join hearing_tests ht 
      on i.hearing_test_id = ht.id 
   inner join users u
      on ht.user_id = u.id 
group by age
order by age
```

|age|avg|
|---|---|
|19.0|0.03262698579106366016|
|20.0|0.05250947604161363705|
|21.0|0.06887982195511060504|
|22.0|0.02775276709363722133|
|23.0|0.06205820662715380088|
|24.0|0.07016705423402422283|
|25.0|0.20666323288775461221|
|26.0|0.42072414468373475991|
|27.0|0.34481759777319224935|
|28.0|0.43725102714977948385|
|29.0|0.61953768497333846743|
|30.0|0.75949660389099264336|
|31.0|0.84893303150286338266|
|32.0|0.89028511113746489226|
|33.0|1.12100943897938436|
|34.0|1.295875221293042178|
|35.0|1.38697036810572778|
|36.0|1.70503464635282089|
|37.0|1.80085885570094305|
|38.0|2.04249674245943193|
|39.0|2.29730254300271673|
|40.0|2.62939656524244417|
|41.0|2.7862815756121714739|
|42.0|3.0369600936336386|
|43.0|3.31711600416413881|
|44.0|3.47924663458267962|
|45.0|3.71637504974796500|
|46.0|4.143178755763949428|
|47.0|4.19535076260945556|
|48.0|4.7099718114982902|
|49.0|5.0834963067108387|
|50.0|5.3768943942430946|
|51.0|5.7321036932291258|
|52.0|6.1257323856573642|
|53.0|6.5530604804636083|
|54.0|6.8234156342824966|
|55.0|7.3256499345924974|
|56.0|7.7054093865913606|
|57.0|8.0618714372977333|
|58.0|8.5097026327519744|
|59.0|8.9803068382550406|
|60.0|9.5248105574491531|
|61.0|9.9151865110924495|
|62.0|10.2512462287715935|
|63.0|10.9588692167526911|
|64.0|11.3778368983651562|
|65.0|11.9178643830673723|
|66.0|12.3527844950263276|
|67.0|12.9491928696981501|
|68.0|13.4729768239408993|
|69.0|14.1184282823359038|
|70.0|14.7132189799207329|
|71.0|15.2125374722245005|
|72.0|15.8719881039081190|
|73.0|16.3437549781293735|
|74.0|17.0654021368281839|
|75.0|17.7414976028562827|
|76.0|18.1472901663922928|
|77.0|18.8947286492602697|
|78.0|19.5778527631790667|
|79.0|20.4843664082407920|
|80.0|20.9098946930008534|
|81.0|21.5789330786173543|
|82.0|22.3738299277090665|
|83.0|23.1312176448569726|
|84.0|23.7538610869333637|
|85.0|24.4634508735282124|
|86.0|25.2706347310901974|
|87.0|26.1212312320440130|
|88.0|26.8215418311891831|
|89.0|27.4547841920001902|
|90.0|28.3126348728679315|
|91.0|29.1956800992250537|


## Code considerations


- The land page on github contains detailed information about how to run 
  and setup this project
- I've used Spark, for the matter of performance and scalability.
- The code is full of comments and logging information, for this reason 
  I believe that it's easy to read and maintain.
- Every step of the code was tested during development
- The code is logging to console. I believe that it makes the code easy to 
  monitor abd debug.
- The solution is ready to scalate, it just needs to be submitted to a Spark 
  cluster.  
- In case of changes of Hearing document format adjustments would be necessary.
- The insight function can have its implementation changed as long as it has
the same output structure otherwise adjustments will be necessary.
