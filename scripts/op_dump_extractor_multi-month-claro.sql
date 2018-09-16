--NetVision data extract for DIRBS
--Reuse CRC data to generate dummy operator input dump for DIRBS
--This version takes all the 5 months data for one operator = Claro, mapping to Mobilink
--Noite this operator has separate voice and data 
--Note due to subtle differences in the datasets, can't just replace the table names with other Colombian operators - need to examine how data was uploaded


--Source Tables 
--NV_TABLE_DECLARE_BEGIN
--NV_TABLE_DECLARE_TABLE = $src_511d,NV_COLOMBIA_MOVILE_SA_ESP_QTLBASICRAW_ROBERTT_20160115_154744788
--NV_TABLE_DECLARE_TABLE = $src_512d,NV_COLOMBIA_MOVILE_SA_ESP_QTLBASICRAW_TOMASG_20160209_134241322
--NV_TABLE_DECLARE_TABLE = $src_601d,NV_CLARO_COLOMBIA_QTLCALLRECORDS_OP_TOMASG_20160325_084942238
--NV_TABLE_DECLARE_TABLE = $src_602d,NV_CLARO_COLOMBIA_QTLCALLRECORDS_OP_TOMASG_20160414_210330064
--NV_TABLE_DECLARE_TABLE = $src_603d,NV_CLARO_COLOMBIA_QTLCALLRECORDS_OP_TOMASG_20160512_182039485
--NV_TABLE_DECLARE_TABLE = $src_511v,NV_COLOMBIA_MOVILE_SA_ESP_QTLBASICRAW_ROBERTT_20160107_134604151
--NV_TABLE_DECLARE_TABLE = $src_512v,NV_COLOMBIA_MOVILE_SA_ESP_QTLBASICRAW_TOMASG_20160210_201235327
--NV_TABLE_DECLARE_TABLE = $src_601v,NV_CLARO_COLOMBIA_QTLCALLRECORDS_OP_TOMASG_20160324_180708452
--NV_TABLE_DECLARE_TABLE = $src_602v,NV_CLARO_COLOMBIA_QTLCALLRECORDS_OP_TOMASG_20160414_155627684
--NV_TABLE_DECLARE_TABLE = $src_603v,NV_CLARO_COLOMBIA_QTLCALLRECORDS_OP_DSALEK_20160516_125550384

--NV_TABLE_DECLARE_END


--Config Variables
--NV_VARIABLE_DECLARE_BEGIN
--NVN variable syntax doesn't support commas, so add multiple options for plmnid separately. Must be same length as each other and new plmnid (unless want to change imsi length)
--Use a dummy value if there is only 1 plmnid belonging to the home operator in source data:
--NV_VARIABLE_DECLARE_VARIABLE = $existing_plmnid1,string,'0732101'
--NV_VARIABLE_DECLARE_VARIABLE = $existing_plmnid2,string,'!!!!!!!'
--NV_VARIABLE_DECLARE_VARIABLE = $new_plmnid,string,'410010'
--NV_VARIABLE_DECLARE_VARIABLE = $msisdn_pre,string,'92'
--Limiting variables:
--  For testing, set to (respectively):
--  > 100
--  > all
--  > --
--  For complete output, set to:
--  > all
--  > [just leave blank]
--  > [just leave blank]
--NV_VARIABLE_DECLARE_VARIABLE = $mylimit,string,100
--NV_VARIABLE_DECLARE_VARIABLE = $myunion,string,all
--NV_VARIABLE_DECLARE_VARIABLE = $kill,string,--
--NV_VARIABLE_DECLARE_END
-- Copyright (c) 2018 Qualcomm Technologies, Inc.
--
--  All rights reserved.
--
--
--
--  Redistribution and use in source and binary forms, with or without modification, are permitted (subject to the
--  limitations in the disclaimer below) provided that the following conditions are met:
--
--
--  * Redistributions of source code must retain the above copyright notice, this list of conditions and the following
--  disclaimer.
--
--  * Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
--  disclaimer in the documentation and/or other materials provided with the distribution.
--
--  * Neither the name of Qualcomm Technologies, Inc. nor the names of its contributors may be used to endorse or promote
--  products derived from this software without specific prior written permission.
--
--  NO EXPRESS OR IMPLIED LICENSES TO ANY PARTY'S PATENT RIGHTS ARE GRANTED BY THIS LICENSE. THIS SOFTWARE IS PROVIDED BY
--  THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
--  THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
--  COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
--  DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
--  OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
--  TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
--  POSSIBILITY OF SUCH DAMAGE.
--

--log outputs table
create temp table loggy (
myindex int
,descr varchar(80)
,num bigint
,mycomment varchar(100)
)distribute on random;

$kill insert into loggy (myindex, descr, num, mycomment) select 100, 'total count 2015-11 data', count(*), NULL from $src_511d;
$kill insert into loggy (myindex, descr, num, mycomment) select 200, 'total count 2015-12 data', count(*), NULL from $src_512d;
$kill insert into loggy (myindex, descr, num, mycomment) select 300, 'total count 2016-01 data', count(*), NULL from $src_601d;
$kill insert into loggy (myindex, descr, num, mycomment) select 400, 'total count 2016-02 data', count(*), NULL from $src_602d;
$kill insert into loggy (myindex, descr, num, mycomment) select 500, 'total count 2016-03 data', count(*), NULL from $src_603d;
$kill insert into loggy (myindex, descr, num, mycomment) select 600, 'total count 2015-11 voice', count(*), NULL from $src_511v;
$kill insert into loggy (myindex, descr, num, mycomment) select 700, 'total count 2015-12 voice', count(*), NULL from $src_512v;
$kill insert into loggy (myindex, descr, num, mycomment) select 800, 'total count 2016-01 voice', count(*), NULL from $src_601v;
$kill insert into loggy (myindex, descr, num, mycomment) select 900, 'total count 2016-02 voice', count(*), NULL from $src_602v;
$kill insert into loggy (myindex, descr, num, mycomment) select 1000, 'total count 2016-03 voice', count(*), NULL from $src_603v;



--do export
--try to preserve some interest in the data by only rewriting MSISDN/IMSI prefix if matches the original home operator

--**************************looks like all imsis for claro are 16digit, with a leading 0!!

--Nov data has date_logged set to a single date, so use fetch_date_time instead
-- also has a header row in the data
create temp table op1 as
(
(select 
	substr(fetch_date_time,1,8) date
	,imei
	,case
		when substr(imsi,1,length($existing_plmnid1)) in ($existing_plmnid1,$existing_plmnid2) then $new_plmnid || substr(imsi,length($existing_plmnid1)+1) 
		else substr(IMSI,2)
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1)) in ($existing_plmnid1,$existing_plmnid2) then $msisdn_pre || substr(imsi, length($msisdn_pre)+2)
		else substr(IMSI,2)
	end msisdn
from $src_511d
$kill where date between '20151101' and '20151130' and imsi <> 'imsi'
limit $mylimit)
union $myunion
(select
	substr(fetch_date_time,1,8) date
	,imei
	,case
		when substr(imsi,1,length($existing_plmnid1)) in ($existing_plmnid1,$existing_plmnid2) then $new_plmnid || substr(imsi,length($existing_plmnid1)+1) 
		else substr(IMSI,2)
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1)) in ($existing_plmnid1,$existing_plmnid2) then $msisdn_pre || substr(imsi, length($msisdn_pre)+2)
		else substr(IMSI,2)
	end msisdn
from $src_511v
$kill where date between '20151101' and '20151130'
limit $mylimit)
) distribute on random;


--dec data has date_logged as a date, not a varchar
create temp table op2 as
(
(select 
	to_char(date_logged, 'YYYYMMDD') date
	,imei
	,case
		when substr(imsi,1,length($existing_plmnid1)) in ($existing_plmnid1,$existing_plmnid2) then $new_plmnid || substr(imsi,length($existing_plmnid1)+1) 
		else substr(IMSI,2)
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1)) in ($existing_plmnid1,$existing_plmnid2) then $msisdn_pre || substr(imsi, length($msisdn_pre)+2)
		else substr(IMSI,2)
	end msisdn
from $src_512d
$kill where date between '20151201' and '20151231'
limit $mylimit)
union $myunion
(select 
	to_char(date_logged, 'YYYYMMDD') date
	,imei
	,case
		when substr(imsi,1,length($existing_plmnid1)) in ($existing_plmnid1,$existing_plmnid2) then $new_plmnid || substr(imsi,length($existing_plmnid1)+1) 
		else substr(IMSI,2)
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1)) in ($existing_plmnid1,$existing_plmnid2) then $msisdn_pre || substr(imsi, length($msisdn_pre)+2)
		else substr(IMSI,2)
	end msisdn
from $src_512v
$kill where date between '20151201' and '20151231'
limit $mylimit)
) distribute on random;

--jan data table has full timestamp in date_logged
create temp table op3 as
(
(select 
	substr(date_logged,1,8) date
	,imei
	,case
		when substr(imsi,1,length($existing_plmnid1)) in ($existing_plmnid1,$existing_plmnid2) then $new_plmnid || substr(imsi,length($existing_plmnid1)+1) 
		else substr(IMSI,2)
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1)) in ($existing_plmnid1,$existing_plmnid2) then $msisdn_pre || substr(imsi, length($msisdn_pre)+2)
		else substr(IMSI,2)
	end msisdn
from $src_601d
$kill where date between '20160101' and '20160131'
limit $mylimit)
union $myunion
(select 
	date_logged date
	,imei
	,case
		when substr(imsi,1,length($existing_plmnid1)) in ($existing_plmnid1,$existing_plmnid2) then $new_plmnid || substr(imsi,length($existing_plmnid1)+1) 
		else substr(IMSI,2)
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1)) in ($existing_plmnid1,$existing_plmnid2) then $msisdn_pre || substr(imsi, length($msisdn_pre)+2)
		else substr(IMSI,2)
	end msisdn
from $src_601v
$kill where date between '20160101' and '20160131'
limit $mylimit)
) distribute on random;


create temp table op4 as
(
(select 
	date_logged date
	,imei
	,case
		when substr(imsi,1,length($existing_plmnid1)) in ($existing_plmnid1,$existing_plmnid2) then $new_plmnid || substr(imsi,length($existing_plmnid1)+1) 
		else substr(IMSI,2)
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1)) in ($existing_plmnid1,$existing_plmnid2) then $msisdn_pre || substr(imsi, length($msisdn_pre)+2)
		else substr(IMSI,2)
	end msisdn
from $src_602d
$kill where date between '20160201' and '20160229'
limit $mylimit)
union $myunion
(select 
	date_logged date
	,imei
	,case
		when substr(imsi,1,length($existing_plmnid1)) in ($existing_plmnid1,$existing_plmnid2) then $new_plmnid || substr(imsi,length($existing_plmnid1)+1) 
		else substr(IMSI,2)
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1)) in ($existing_plmnid1,$existing_plmnid2) then $msisdn_pre || substr(imsi, length($msisdn_pre)+2)
		else substr(IMSI,2)
	end msisdn
from $src_602v
$kill where date between '20160201' and '20160229'
limit $mylimit)
) distribute on random;

create temp table op5 as
(
(select 
	date_logged date
	,imei
	,case
		when substr(imsi,1,length($existing_plmnid1)) in ($existing_plmnid1,$existing_plmnid2) then $new_plmnid || substr(imsi,length($existing_plmnid1)+1) 
		else substr(IMSI,2)
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1)) in ($existing_plmnid1,$existing_plmnid2) then $msisdn_pre || substr(imsi, length($msisdn_pre)+2)
		else substr(IMSI,2)
	end msisdn
from $src_603d
$kill where date between '20160301' and '20160331'
limit $mylimit)
union $myunion
(select 
	date_logged date
	,imei
	,case
		when substr(imsi,1,length($existing_plmnid1)) in ($existing_plmnid1,$existing_plmnid2) then $new_plmnid || substr(imsi,length($existing_plmnid1)+1) 
		else substr(IMSI,2)
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1)) in ($existing_plmnid1,$existing_plmnid2) then $msisdn_pre || substr(imsi, length($msisdn_pre)+2)
		else substr(IMSI,2)
	end msisdn
from $src_603v
$kill where date between '20160301' and '20160331'
limit $mylimit)
) distribute on random;

insert into loggy (myindex, descr, num, mycomment) select 1100, 'distinct count 2015-11', count(*), NULL from op1;
insert into loggy (myindex, descr, num, mycomment) select 1200, 'distinct count 2015-12', count(*), NULL from op2;
insert into loggy (myindex, descr, num, mycomment) select 1300, 'distinct count 2016-01', count(*), NULL from op3;
insert into loggy (myindex, descr, num, mycomment) select 1400, 'distinct count 2016-02', count(*), NULL from op4;
insert into loggy (myindex, descr, num, mycomment) select 1500, 'distinct count 2016-03', count(*), NULL from op5;

--compute null counts, to compare with DIRBS import checks
insert into loggy (myindex, descr, num, mycomment) 
select 
	1100
	,'count any null in triplet 2015-11'
	,count(*)
	,NULL
from 
	op1
where 
	imsi = '' or imsi is null
	or imei = '' or imei is null
	or msisdn = '' or msisdn is null
;

insert into loggy (myindex, descr, num, mycomment) 
select 
	1200
	,'count any null in triplet 2015-12'
	,count(*)
	,NULL
from 
	op2
where 
	imsi = '' or imsi is null
	or imei = '' or imei is null
	or msisdn = '' or msisdn is null
;
insert into loggy (myindex, descr, num, mycomment) 
select 
	1300
	,'count any null in triplet 2016-01'
	,count(*)
	,NULL
from 
	op3
where 
	imsi = '' or imsi is null
	or imei = '' or imei is null
	or msisdn = '' or msisdn is null
;
insert into loggy (myindex, descr, num, mycomment) 
select 
	1400
	,'count any null in triplet 2016-02'
	,count(*)
	,NULL
from 
	op4
where 
	imsi = '' or imsi is null
	or imei = '' or imei is null
	or msisdn = '' or msisdn is null
;
insert into loggy (myindex, descr, num, mycomment) 
select 
	1500
	,'count any null in triplet 2016-03'
	,count(*)
	,NULL
from 
	op5
where 
	imsi = '' or imsi is null
	or imei = '' or imei is null
	or msisdn = '' or msisdn is null
;



select myindex, descr,num, mycomment from loggy order by myindex;

--filter the output in case any stray characters made it through
select * from op1 where regexp_like(imei, '^[0-9A-Fa-f*#]+$') and regexp_like(imsi, '^[0-9]+$');
select * from op2 where regexp_like(imei, '^[0-9A-Fa-f*#]+$') and regexp_like(imsi, '^[0-9]+$');
select * from op3 where regexp_like(imei, '^[0-9A-Fa-f*#]+$') and regexp_like(imsi, '^[0-9]+$');
select * from op4 where regexp_like(imei, '^[0-9A-Fa-f*#]+$') and regexp_like(imsi, '^[0-9]+$');
select * from op5 where regexp_like(imei, '^[0-9A-Fa-f*#]+$') and regexp_like(imsi, '^[0-9]+$');
