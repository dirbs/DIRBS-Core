--NetVision data extract for DIRBS
--
--
-- Copyright (c) 2018 Qualcomm Technologies, Inc.
--
-- All rights reserved.
--
-- Redistribution and use in source and binary forms, with or without modification,
-- are permitted (subject to the limitations in the disclaimer below) provided that the
-- following conditions are met:
--
--  * Redistributions of source code must retain the above copyright notice, this list of conditions
--    and the following disclaimer.
--  * Redistributions in binary form must reproduce the above copyright notice, this list of conditions
--    and the following disclaimer in the documentation and/or other materials provided with the distribution.
--  * Neither the name of Qualcomm Technologies, Inc. nor the names of its contributors may be used to endorse
--    or promote products derived from this software without specific prior written permission.
--
-- NO EXPRESS OR IMPLIED LICENSES TO ANY PARTY'S PATENT RIGHTS ARE GRANTED BY THIS LICENSE. THIS SOFTWARE IS PROVIDED
-- BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
-- TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
-- THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
-- CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
-- DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
-- STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
-- EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
--
--
--
--Reuse CRC data to generate external table format for postgres, for direct load into DB (as opposed to "proper" operator import)
--takes all the 5 months data for all operators, and maps to each operator in turn, LIMITing by expected data size
--Note due to subtle differences in the datasets, can't just replace the table names with other Colombian operators - need to examine how data was uploaded



--Source Tables 
--NV_TABLE_DECLARE_BEGIN
--Avantel:
--NV_TABLE_DECLARE_TABLE = $s511a,NV_COLOMBIA_MOVILE_SA_ESP_QTLBASICRAW_ROBERTT_20160107_132114743
--NV_TABLE_DECLARE_TABLE = $s512a,NV_COLOMBIA_MOVILE_SA_ESP_QTLBASICRAW_TOMASG_20160208_221914641
--NV_TABLE_DECLARE_TABLE = $s601a,NV_AVANTEL_QTLCALLRECORDS_OP_TOMASG_20160324_111119016
--No avantel feb data as no unique timestamp
--NV_TABLE_DECLARE_TABLE = $s603a,NV_AVANTEL_QTLCALLRECORDS_OP_TOMASG_20160512_163614890
--Claro:
--NV_TABLE_DECLARE_TABLE = $s511c_d,NV_COLOMBIA_MOVILE_SA_ESP_QTLBASICRAW_ROBERTT_20160115_154744788
--NV_TABLE_DECLARE_TABLE = $s512c_d,NV_COLOMBIA_MOVILE_SA_ESP_QTLBASICRAW_TOMASG_20160209_134241322
--NV_TABLE_DECLARE_TABLE = $s601c_d,NV_CLARO_COLOMBIA_QTLCALLRECORDS_OP_TOMASG_20160325_084942238
--NV_TABLE_DECLARE_TABLE = $s602c_d,NV_CLARO_COLOMBIA_QTLCALLRECORDS_OP_TOMASG_20160414_210330064
--NV_TABLE_DECLARE_TABLE = $s603c_d,NV_CLARO_COLOMBIA_QTLCALLRECORDS_OP_TOMASG_20160512_182039485
--NV_TABLE_DECLARE_TABLE = $s511c_v,NV_COLOMBIA_MOVILE_SA_ESP_QTLBASICRAW_ROBERTT_20160107_134604151
--NV_TABLE_DECLARE_TABLE = $s512c_v,NV_COLOMBIA_MOVILE_SA_ESP_QTLBASICRAW_TOMASG_20160210_201235327
--NV_TABLE_DECLARE_TABLE = $s601c_v,NV_CLARO_COLOMBIA_QTLCALLRECORDS_OP_TOMASG_20160324_180708452
--NV_TABLE_DECLARE_TABLE = $s602c_v,NV_CLARO_COLOMBIA_QTLCALLRECORDS_OP_TOMASG_20160414_155627684
--NV_TABLE_DECLARE_TABLE = $s603c_v,NV_CLARO_COLOMBIA_QTLCALLRECORDS_OP_DSALEK_20160516_125550384
--ETB:
--NV_TABLE_DECLARE_TABLE = $s511e,NV_COLOMBIA_MOVILE_SA_ESP_QTLBASICRAW_ROBERTT_20160107_150639357
--NV_TABLE_DECLARE_TABLE = $s512e,NV_COLOMBIA_MOVILE_SA_ESP_QTLBASICRAW_TOMASG_20160210_120047842
--NV_TABLE_DECLARE_TABLE = $s601e,NV_ETB_QTLCALLRECORDS_OP_DSALEK_20160408_145007098
--NV_TABLE_DECLARE_TABLE = $s602e,NV_ETB_QTLCALLRECORDS_OP_DSALEK_20160422_094202927
--NV_TABLE_DECLARE_TABLE = $s603e,NV_ETB_QTLCALLRECORDS_OP_DSALEK_20160512_221123656
--Telefonica:
--NV_TABLE_DECLARE_TABLE = $s511f,NV_COLOMBIA_MOVILE_SA_ESP_QTLBASICRAW_ROBERTT_20160107_154934821
--NV_TABLE_DECLARE_TABLE = $s512f,NV_COLOMBIA_MOVILE_SA_ESP_QTLBASICRAW_TOMASG_20160209_151109456
--NV_TABLE_DECLARE_TABLE = $s601f_d,NV_TELEFONICA_COLOMBIA_QTLCALLRECORDS_OP_DSALEK_20160327_224648894
--NV_TABLE_DECLARE_TABLE = $s602f_d,NV_TELEFONICA_COLOMBIA_QTLCALLRECORDS_OP_DSALEK_20160415_073429733
--NV_TABLE_DECLARE_TABLE = $s603f_d,NV_TELEFONICA_COLOMBIA_QTLCALLRECORDS_OP_DSALEK_20160513_123731592
--NV_TABLE_DECLARE_TABLE = $s601f_v,NV_TELEFONICA_COLOMBIA_QTLCALLRECORDS_OP_DSALEK_20160328_144516166
--NV_TABLE_DECLARE_TABLE = $s602f_v,NV_TELEFONICA_COLOMBIA_QTLCALLRECORDS_OP_DSALEK_20160414_213710307
--NV_TABLE_DECLARE_TABLE = $s603f_v,NV_TELEFONICA_COLOMBIA_QTLCALLRECORDS_OP_DSALEK_20160513_081406011
--Tigo:
--NV_TABLE_DECLARE_TABLE = $s511g,NV_COLOMBIA_MOVILE_SA_ESP_QTLBASICRAW_ROBERTT_20160107_164211882
--NV_TABLE_DECLARE_TABLE = $s512g,NV_COLOMBIA_MOVILE_SA_ESP_QTLBASICRAW_TOMASG_20160212_223601473
--NV_TABLE_DECLARE_TABLE = $s601g,NV_TIGO_MILLICOM_COLOMBIA_QTLCALLRECORDS_OP_TOMASG_20160328_092803025
--NV_TABLE_DECLARE_TABLE = $s602g,NV_TIGO_MILLICOM_COLOMBIA_QTLCALLRECORDS_OP_DSALEK_20160417_081005115
--NV_TABLE_DECLARE_TABLE = $s603g,NV_TIGO_MILLICOM_COLOMBIA_QTLCALLRECORDS_OP_DSALEK_20160515_001119813
--NV_TABLE_DECLARE_END


--Config Variables
--NV_VARIABLE_DECLARE_BEGIN
--NVN variable syntax doesn't support commas, so add multiple options for plmnid separately. Must be same length as each other and new plmnid (unless want to change imsi length)
--Use a dummy value if there is only 1 plmnid belonging to the home operator in source data:
--NV_VARIABLE_DECLARE_VARIABLE = $existing_plmnid_avantel,string,'732130'
--NV_VARIABLE_DECLARE_VARIABLE = $existing_plmnid1_claro,string,'0732101'
--NV_VARIABLE_DECLARE_VARIABLE = $existing_plmnid2_claro,string,'!!!!!!!'
--NV_VARIABLE_DECLARE_VARIABLE = $existing_plmnid_etb,string,'732187'
--NV_VARIABLE_DECLARE_VARIABLE = $existing_plmnid1_telefonica,string,'732123'
--NV_VARIABLE_DECLARE_VARIABLE = $existing_plmnid2_telefonica,string,'!!!!!!'
--NV_VARIABLE_DECLARE_VARIABLE = $existing_plmnid1_tigo,string,'732103'
--NV_VARIABLE_DECLARE_VARIABLE = $existing_plmnid2_tigo,string,'732111'
--NV_VARIABLE_DECLARE_VARIABLE = $matched_plmnid,string,'||||||'
--NV_VARIABLE_DECLARE_VARIABLE = $mobilink_plmnid,string,'410010'
--NV_VARIABLE_DECLARE_VARIABLE = $ufone_plmnid,string,'410030'
--NV_VARIABLE_DECLARE_VARIABLE = $zong_plmnid,string,'410040'
--NV_VARIABLE_DECLARE_VARIABLE = $telenor_plmnid,string,'410060'
--NV_VARIABLE_DECLARE_VARIABLE = $msisdn_pre,string,'92'

--limits for output of dump Tables:
--NV_VARIABLE_DECLARE_VARIABLE = $limit_mobilink,string,ALL
--NV_VARIABLE_DECLARE_VARIABLE = $limit_zong,string,450000000
--NV_VARIABLE_DECLARE_VARIABLE = $limit_ufone,string,450000000
--NV_VARIABLE_DECLARE_VARIABLE = $limit_telenor,string,750000000


--Limiting variables:
--  For testing, set to (respectively):
--  > 10
--  > all
--  > --
--  > [just leave blank]
--  For complete output, set to:
--  > all
--  > [just leave blank]
--  > [just leave blank]
--  > --
--NV_VARIABLE_DECLARE_VARIABLE = $mylimit,string,10
--NV_VARIABLE_DECLARE_VARIABLE = $myunion,string,all
--NV_VARIABLE_DECLARE_VARIABLE = $kill,string,--
--NV_VARIABLE_DECLARE_VARIABLE = $keep,string,
--NV_VARIABLE_DECLARE_END 

--log outputs table
create temp table loggy (
myindex int
,descr varchar(80)
,num bigint
,mycomment varchar(100)
)distribute on random;

--skip log of total records
--[deleted statements compared to individual scripts]


--do export
--try to preserve some interest in the data by only rewriting MSISDN/IMSI prefix if matches the original home operator



--Nov data generally has date_logged set to a single date, so use fetch_date_time instead

create temp table op1 as
(
--avantel:
(select
	substr(fetch_date_time,1,8) date
	,trim(imei) trimmed_imei
	,case
		when length(translate(substr(trimmed_imei,1,14),'0123456789',''))=0 then substr(trimmed_imei,1,14)
		else upper(trimmed_imei)
	end imei_norm
	,case
		when substr(trim(imsi),1,length($existing_plmnid_avantel)) in ($existing_plmnid_avantel) then $matched_plmnid || substr(trim(imsi),length($existing_plmnid_avantel)+1) 
		else trim(imsi)
	end imsi
	,case
		when substr(trim(imsi),1,length($existing_plmnid_avantel)) in ($existing_plmnid_avantel) then $msisdn_pre || substr(trim(imsi), length($msisdn_pre)+1)
		else trim(imsi)
	end msisdn
	$keep ,'avantel' opname
from $s511a
$kill where date between '20151101' and '20151130' and trim(imsi) is not null
limit $mylimit)
union $myunion
--claro data:
(select 
	substr(fetch_date_time,1,8) date
	,trim(imei) trimmed_imei
	,case
		when length(translate(substr(trimmed_imei,1,14),'0123456789',''))=0 then substr(trimmed_imei,1,14)
		else upper(trimmed_imei)
	end imei_norm
	,case
		when substr(imsi,1,length($existing_plmnid1_claro)) in ($existing_plmnid1_claro,$existing_plmnid2_claro) then $matched_plmnid || substr(imsi,length($existing_plmnid1_claro)+1) 
		else substr(IMSI,2)
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1_claro)) in ($existing_plmnid1_claro,$existing_plmnid2_claro) then $msisdn_pre || substr(imsi, length($msisdn_pre)+2)
		else substr(IMSI,2)
	end msisdn
	$keep ,'Claro data' opname
from $s511c_d
$kill where date between '20151101' and '20151130' and imsi <> 'imsi'
limit $mylimit)
union $myunion
--claro voice:
(select
	substr(fetch_date_time,1,8) date
	,trim(imei) trimmed_imei
	,case
		when length(translate(substr(trimmed_imei,1,14),'0123456789',''))=0 then substr(trimmed_imei,1,14)
		else upper(trimmed_imei)
	end imei_norm
	,case
		when substr(imsi,1,length($existing_plmnid1_claro)) in ($existing_plmnid1_claro,$existing_plmnid2_claro) then $matched_plmnid || substr(imsi,length($existing_plmnid1_claro)+1) 
		else substr(IMSI,2)
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1_claro)) in ($existing_plmnid1_claro,$existing_plmnid2_claro) then $msisdn_pre || substr(imsi, length($msisdn_pre)+2)
		else substr(IMSI,2)
	end msisdn
	$keep ,'Claro voice' opname
from $s511c_v
$kill where date between '20151101' and '20151130'
limit $mylimit)
union $myunion
--etb:
(select 
	to_char(to_date(fetch_date_time, 'YYYY-MM-DD HH:MI:SS'),'YYYYMMDD') date
	,trim(imei) trimmed_imei
	,case
		when length(translate(substr(trimmed_imei,1,14),'0123456789',''))=0 then substr(trimmed_imei,1,14)
		else upper(trimmed_imei)
	end imei_norm
	,case
		when substr(imsi,1,length($existing_plmnid_etb)) = $existing_plmnid_etb then $matched_plmnid || substr(imsi,length($existing_plmnid_etb)+1,length(imsi)-length($existing_plmnid_etb)) 
		else IMSI
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid_etb)) = $existing_plmnid_etb  then $msisdn_pre || substr(imsi, length($msisdn_pre)+1, length(imsi)-length($msisdn_pre))
		else imsi
	end msisdn
	$keep ,'etb' opname
from $s511e
limit $mylimit)
union $myunion
--telefonica combined:
(select 
	substr(fetch_date_time,1,8) date
	,trim(imei) trimmed_imei
	,case
		when length(translate(substr(trimmed_imei,1,14),'0123456789',''))=0 then substr(trimmed_imei,1,14)
		else upper(trimmed_imei)
	end imei_norm
	,case
		when substr(imsi,1,length($existing_plmnid1_telefonica)) in ($existing_plmnid1_telefonica,$existing_plmnid2_telefonica) then $matched_plmnid || substr(rtrim(imsi),length($existing_plmnid1_telefonica)+1) 
		else imsi
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1_telefonica)) in ($existing_plmnid1_telefonica,$existing_plmnid2_telefonica) then $msisdn_pre || substr(rtrim(imsi), length($msisdn_pre)+1)
		else imsi
	end msisdn
	$keep ,'telefonica' opname
from $s511f
$kill where date between '20151101' and '20151130'
limit $mylimit)
union $myunion
--tigo:
(select 
	to_char(to_date(fetch_date_time, 'DD/MM/YYYY HH:MI:SS'),'YYYYMMDD') date
	,trim(imei) trimmed_imei
	,case
		when length(translate(substr(trimmed_imei,1,14),'0123456789',''))=0 then substr(trimmed_imei,1,14)
		else upper(trimmed_imei)
	end imei_norm
	,case
		when substr(imsi,1,length($existing_plmnid1_tigo)) in ($existing_plmnid1_tigo,$existing_plmnid2_tigo) then $matched_plmnid || substr(imsi,length($existing_plmnid1_tigo)+1,length(imsi)-length($existing_plmnid1_tigo)) 
		else IMSI
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1_tigo)) in ($existing_plmnid1_tigo,$existing_plmnid2_tigo) then $msisdn_pre || substr(imsi, length($msisdn_pre)+1, length(imsi)-length($msisdn_pre))
		else imsi
	end msisdn
	$keep ,'tigo' opname
from $s511g
$kill where date between '20151101' and '20151130'
limit $mylimit)
) distribute on random;


--dec data has date_logged as a date, not a varchar
create temp table op2 as
(
--avantel:
(select
	to_char(date_logged, 'YYYYMMDD') date
	,trim(imei) trimmed_imei
	,case
		when length(translate(substr(trimmed_imei,1,14),'0123456789',''))=0 then substr(trimmed_imei,1,14)
		else upper(trimmed_imei)
	end imei_norm
	,case
		when substr(trim(imsi),1,length($existing_plmnid_avantel)) in ($existing_plmnid_avantel) then $matched_plmnid || substr(trim(imsi),length($existing_plmnid_avantel)+1) 
		else trim(imsi)
	end imsi
	,case
		when substr(trim(imsi),1,length($existing_plmnid_avantel)) in ($existing_plmnid_avantel) then $msisdn_pre || substr(trim(imsi), length($msisdn_pre)+1)
		else trim(imsi)
	end msisdn
	$keep ,'avantel' opname
from $s512a
$kill where date between '20151201' and '20151231' and trim(imsi) is not null
limit $mylimit)
union $myunion
--claro data:
(select 
	to_char(date_logged, 'YYYYMMDD') date
	,trim(imei) trimmed_imei
	,case
		when length(translate(substr(trimmed_imei,1,14),'0123456789',''))=0 then substr(trimmed_imei,1,14)
		else upper(trimmed_imei)
	end imei_norm
	,case
		when substr(imsi,1,length($existing_plmnid1_claro)) in ($existing_plmnid1_claro,$existing_plmnid2_claro) then $matched_plmnid || substr(imsi,length($existing_plmnid1_claro)+1) 
		else substr(IMSI,2)
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1_claro)) in ($existing_plmnid1_claro,$existing_plmnid2_claro) then $msisdn_pre || substr(imsi, length($msisdn_pre)+2)
		else substr(IMSI,2)
	end msisdn
	$keep ,'Claro data' opname
from $s512c_d
$kill where date between '20151201' and '20151231'
limit $mylimit)
union $myunion
--claro voice:
(select 
	to_char(date_logged, 'YYYYMMDD') date
	,trim(imei) trimmed_imei
	,case
		when length(translate(substr(trimmed_imei,1,14),'0123456789',''))=0 then substr(trimmed_imei,1,14)
		else upper(trimmed_imei)
	end imei_norm
	,case
		when substr(imsi,1,length($existing_plmnid1_claro)) in ($existing_plmnid1_claro,$existing_plmnid2_claro) then $matched_plmnid || substr(imsi,length($existing_plmnid1_claro)+1) 
		else substr(IMSI,2)
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1_claro)) in ($existing_plmnid1_claro,$existing_plmnid2_claro) then $msisdn_pre || substr(imsi, length($msisdn_pre)+2)
		else substr(IMSI,2)
	end msisdn
	$keep ,'Claro voice' opname
from $s512c_v
$kill where date between '20151201' and '20151231'
limit $mylimit)
union $myunion
--ETB:
(select 
	to_char(date_logged, 'YYYYMMDD') date
	,trim(imei) trimmed_imei
	,case
		when length(translate(substr(trimmed_imei,1,14),'0123456789',''))=0 then substr(trimmed_imei,1,14)
		else upper(trimmed_imei)
	end imei_norm
	,case
		when substr(imsi,1,length($existing_plmnid_etb)) = $existing_plmnid_etb then $matched_plmnid || substr(imsi,length($existing_plmnid_etb)+1,length(imsi)-length($existing_plmnid_etb)) 
		else IMSI
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid_etb)) = $existing_plmnid_etb  then $msisdn_pre || substr(imsi, length($msisdn_pre)+1, length(imsi)-length($msisdn_pre))
		else imsi
	end msisdn
	$keep ,'etb' opname
from $s512e
limit $mylimit)
union $myunion
--telefonica combined:
(select 
	to_char(date_logged, 'YYYYMMDD') date
	,trim(imei) trimmed_imei
	,case
		when length(translate(substr(trimmed_imei,1,14),'0123456789',''))=0 then substr(trimmed_imei,1,14)
		else upper(trimmed_imei)
	end imei_norm
	,case
		when substr(imsi,1,length($existing_plmnid1_telefonica)) in ($existing_plmnid1_telefonica,$existing_plmnid2_telefonica) then $matched_plmnid || substr(imsi,length($existing_plmnid1_telefonica)+1) 
		else imsi
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1_telefonica)) in ($existing_plmnid1_telefonica,$existing_plmnid2_telefonica) then $msisdn_pre || substr(imsi, length($msisdn_pre)+1)
		else imsi
	end msisdn
	$keep ,'telefonica' opname
from $s512f
$kill where date between '20151201' and '20151231'
limit $mylimit)
union $myunion
--tigo:
(select 
	to_char(date_logged, 'YYYYMMDD') date
	,trim(imei) trimmed_imei
	,case
		when length(translate(substr(trimmed_imei,1,14),'0123456789',''))=0 then substr(trimmed_imei,1,14)
		else upper(trimmed_imei)
	end imei_norm
	,case
		when substr(imsi,1,length($existing_plmnid1_tigo)) in ($existing_plmnid1_tigo,$existing_plmnid2_tigo) then $matched_plmnid || substr(imsi,length($existing_plmnid1_tigo)+1,length(imsi)-length($existing_plmnid1_tigo)) 
		else IMSI
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1_tigo)) in ($existing_plmnid1_tigo,$existing_plmnid2_tigo) then $msisdn_pre || substr(imsi, length($msisdn_pre)+1, length(imsi)-length($msisdn_pre))
		else imsi
	end msisdn
	$keep ,'tigo' opname
from $s512g
$kill where date between '20151201' and '20151231'
limit $mylimit)
) distribute on random;


create temp table op3 as
(
--avantel:
(select
	substr(date_logged,1,8) date
	,trim(imei) trimmed_imei
	,case
		when length(translate(substr(trimmed_imei,1,14),'0123456789',''))=0 then substr(trimmed_imei,1,14)
		else upper(trimmed_imei)
	end imei_norm
	,case
		when substr(trim(imsi),1,length($existing_plmnid_avantel)) in ($existing_plmnid_avantel) then $matched_plmnid || substr(trim(imsi),length($existing_plmnid_avantel)+1) 
		else trim(imsi)
	end imsi
	,case
		when substr(trim(imsi),1,length($existing_plmnid_avantel)) in ($existing_plmnid_avantel) then $msisdn_pre || substr(trim(imsi), length($msisdn_pre)+1)
		else trim(imsi)
	end msisdn
	$keep ,'avantel' opname
from $s601a
$kill where date between '20160101' and '20160131' and trim(imsi) is not null
limit $mylimit)
union $myunion
--claro data:
(select 
	substr(date_logged,1,8) date
	,trim(imei) trimmed_imei
	,case
		when length(translate(substr(trimmed_imei,1,14),'0123456789',''))=0 then substr(trimmed_imei,1,14)
		else upper(trimmed_imei)
	end imei_norm
	,case
		when substr(imsi,1,length($existing_plmnid1_claro)) in ($existing_plmnid1_claro,$existing_plmnid2_claro) then $matched_plmnid || substr(imsi,length($existing_plmnid1_claro)+1) 
		else substr(IMSI,2)
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1_claro)) in ($existing_plmnid1_claro,$existing_plmnid2_claro) then $msisdn_pre || substr(imsi, length($msisdn_pre)+2)
		else substr(IMSI,2)
	end msisdn
	$keep ,'Claro data' opname		
from $s601c_d
$kill where date between '20160101' and '20160131'
limit $mylimit)
union $myunion
--claro voice:
(select 
	date_logged date
	,trim(imei) trimmed_imei
	,case
		when length(translate(substr(trimmed_imei,1,14),'0123456789',''))=0 then substr(trimmed_imei,1,14)
		else upper(trimmed_imei)
	end imei_norm
	,case
		when substr(imsi,1,length($existing_plmnid1_claro)) in ($existing_plmnid1_claro,$existing_plmnid2_claro) then $matched_plmnid || substr(imsi,length($existing_plmnid1_claro)+1) 
		else substr(IMSI,2)
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1_claro)) in ($existing_plmnid1_claro,$existing_plmnid2_claro) then $msisdn_pre || substr(imsi, length($msisdn_pre)+2)
		else substr(IMSI,2)
	end msisdn
	$keep ,'Claro voice' opname
from $s601c_v
$kill where date between '20160101' and '20160131'
limit $mylimit)
union $myunion
--ETB:
(select 
	date_logged date
	,trim(imei) trimmed_imei
	,case
		when length(translate(substr(trimmed_imei,1,14),'0123456789',''))=0 then substr(trimmed_imei,1,14)
		else upper(trimmed_imei)
	end imei_norm
	,case
		when substr(imsi,1,length($existing_plmnid_etb)) = $existing_plmnid_etb then $matched_plmnid || substr(imsi,length($existing_plmnid_etb)+1,length(imsi)-length($existing_plmnid_etb)) 
		else IMSI
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid_etb)) = $existing_plmnid_etb  then $msisdn_pre || substr(imsi, length($msisdn_pre)+1, length(imsi)-length($msisdn_pre))
		else imsi
	end msisdn
	$keep ,'etb' opname
from $s601e
$kill where date_logged is not null
limit $mylimit)
union $myunion
--telefonica data:
(select 
	substr(date_logged,1,8) date
	,trim(imei) trimmed_imei
	,case
		when length(translate(substr(trimmed_imei,1,14),'0123456789',''))=0 then substr(trimmed_imei,1,14)
		else upper(trimmed_imei)
	end imei_norm
	,case
		when substr(imsi,1,length($existing_plmnid1_telefonica)) in ($existing_plmnid1_telefonica,$existing_plmnid2_telefonica) then $matched_plmnid || substr(imsi,length($existing_plmnid1_telefonica)+1) 
		else imsi
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1_telefonica)) in ($existing_plmnid1_telefonica,$existing_plmnid2_telefonica) then $msisdn_pre || substr(imsi, length($msisdn_pre)+1)
		else imsi
	end msisdn
	$keep ,'telefonica data' opname
from $s601f_d
$kill where date between '20160101' and '20160131'
limit $mylimit)
union $myunion
--telefonica voice:
(select 
	date_logged date
	,trim(imei) trimmed_imei
	,case
		when length(translate(substr(trimmed_imei,1,14),'0123456789',''))=0 then substr(trimmed_imei,1,14)
		else upper(trimmed_imei)
	end imei_norm
	,case
		when substr(imsi,1,length($existing_plmnid1_telefonica)) in ($existing_plmnid1_telefonica,$existing_plmnid2_telefonica) then $matched_plmnid || substr(imsi,length($existing_plmnid1_telefonica)+1) 
		else imsi
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1_telefonica)) in ($existing_plmnid1_telefonica,$existing_plmnid2_telefonica) then $msisdn_pre || substr(imsi, length($msisdn_pre)+1)
		else imsi
	end msisdn
	$keep ,'telefonica voice' opname
from $s601f_v
$kill where date between '20160101' and '20160131'
limit $mylimit)
union $myunion
--tigo:
(select 
	date_logged date
	,trim(imei) trimmed_imei
	,case
		when length(translate(substr(trimmed_imei,1,14),'0123456789',''))=0 then substr(trimmed_imei,1,14)
		else upper(trimmed_imei)
	end imei_norm
	,case
		when substr(imsi,1,length($existing_plmnid1_tigo)) in ($existing_plmnid1_tigo,$existing_plmnid2_tigo) then $matched_plmnid || substr(imsi,length($existing_plmnid1_tigo)+1,length(imsi)-length($existing_plmnid1_tigo)) 
		else IMSI
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1_tigo)) in ($existing_plmnid1_tigo,$existing_plmnid2_tigo) then $msisdn_pre || substr(imsi, length($msisdn_pre)+1, length(imsi)-length($msisdn_pre))
		else imsi
	end msisdn
	$keep ,'tigo' opname
from $s601g
$kill where date between '20160101' and '20160131'
limit $mylimit)
) distribute on random;


create temp table op4 as
(
--avantel:
--*********[no avantel data for feb]************
--claro data:
(select 
	date_logged date
	,trim(imei) trimmed_imei
	,case
		when length(translate(substr(trimmed_imei,1,14),'0123456789',''))=0 then substr(trimmed_imei,1,14)
		else upper(trimmed_imei)
	end imei_norm
	,case
		when substr(imsi,1,length($existing_plmnid1_claro)) in ($existing_plmnid1_claro,$existing_plmnid2_claro) then $matched_plmnid || substr(imsi,length($existing_plmnid1_claro)+1) 
		else substr(IMSI,2)
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1_claro)) in ($existing_plmnid1_claro,$existing_plmnid2_claro) then $msisdn_pre || substr(imsi, length($msisdn_pre)+2)
		else substr(IMSI,2)
	end msisdn
	$keep ,'Claro data' opname
from $s602c_d
$kill where date between '20160201' and '20160229'
limit $mylimit)
union $myunion
--claro voice:
(select 
	date_logged date
	,trim(imei) trimmed_imei
	,case
		when length(translate(substr(trimmed_imei,1,14),'0123456789',''))=0 then substr(trimmed_imei,1,14)
		else upper(trimmed_imei)
	end imei_norm
	,case
		when substr(imsi,1,length($existing_plmnid1_claro)) in ($existing_plmnid1_claro,$existing_plmnid2_claro) then $matched_plmnid || substr(imsi,length($existing_plmnid1_claro)+1) 
		else substr(IMSI,2)
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1_claro)) in ($existing_plmnid1_claro,$existing_plmnid2_claro) then $msisdn_pre || substr(imsi, length($msisdn_pre)+2)
		else substr(IMSI,2)
	end msisdn
	$keep ,'Claro voice' opname
from $s602c_v
$kill where date between '20160201' and '20160229'
limit $mylimit)
union $myunion
--ETB:
(select 
	date_logged date
	,trim(imei) trimmed_imei
	,case
		when length(translate(substr(trimmed_imei,1,14),'0123456789',''))=0 then substr(trimmed_imei,1,14)
		else upper(trimmed_imei)
	end imei_norm
	,case
		when substr(imsi,1,length($existing_plmnid_etb)) = $existing_plmnid_etb then $matched_plmnid || substr(imsi,length($existing_plmnid_etb)+1,length(imsi)-length($existing_plmnid_etb)) 
		else IMSI
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid_etb)) = $existing_plmnid_etb  then $msisdn_pre || substr(imsi, length($msisdn_pre)+1, length(imsi)-length($msisdn_pre))
		else imsi
	end msisdn
	$keep ,'etb' opname
from $s602e
$kill where date_logged is not null
limit $mylimit)
union $myunion
--telefonica data:
(select 
	date_logged date
	,trim(imei) trimmed_imei
	,case
		when length(translate(substr(trimmed_imei,1,14),'0123456789',''))=0 then substr(trimmed_imei,1,14)
		else upper(trimmed_imei)
	end imei_norm
	,case
		when substr(imsi,1,length($existing_plmnid1_telefonica)) in ($existing_plmnid1_telefonica,$existing_plmnid2_telefonica) then $matched_plmnid || substr(imsi,length($existing_plmnid1_telefonica)+1) 
		else imsi
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1_telefonica)) in ($existing_plmnid1_telefonica,$existing_plmnid2_telefonica) then $msisdn_pre || substr(imsi, length($msisdn_pre)+1)
		else imsi
	end msisdn
	$keep ,'telefonica data' opname
from $s602f_d
$kill where date between '20160201' and '20160229'
limit $mylimit)
union $myunion
--telefonica voice:
(select 
	date_logged date
	,trim(imei) trimmed_imei
	,case
		when length(translate(substr(trimmed_imei,1,14),'0123456789',''))=0 then substr(trimmed_imei,1,14)
		else upper(trimmed_imei)
	end imei_norm
	,case
		when substr(imsi,1,length($existing_plmnid1_telefonica)) in ($existing_plmnid1_telefonica,$existing_plmnid2_telefonica) then $matched_plmnid || substr(imsi,length($existing_plmnid1_telefonica)+1) 
		else imsi
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1_telefonica)) in ($existing_plmnid1_telefonica,$existing_plmnid2_telefonica) then $msisdn_pre || substr(imsi, length($msisdn_pre)+1)
		else imsi
	end msisdn
	$keep ,'telefonica voice' opname
from $s602f_v
$kill where date between '20160201' and '20160229'
limit $mylimit)
union $myunion
--tigo:
(select 
	date_logged date
	,trim(imei) trimmed_imei
	,case
		when length(translate(substr(trimmed_imei,1,14),'0123456789',''))=0 then substr(trimmed_imei,1,14)
		else upper(trimmed_imei)
	end imei_norm
	,case
		when substr(imsi,1,length($existing_plmnid1_tigo)) in ($existing_plmnid1_tigo,$existing_plmnid2_tigo) then $matched_plmnid || substr(imsi,length($existing_plmnid1_tigo)+1,length(imsi)-length($existing_plmnid1_tigo)) 
		else IMSI
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1_tigo)) in ($existing_plmnid1_tigo,$existing_plmnid2_tigo) then $msisdn_pre || substr(imsi, length($msisdn_pre)+1, length(imsi)-length($msisdn_pre))
		else imsi
	end msisdn
	$keep ,'tigo' opname
from $s602g
$kill where date between '20160201' and '20160229'
limit $mylimit)
) distribute on random;

create temp table op5 as
(
--avantel:
(select
	date_logged date
	,trim(imei) trimmed_imei
	,case
		when length(translate(substr(trimmed_imei,1,14),'0123456789',''))=0 then substr(trimmed_imei,1,14)
		else upper(trimmed_imei)
	end imei_norm
	,case
		when substr(trim(imsi),1,length($existing_plmnid_avantel)) in ($existing_plmnid_avantel) then $matched_plmnid || substr(trim(imsi),length($existing_plmnid_avantel)+1) 
		else trim(imsi)
	end imsi
	,case
		when substr(trim(imsi),1,length($existing_plmnid_avantel)) in ($existing_plmnid_avantel) then $msisdn_pre || substr(trim(imsi), length($msisdn_pre)+1)
		else trim(imsi)
	end msisdn
	$keep ,'avantel' opname
from $s603a
$kill where date between '20160301' and '20160331' and trim(imsi) is not null
limit $mylimit)
union $myunion
--claro data:
(select 
	date_logged date
	,trim(imei) trimmed_imei
	,case
		when length(translate(substr(trimmed_imei,1,14),'0123456789',''))=0 then substr(trimmed_imei,1,14)
		else upper(trimmed_imei)
	end imei_norm
	,case
		when substr(imsi,1,length($existing_plmnid1_claro)) in ($existing_plmnid1_claro,$existing_plmnid2_claro) then $matched_plmnid || substr(imsi,length($existing_plmnid1_claro)+1) 
		else substr(IMSI,2)
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1_claro)) in ($existing_plmnid1_claro,$existing_plmnid2_claro) then $msisdn_pre || substr(imsi, length($msisdn_pre)+2)
		else substr(IMSI,2)
	end msisdn
	$keep ,'Claro data' opname
from $s603c_d
$kill where date between '20160301' and '20160331'
limit $mylimit)
union $myunion
--claro voice:
(select 
	date_logged date
	,trim(imei) trimmed_imei
	,case
		when length(translate(substr(trimmed_imei,1,14),'0123456789',''))=0 then substr(trimmed_imei,1,14)
		else upper(trimmed_imei)
	end imei_norm
	,case
		when substr(imsi,1,length($existing_plmnid1_claro)) in ($existing_plmnid1_claro,$existing_plmnid2_claro) then $matched_plmnid || substr(imsi,length($existing_plmnid1_claro)+1) 
		else substr(IMSI,2)
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1_claro)) in ($existing_plmnid1_claro,$existing_plmnid2_claro) then $msisdn_pre || substr(imsi, length($msisdn_pre)+2)
		else substr(IMSI,2)
	end msisdn
	$keep ,'Claro voice' opname
from $s603c_v
$kill where date between '20160301' and '20160331'
limit $mylimit)
union $myunion
--ETB:
(select 
	date_logged date
	,trim(imei) trimmed_imei
	,case
		when length(translate(substr(trimmed_imei,1,14),'0123456789',''))=0 then substr(trimmed_imei,1,14)
		else upper(trimmed_imei)
	end imei_norm
	,case
		when substr(imsi,1,length($existing_plmnid_etb)) = $existing_plmnid_etb then $matched_plmnid || substr(imsi,length($existing_plmnid_etb)+1,length(imsi)-length($existing_plmnid_etb)) 
		else IMSI
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid_etb)) = $existing_plmnid_etb  then $msisdn_pre || substr(imsi, length($msisdn_pre)+1, length(imsi)-length($msisdn_pre))
		else imsi
	end msisdn
	$keep ,'etb' opname
from $s603e
$kill where date_logged is not null
limit $mylimit)
union $myunion
--telefonica data:
(select 
	date_logged date
	,trim(imei) trimmed_imei
	,case
		when length(translate(substr(trimmed_imei,1,14),'0123456789',''))=0 then substr(trimmed_imei,1,14)
		else upper(trimmed_imei)
	end imei_norm
	,case
		when substr(imsi,1,length($existing_plmnid1_telefonica)) in ($existing_plmnid1_telefonica,$existing_plmnid2_telefonica) then $matched_plmnid || substr(imsi,length($existing_plmnid1_telefonica)+1) 
		else imsi
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1_telefonica)) in ($existing_plmnid1_telefonica,$existing_plmnid2_telefonica) then $msisdn_pre || substr(imsi, length($msisdn_pre)+1)
		else imsi
	end msisdn
	$keep ,'telefonica data' opname
from $s603f_d
$kill where date between '20160301' and '20160331'
limit $mylimit)
union $myunion
--telefonica voice:
(select 
	date_logged date
	,trim(imei) trimmed_imei
	,case
		when length(translate(substr(trimmed_imei,1,14),'0123456789',''))=0 then substr(trimmed_imei,1,14)
		else upper(trimmed_imei)
	end imei_norm
	,case
		when substr(imsi,1,length($existing_plmnid1_telefonica)) in ($existing_plmnid1_telefonica,$existing_plmnid2_telefonica) then $matched_plmnid || substr(imsi,length($existing_plmnid1_telefonica)+1) 
		else imsi
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1_telefonica)) in ($existing_plmnid1_telefonica,$existing_plmnid2_telefonica) then $msisdn_pre || substr(imsi, length($msisdn_pre)+1)
		else imsi
	end msisdn
	$keep ,'telefonica voice' opname
from $s603f_v
$kill where date between '20160301' and '20160331'
limit $mylimit)
union $myunion
--tigo:
(select 
	date_logged date
	,trim(imei) trimmed_imei
	,case
		when length(translate(substr(trimmed_imei,1,14),'0123456789',''))=0 then substr(trimmed_imei,1,14)
		else upper(trimmed_imei)
	end imei_norm
	,case
		when substr(imsi,1,length($existing_plmnid1_tigo)) in ($existing_plmnid1_tigo,$existing_plmnid2_tigo) then $matched_plmnid || substr(imsi,length($existing_plmnid1_tigo)+1,length(imsi)-length($existing_plmnid1_tigo)) 
		else IMSI
	end imsi
	,case
		when substr(imsi,1,length($existing_plmnid1_tigo)) in ($existing_plmnid1_tigo,$existing_plmnid2_tigo) then $msisdn_pre || substr(imsi, length($msisdn_pre)+1, length(imsi)-length($msisdn_pre))
		else imsi
	end msisdn
	$keep ,'tigo' opname
from $s603g
$kill where date between '20160301' and '20160331'
limit $mylimit)
) distribute on random;

insert into loggy (myindex, descr, num, mycomment) select 1100, 'distinct count 2015-11', count(*), NULL from op1;
insert into loggy (myindex, descr, num, mycomment) select 1200, 'distinct count 2015-12', count(*), NULL from op2;
insert into loggy (myindex, descr, num, mycomment) select 1300, 'distinct count 2016-01', count(*), NULL from op3;
insert into loggy (myindex, descr, num, mycomment) select 1400, 'distinct count 2016-02', count(*), NULL from op4;
insert into loggy (myindex, descr, num, mycomment) select 1500, 'distinct count 2016-03', count(*), NULL from op5;



select myindex, descr,num, mycomment from loggy order by myindex;

--filter the table in case any stray characters made it through. Allow pipe in IMSI for the placeholder PLMNID
create temp table op1_clean as (select * from op1 where regexp_like(trimmed_imei, '^[0-9A-Fa-f*#]+$') and regexp_like(imsi, '^[0-9\|]+$')) distribute on random;
create temp table op2_clean as (select * from op2 where regexp_like(trimmed_imei, '^[0-9A-Fa-f*#]+$') and regexp_like(imsi, '^[0-9\|]+$')) distribute on random;
create temp table op3_clean as (select * from op3 where regexp_like(trimmed_imei, '^[0-9A-Fa-f*#]+$') and regexp_like(imsi, '^[0-9\|]+$')) distribute on random;
create temp table op4_clean as (select * from op4 where regexp_like(trimmed_imei, '^[0-9A-Fa-f*#]+$') and regexp_like(imsi, '^[0-9\|]+$')) distribute on random;
create temp table op5_clean as (select * from op5 where regexp_like(trimmed_imei, '^[0-9A-Fa-f*#]+$') and regexp_like(imsi, '^[0-9\|]+$')) distribute on random;


--output each table as a per-operator flavour

create temp table mobilink1 as (select 'mobilink' id, -1 import_id, msisdn, trimmed_imei imei, imei_norm, case when substr(imsi,1,length($matched_plmnid))=$matched_plmnid then $mobilink_plmnid || substr(imsi, length($matched_plmnid)+1) else imsi end imsi, date connection_date, '000' rat from op1_clean limit $limit_mobilink) distribute on random;
create temp table mobilink2 as (select 'mobilink' id, -1 import_id, msisdn, trimmed_imei imei, imei_norm, case when substr(imsi,1,length($matched_plmnid))=$matched_plmnid then $mobilink_plmnid || substr(imsi, length($matched_plmnid)+1) else imsi end imsi, date connection_date, '000' rat from op2_clean limit $limit_mobilink) distribute on random;
create temp table mobilink3 as (select 'mobilink' id, -1 import_id, msisdn, trimmed_imei imei, imei_norm, case when substr(imsi,1,length($matched_plmnid))=$matched_plmnid then $mobilink_plmnid || substr(imsi, length($matched_plmnid)+1) else imsi end imsi, date connection_date, '000' rat from op3_clean limit $limit_mobilink) distribute on random;
create temp table mobilink4 as (select 'mobilink' id, -1 import_id, msisdn, trimmed_imei imei, imei_norm, case when substr(imsi,1,length($matched_plmnid))=$matched_plmnid then $mobilink_plmnid || substr(imsi, length($matched_plmnid)+1) else imsi end imsi, date connection_date, '000' rat from op4_clean limit $limit_mobilink) distribute on random;
create temp table mobilink5 as (select 'mobilink' id, -1 import_id, msisdn, trimmed_imei imei, imei_norm, case when substr(imsi,1,length($matched_plmnid))=$matched_plmnid then $mobilink_plmnid || substr(imsi, length($matched_plmnid)+1) else imsi end imsi, date connection_date, '000' rat from op5_clean limit $limit_mobilink) distribute on random;

create temp table ufone1 as (select 'ufone' id, -1 import_id, msisdn, trimmed_imei imei, imei_norm, case when substr(imsi,1,length($matched_plmnid))=$matched_plmnid then $ufone_plmnid || substr(imsi, length($matched_plmnid)+1) else imsi end imsi, date connection_date, '000' rat from op1_clean limit $limit_ufone) distribute on random;
create temp table ufone2 as (select 'ufone' id, -1 import_id, msisdn, trimmed_imei imei, imei_norm, case when substr(imsi,1,length($matched_plmnid))=$matched_plmnid then $ufone_plmnid || substr(imsi, length($matched_plmnid)+1) else imsi end imsi, date connection_date, '000' rat from op2_clean limit $limit_ufone) distribute on random;
create temp table ufone3 as (select 'ufone' id, -1 import_id, msisdn, trimmed_imei imei, imei_norm, case when substr(imsi,1,length($matched_plmnid))=$matched_plmnid then $ufone_plmnid || substr(imsi, length($matched_plmnid)+1) else imsi end imsi, date connection_date, '000' rat from op3_clean limit $limit_ufone) distribute on random;
create temp table ufone4 as (select 'ufone' id, -1 import_id, msisdn, trimmed_imei imei, imei_norm, case when substr(imsi,1,length($matched_plmnid))=$matched_plmnid then $ufone_plmnid || substr(imsi, length($matched_plmnid)+1) else imsi end imsi, date connection_date, '000' rat from op4_clean limit $limit_ufone) distribute on random;
create temp table ufone5 as (select 'ufone' id, -1 import_id, msisdn, trimmed_imei imei, imei_norm, case when substr(imsi,1,length($matched_plmnid))=$matched_plmnid then $ufone_plmnid || substr(imsi, length($matched_plmnid)+1) else imsi end imsi, date connection_date, '000' rat from op5_clean limit $limit_ufone) distribute on random;

create temp table zong1 as (select 'zong' id, -1 import_id, msisdn, trimmed_imei imei, imei_norm, case when substr(imsi,1,length($matched_plmnid))=$matched_plmnid then $zong_plmnid || substr(imsi, length($matched_plmnid)+1) else imsi end imsi, date connection_date, '000' rat from op1_clean limit $limit_zong) distribute on random;
create temp table zong2 as (select 'zong' id, -1 import_id, msisdn, trimmed_imei imei, imei_norm, case when substr(imsi,1,length($matched_plmnid))=$matched_plmnid then $zong_plmnid || substr(imsi, length($matched_plmnid)+1) else imsi end imsi, date connection_date, '000' rat from op2_clean limit $limit_zong) distribute on random;
create temp table zong3 as (select 'zong' id, -1 import_id, msisdn, trimmed_imei imei, imei_norm, case when substr(imsi,1,length($matched_plmnid))=$matched_plmnid then $zong_plmnid || substr(imsi, length($matched_plmnid)+1) else imsi end imsi, date connection_date, '000' rat from op3_clean limit $limit_zong) distribute on random;
create temp table zong4 as (select 'zong' id, -1 import_id, msisdn, trimmed_imei imei, imei_norm, case when substr(imsi,1,length($matched_plmnid))=$matched_plmnid then $zong_plmnid || substr(imsi, length($matched_plmnid)+1) else imsi end imsi, date connection_date, '000' rat from op4_clean limit $limit_zong) distribute on random;
create temp table zong5 as (select 'zong' id, -1 import_id, msisdn, trimmed_imei imei, imei_norm, case when substr(imsi,1,length($matched_plmnid))=$matched_plmnid then $zong_plmnid || substr(imsi, length($matched_plmnid)+1) else imsi end imsi, date connection_date, '000' rat from op5_clean limit $limit_zong) distribute on random;

create temp table telenor1 as (select 'telenor' id, -1 import_id, msisdn, trimmed_imei imei, imei_norm, case when substr(imsi,1,length($matched_plmnid))=$matched_plmnid then $telenor_plmnid || substr(imsi, length($matched_plmnid)+1) else imsi end imsi, date connection_date, '000' rat from op1_clean limit $limit_telenor) distribute on random;
create temp table telenor2 as (select 'telenor' id, -1 import_id, msisdn, trimmed_imei imei, imei_norm, case when substr(imsi,1,length($matched_plmnid))=$matched_plmnid then $telenor_plmnid || substr(imsi, length($matched_plmnid)+1) else imsi end imsi, date connection_date, '000' rat from op2_clean limit $limit_telenor) distribute on random;
create temp table telenor3 as (select 'telenor' id, -1 import_id, msisdn, trimmed_imei imei, imei_norm, case when substr(imsi,1,length($matched_plmnid))=$matched_plmnid then $telenor_plmnid || substr(imsi, length($matched_plmnid)+1) else imsi end imsi, date connection_date, '000' rat from op3_clean limit $limit_telenor) distribute on random;
create temp table telenor4 as (select 'telenor' id, -1 import_id, msisdn, trimmed_imei imei, imei_norm, case when substr(imsi,1,length($matched_plmnid))=$matched_plmnid then $telenor_plmnid || substr(imsi, length($matched_plmnid)+1) else imsi end imsi, date connection_date, '000' rat from op4_clean limit $limit_telenor) distribute on random;
create temp table telenor5 as (select 'telenor' id, -1 import_id, msisdn, trimmed_imei imei, imei_norm, case when substr(imsi,1,length($matched_plmnid))=$matched_plmnid then $telenor_plmnid || substr(imsi, length($matched_plmnid)+1) else imsi end imsi, date connection_date, '000' rat from op5_clean limit $limit_telenor) distribute on random;

--outputs
--note NVN bug means computed hash column has no header value, but postgres doesn't need it
--dropping IMEI from output, and distincting here to take care of different imeis mapping to same imei_norm
select distinct id, import_id, msisdn, imei_norm, imsi, connection_date, rat, rawtohex(hash('id' || nvl(id, '') ||'connection_date' || nvl(connection_date, '') ||'imei_norm' || nvl(imei_norm, '') ||'imsi' || nvl(imsi, '') ||'msisdn' || nvl(msisdn, '') ||'rat' || nvl(rat, ''))) hash from mobilink1;   
select distinct id, import_id, msisdn, imei_norm, imsi, connection_date, rat, rawtohex(hash('id' || nvl(id, '') ||'connection_date' || nvl(connection_date, '') ||'imei_norm' || nvl(imei_norm, '') ||'imsi' || nvl(imsi, '') ||'msisdn' || nvl(msisdn, '') ||'rat' || nvl(rat, ''))) hash from mobilink2;   
select distinct id, import_id, msisdn, imei_norm, imsi, connection_date, rat, rawtohex(hash('id' || nvl(id, '') ||'connection_date' || nvl(connection_date, '') ||'imei_norm' || nvl(imei_norm, '') ||'imsi' || nvl(imsi, '') ||'msisdn' || nvl(msisdn, '') ||'rat' || nvl(rat, ''))) hash from mobilink3;   
select distinct id, import_id, msisdn, imei_norm, imsi, connection_date, rat, rawtohex(hash('id' || nvl(id, '') ||'connection_date' || nvl(connection_date, '') ||'imei_norm' || nvl(imei_norm, '') ||'imsi' || nvl(imsi, '') ||'msisdn' || nvl(msisdn, '') ||'rat' || nvl(rat, ''))) hash from mobilink4;   
select distinct id, import_id, msisdn, imei_norm, imsi, connection_date, rat, rawtohex(hash('id' || nvl(id, '') ||'connection_date' || nvl(connection_date, '') ||'imei_norm' || nvl(imei_norm, '') ||'imsi' || nvl(imsi, '') ||'msisdn' || nvl(msisdn, '') ||'rat' || nvl(rat, ''))) hash from mobilink5;   

select distinct id, import_id, msisdn, imei_norm, imsi, connection_date, rat, rawtohex(hash('id' || nvl(id, '') ||'connection_date' || nvl(connection_date, '') ||'imei_norm' || nvl(imei_norm, '') ||'imsi' || nvl(imsi, '') ||'msisdn' || nvl(msisdn, '') ||'rat' || nvl(rat, ''))) hash from ufone1;   
select distinct id, import_id, msisdn, imei_norm, imsi, connection_date, rat, rawtohex(hash('id' || nvl(id, '') ||'connection_date' || nvl(connection_date, '') ||'imei_norm' || nvl(imei_norm, '') ||'imsi' || nvl(imsi, '') ||'msisdn' || nvl(msisdn, '') ||'rat' || nvl(rat, ''))) hash from ufone2;   
select distinct id, import_id, msisdn, imei_norm, imsi, connection_date, rat, rawtohex(hash('id' || nvl(id, '') ||'connection_date' || nvl(connection_date, '') ||'imei_norm' || nvl(imei_norm, '') ||'imsi' || nvl(imsi, '') ||'msisdn' || nvl(msisdn, '') ||'rat' || nvl(rat, ''))) hash from ufone3;   
select distinct id, import_id, msisdn, imei_norm, imsi, connection_date, rat, rawtohex(hash('id' || nvl(id, '') ||'connection_date' || nvl(connection_date, '') ||'imei_norm' || nvl(imei_norm, '') ||'imsi' || nvl(imsi, '') ||'msisdn' || nvl(msisdn, '') ||'rat' || nvl(rat, ''))) hash from ufone4;   
select distinct id, import_id, msisdn, imei_norm, imsi, connection_date, rat, rawtohex(hash('id' || nvl(id, '') ||'connection_date' || nvl(connection_date, '') ||'imei_norm' || nvl(imei_norm, '') ||'imsi' || nvl(imsi, '') ||'msisdn' || nvl(msisdn, '') ||'rat' || nvl(rat, ''))) hash from ufone5; 

select distinct id, import_id, msisdn, imei_norm, imsi, connection_date, rat, rawtohex(hash('id' || nvl(id, '') ||'connection_date' || nvl(connection_date, '') ||'imei_norm' || nvl(imei_norm, '') ||'imsi' || nvl(imsi, '') ||'msisdn' || nvl(msisdn, '') ||'rat' || nvl(rat, ''))) hash from zong1;   
select distinct id, import_id, msisdn, imei_norm, imsi, connection_date, rat, rawtohex(hash('id' || nvl(id, '') ||'connection_date' || nvl(connection_date, '') ||'imei_norm' || nvl(imei_norm, '') ||'imsi' || nvl(imsi, '') ||'msisdn' || nvl(msisdn, '') ||'rat' || nvl(rat, ''))) hash from zong2;   
select distinct id, import_id, msisdn, imei_norm, imsi, connection_date, rat, rawtohex(hash('id' || nvl(id, '') ||'connection_date' || nvl(connection_date, '') ||'imei_norm' || nvl(imei_norm, '') ||'imsi' || nvl(imsi, '') ||'msisdn' || nvl(msisdn, '') ||'rat' || nvl(rat, ''))) hash from zong3;   
select distinct id, import_id, msisdn, imei_norm, imsi, connection_date, rat, rawtohex(hash('id' || nvl(id, '') ||'connection_date' || nvl(connection_date, '') ||'imei_norm' || nvl(imei_norm, '') ||'imsi' || nvl(imsi, '') ||'msisdn' || nvl(msisdn, '') ||'rat' || nvl(rat, ''))) hash from zong4;   
select distinct id, import_id, msisdn, imei_norm, imsi, connection_date, rat, rawtohex(hash('id' || nvl(id, '') ||'connection_date' || nvl(connection_date, '') ||'imei_norm' || nvl(imei_norm, '') ||'imsi' || nvl(imsi, '') ||'msisdn' || nvl(msisdn, '') ||'rat' || nvl(rat, ''))) hash from zong5; 

select distinct id, import_id, msisdn, imei_norm, imsi, connection_date, rat, rawtohex(hash('id' || nvl(id, '') ||'connection_date' || nvl(connection_date, '') ||'imei_norm' || nvl(imei_norm, '') ||'imsi' || nvl(imsi, '') ||'msisdn' || nvl(msisdn, '') ||'rat' || nvl(rat, ''))) hash from telenor1;   
select distinct id, import_id, msisdn, imei_norm, imsi, connection_date, rat, rawtohex(hash('id' || nvl(id, '') ||'connection_date' || nvl(connection_date, '') ||'imei_norm' || nvl(imei_norm, '') ||'imsi' || nvl(imsi, '') ||'msisdn' || nvl(msisdn, '') ||'rat' || nvl(rat, ''))) hash from telenor2;   
select distinct id, import_id, msisdn, imei_norm, imsi, connection_date, rat, rawtohex(hash('id' || nvl(id, '') ||'connection_date' || nvl(connection_date, '') ||'imei_norm' || nvl(imei_norm, '') ||'imsi' || nvl(imsi, '') ||'msisdn' || nvl(msisdn, '') ||'rat' || nvl(rat, ''))) hash from telenor3;   
select distinct id, import_id, msisdn, imei_norm, imsi, connection_date, rat, rawtohex(hash('id' || nvl(id, '') ||'connection_date' || nvl(connection_date, '') ||'imei_norm' || nvl(imei_norm, '') ||'imsi' || nvl(imsi, '') ||'msisdn' || nvl(msisdn, '') ||'rat' || nvl(rat, ''))) hash from telenor4;   
select distinct id, import_id, msisdn, imei_norm, imsi, connection_date, rat, rawtohex(hash('id' || nvl(id, '') ||'connection_date' || nvl(connection_date, '') ||'imei_norm' || nvl(imei_norm, '') ||'imsi' || nvl(imsi, '') ||'msisdn' || nvl(msisdn, '') ||'rat' || nvl(rat, ''))) hash from telenor5; 


