insert overwrite table temp.t_pachong_shareholder_info_02
SELECT concat(k2.enterprisename,'_04_',k2.registrationno,'_',k2.id),
k2.registrationno,
k2.enterprisename,
k2.shareholder_type,
k2.shareholder_name,
k2.shareholder_certificationtype,
k2.shareholder_certificationno,
k2.subscripted_capital,
k2.subscripted_method,
k2.subscripted_amount,
k2.subscripted_time,
k2.actualpaid_capital,
k2.actualpaid_method,
k2.actualpaid_amount,
k2.actualpaid_time,
k2.lastupdatetime,
k2.shareholder_details,
k2.detailflag,
k2.id,
k2.dt
FROM
(SELECT  k.registrationno,
r.enterprisename,
k.shareholder_type,
k.shareholder_name,
k.shareholder_certificationtype,
k.shareholder_certificationno,
k.subscripted_capital,
k.subscripted_method,
k.subscripted_amount,
k.subscripted_time,
k.actualpaid_capital,
k.actualpaid_method,
k.actualpaid_amount,
k.actualpaid_time,
k.lastupdatetime,
k.shareholder_details,
k.detailflag,
k.id,
k.dt
FROM (select *,ROW_NUMBER() OVER(PARTITION BY registrationno,shareholder_type,shareholder_name,shareholder_certificationtype,shareholder_certificationno order by dt desc) rank  from ods.pachong_shareholder_info where dt='temp') k 
JOIN (SELECT registrationno,enterprisename,max(dt) FROM ods.pachong_registered_info 
WHERE dt='temp'   and enterprisename not like '%有限合伙%'
group by registrationno,enterprisename) r 
ON k.registrationno=r.registrationno and k.rank=1
) k2;

insert overwrite table temp.t_pachong_shareholder_info
SELECT concat(k2.enterprisename,'_04_',k2.registrationno,'_',k2.id),
k2.shareholder_type,
k2.shareholder_name,
k2.shareholder_certificationtype,
k2.shareholder_certificationno,
k2.subscripted_capital,
k2.subscripted_method,
k2.subscripted_amount,
k2.subscripted_time,
k2.actualpaid_capital,
k2.actualpaid_method,
k2.actualpaid_amount,
k2.actualpaid_time,
k2.lastupdatetime,
k2.shareholder_details,
k2.detailflag,
k2.id,
k2.dt
FROM
(SELECT  k.registrationno,
r.enterprisename,
k.shareholder_type,
k.shareholder_name,
k.shareholder_certificationtype,
k.shareholder_certificationno,
k.subscripted_capital,
k.subscripted_method,
k.subscripted_amount,
k.subscripted_time,
k.actualpaid_capital,
k.actualpaid_method,
k.actualpaid_amount,
k.actualpaid_time,
k.lastupdatetime,
k.shareholder_details,
k.detailflag,
k.id,
k.dt
FROM (select *,ROW_NUMBER() OVER(PARTITION BY registrationno,shareholder_type,shareholder_name,shareholder_certificationtype,shareholder_certificationno order by dt desc) rank  from ods.pachong_shareholder_info where dt='temp') k 
JOIN (SELECT registrationno,enterprisename,max(dt) FROM ods.pachong_registered_info 
WHERE dt='temp'   and enterprisename not like '%有限合伙%'
group by registrationno,enterprisename) r 
ON k.registrationno=r.registrationno and k.rank=1
) k2;


insert overwrite table temp.t_pachong_keyperson_info
SELECT concat(k2.enterprisename,'_06_',k2.registrationno,'_',k2.id),
k2.keyperson_no,
k2.keyperson_name,
k2.keyperson_position,
k2.lastupdatetime,
k2.id,
k2.dt
FROM
(SELECT  k.registrationno,
r.enterprisename,k.keyperson_no,k.keyperson_name,k.keyperson_position,
k.lastupdatetime,
k.id,k.dt
FROM (select *,ROW_NUMBER() OVER(PARTITION BY registrationno,keyperson_name,keyperson_position order by id desc) rank from ods.pachong_keyperson_info where dt = 'temp' ) k 
JOIN (SELECT  registrationno,enterprisename,max(dt) dt FROM ods.pachong_registered_info WHERE dt = 'temp' group by registrationno,enterprisename) r ON 
k.registrationno=r.registrationno and k.rank=1
 ) k2;

insert overwrite table temp.t_pachong_keyperson_info_02 
SELECT concat(k2.enterprisename,'_06_',k2.registrationno,'_',k2.id),
k2.registrationno,k2.enterprisename,
k2.keyperson_no,
k2.keyperson_name,
k2.keyperson_position,
k2.lastupdatetime,
k2.id,
k2.dt
FROM
(SELECT  k.registrationno,
r.enterprisename,
k.keyperson_no,
k.keyperson_name,
k.keyperson_position,
k.lastupdatetime,
k.id,
k.dt
FROM (select *,ROW_NUMBER() OVER(PARTITION BY registrationno,keyperson_name,keyperson_position order by id desc) rank from ods.pachong_keyperson_info where dt='temp') k 
JOIN (SELECT  registrationno,enterprisename,max(dt) dt FROM ods.pachong_registered_info WHERE dt = 'temp'   group by registrationno,enterprisename) r ON 
k.registrationno=r.registrationno and k.rank=1
) k2;



add jar /apps/likai/LengJingUdf.jar;
CREATE TEMPORARY FUNCTION parseDate AS 'gongshang.ParseDate';
create temporary function SplitMoney as 'gongshang.SplitMoney';
add jar /root/kpi/glad/moneyParse-0.0.1-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION parseMoney AS 'org.parse.money.moneyParse.Parsemoney';
 insert overwrite table  temp.t_pachong_registered_info
select  t1.* from
(
select concat(r.enterprisename,'_01_',r.registrationno,'_'),r.registrationno,r.enterprisename,r.province,r.legalrepresentative,
r.enterprisetype,parseDate(r.establishmentdate),Parsemoney(split(SplitMoney(r.registeredcapital),',')[0],split(SplitMoney(r.registeredcapital),',')[1]),
if(Parsemoney(split(SplitMoney(r.registeredcapital),',')[0],split(SplitMoney(r.registeredcapital),',')[1]) <> '','万元',''),
r.residenceaddress,r.validityfrom,r.validityto,r.businessscope,r.registrationinstitution,r.approvaldate,r.registrationstatus,r.principal,r.businessplace,r.lastupdatetime,
r.revocationdate,r.investor,r.mianbusinessplace,r.partnershipfrom,r.partnershipto,r.executivepartner,r.compositionform,
r.operator,r.chiefrepresentative,r.enterpriseassigning,r.tyshxy_code,r.totalcontributionofmembers,r.dt
from ods.pachong_registered_info r where  r.dt='temp' ) t1;


insert overwrite table temp.t_lengjingsearch 
select d.key,d.enterprisename,d.legalrepresentative,d.establishmentdate,d.registeredcapital,d.registrationstatus,d.residenceaddress,d.businessscope,t.keyperson_name,s.shareholder_name
from  (select * from temp.t_pachong_registered_info r where 1=1) d
left join (select concat_ws(' ',collect_set(keyperson_name)) keyperson_name,registrationno,enterprisename 
from temp.t_pachong_keyperson_info_02 g group by registrationno,enterprisename) t on t.registrationno=d.registrationno and t.enterprisename=d.enterprisename
left join (select concat_ws(' ',collect_set(shareholder_name)) shareholder_name,registrationno,enterprisename 
from temp.t_pachong_shareholder_info_02 g group by registrationno,enterprisename) s on s.registrationno=d.registrationno and s.enterprisename=d.enterprisename

