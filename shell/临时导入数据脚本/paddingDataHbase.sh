###工商表数据导入
hbase org.apache.hadoop.hbase.mapreduce.ImportTsv  -Dimporttsv.separator=$(echo -e '\001') -Dimporttsv.columns=HBASE_ROW_KEY,Registered_Info:registrationno,Registered_Info:enterprisename,Registered_Info:province,Registered_Info:legalrepresentative,Registered_Info:enterprisetype,Registered_Info:establishmentdate,Registered_Info:registeredcapital,Registered_Info:registeredcurrency,Registered_Info:residenceaddress,Registered_Info:validityfrom,Registered_Info:validityto,Registered_Info:businessscope,Registered_Info:registrationinstitution,Registered_Info:approvaldate,Registered_Info:registrationstatus,Registered_Info:principal,Registered_Info:businessplace,Registered_Info:lastupdatetime,Registered_Info:revocationdate,Registered_Info:investor,Registered_Info:mianbusinessplace,Registered_Info:partnershipfrom,Registered_Info:partnershipto,Registered_Info:executivepartner,Registered_Info:compositionform,Registered_Info:operator,Registered_Info:chiefrepresentative,Registered_Info:enterpriseassigning,Registered_Info:tyshxy_code,Registered_Info:totalcontributionofmembers,Registered_Info:dt  GS /user/hive/warehouse/temp.db/t_pachong_registered_info


#####股东信息表导入数据
hbase org.apache.hadoop.hbase.mapreduce.ImportTsv  -Dimporttsv.separator=$(echo -e '\001') -Dimporttsv.columns=HBASE_ROW_KEY,Shareholder_Info:shareholder_type,Shareholder_Info:shareholder_name,Shareholder_Info:shareholder_certificationtype,Shareholder_Info:shareholder_certificationno,Shareholder_Info:subscripted_capital,Shareholder_Info:subscripted_method,Shareholder_Info:subscripted_amount,Shareholder_Info:subscripted_time,Shareholder_Info:actualpaid_capital,Shareholder_Info:actualpaid_method,Shareholder_Info:actualpaid_amount,Shareholder_Info:actualpaid_time,Shareholder_Info:lastupdatetime,Shareholder_Info:shareholder_details,Shareholder_Info:detailflag,Shareholder_Info:id,Shareholder_Info:dt GS /user/hive/warehouse/temp.db/t_pachong_shareholder_info


#####主要人员导入数据
hbase org.apache.hadoop.hbase.mapreduce.ImportTsv  -Dimporttsv.separator=$(echo -e '\001') -Dimporttsv.columns=HBASE_ROW_KEY,KeyPerson_Info:keyperson_no,KeyPerson_Info:keyperson_name,KeyPerson_Info:keyperson_position,KeyPerson_Info:lastupdatetime,KeyPerson_Info:id,KeyPerson_Info:dt GS /user/hive/warehouse/temp.db/t_pachong_keyperson_info

###搜索表导入数据
hadoop  jar /root/kpi/etl.jar  org.hbase.data.JobMain
