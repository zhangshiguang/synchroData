#!/usr/bin/ruby -w
#encoding:utf-8
#########################################################
# Sublime  ⌘+B keys 'build'this file ,is running the file
#  by jordan  2017-02 
# 环境：ruby2.2.6p396 by rubyinstaller2.2.6
# gem install writeexcel,mysql2,spreadsheet
#
# globlevar.rb
#########################################################
#
#########################################################
require 'rubygems'
require 'date'
require 'time'

$localDB = 'localDB'  
$remoteDB='remoteDB'

$turn = 1
$groupsarray = Array.new

$step=0
$asciiflag = ['-','\\','|','/']
$interCustmer = "'test1','test2'"


#因为金蝶ERP从2016年10月开始使用，故ERP相关的统计都从此月开始
$monthArr_erp = Array["2016-10","2016-11","2016-12"]
startD= Date.new(2017,1)
while startD <= DateTime.now do
	$monthArr_erp.push(startD.strftime("%Y-%m"))   
	startD = startD.next_month()
end 

def initTitle(table)
	$memaxid=0 
    $maxid=0 
	s = "#{$turn} "
	$turn.times do 
		s += "-"
	end
	puts "\n"<<s<<">"<<table
	$turn += 1
	$tablename = table
	$fieldslist = []
	$fieldslist_me = []
	$fieldshash_type = Hash.new
	$fieldshash_nullable = Hash.new
	check_table_0($tablename)     #定义是否需要重建函数
	check_table_1($tablename)     #定义是否需要创建函数
	check_record_0($tablename)    #定义获取ID差异函数
	synchronization_0($tablename) #定义同步函数

end
##
# Calculates the difference in years and month between two dates
# Returns an array [year, month]
def date_diff(date1,date2)
  return (date2.year * 12 + date2.month) - (date1.year * 12 + date1.month)
  
end
def date_diff_ex(date1,date2)
  return (date2.year * 12 + date2.month) - (date1.year * 12 + date1.month) + 1
  
end

def synchronization_updatebydays(table,flag,days=0)
	
	#update	
	maxid = 0
	minid = 0
	initTitle(table)
	eval("check_table_#{table}()")
	eval("check_create_table_#{table}()")
	sql =''
	if 0<days then
		if  $fieldslist.include?('create_date') then
			sql = "select max(id) as maxid,min(id) as minid from `#{table}` where create_date >'#{DateTime.now.prev_day(days).strftime("%Y-%m-%d")}' ;"
		else 
			sql = "select max(id) as maxid,min(id) as minid from `#{table}` ;"
		end	
		#print sql 
		result = $remotDbClient.query(sql)
		result.each do |row| 
			maxid = (nil != row['maxid'])? row['maxid']: 0
			minid = (nil != row['maxid']) ? row['minid'] : 0
			#puts(">>>MAX id  is #{mmaxid}，MIN id is #{mminid}. #{Time.now}")
		end
		puts("MAX id  is #{maxid}，MIN id is #{minid}. #{Time.now}")
		if maxid > minid then 
			page = 100
			start = minid
			while true
				
				sql = "select * from `#{table}` where id > #{start} order by id limit #{page};";
				
				result = $remotDbClient.query(sql)
				puts(sql);
				if result.size >0 then
					result.each do |row| 
						start = row['id']
						# print("#{row['id']} ");   ## eval 内打印语句需加分号表示结束，否则解释器可能会把下一行解释为打印内容。
						sql = "update #{table} set ";            
						for f in $fieldslist
							# puts("row['#{f}'] =#{row["#{f}"]}, #{$fieldshash_type["#{f}"]}");
							if f != 'id' then 
								sql += f + '='
								
								fv ='' ;
								if $fieldshash_type["#{f}"] == 'bit(1)' then
									fv = checkfield_ex_bit(row["#{f}"],$fieldshash_nullable["#{f}"]) ;
								elsif $fieldshash_type["#{f}"] == 'datetime' then	
									fv = checkfield_ex_datetime(row["#{f}"],$fieldshash_nullable["#{f}"]) ;
								else	
									fv = checkfield_ex(row["#{f}"],$fieldshash_nullable["#{f}"]) ;
								end	
								sql += fv ;
							end	
						end
						sql.chop!
						sql += " where id=#{row["id"]};" ;
						#print '>' ;#puts(sql );
						$myclient.query(sql) 
					end
					#start += page
					
				else 
					break
				end
			end
		else 	
		end
	end	
	#puts("update ok at #{Time.now}")
	# insert （更新完旧数据，插入新数据）
	synchronization_do(table,flag) 
end

def synchronization_do(table,flag)
	#puts("synchronization_do(#{table},#{flag})")
	from_0 = false         #和本地比较
	if from_0 then  
		$memaxid = 0   
		sql ="delete from #{table} where 1=1;"
		$myclient.query(sql)
	end
	eval ("check_table_#{table}()")
	eval ("check_create_table_#{table}()")
    while true
        eval("check_record_#{table}()")
        if $maxid>$memaxid then
            eval("synchronization_#{table}()")
        else
            break
        end
    end
end
def synchronization(table,flag)
	initTitle(table)
	
	synchronization_do(table,flag)	
end

def synchronization_new(table,flag)
	initTitle(table)
	
	from_0 = flag         #和本地比较
	if from_0 then  
		$memaxid = 0   
		sql ="delete from #{$tablename} where 1=1;"
		$myclient.query(sql)
	end
	eval ("check_table_#{$tablename}()")
	eval ("check_create_table_#{$tablename}()")
    while true
        eval("check_record_#{$tablename}()")
        if $maxid>$memaxid then
            eval("synchronization_#{$tablename}()")
        else
            break
        end
    end
end
### 仅仅完全更新最近一周的数据
def synchronization_new2(table,flag)
	initTitle(table)
	
	from_0 = flag         #和本地比较
	if from_0 then  
		$memaxid = 0   
		sql ="delete from #{$tablename} where 1=1 and `create_date`>'#{(Date.today()-3).strftime("%Y-%m-%d")}';"
		print sql
		$myclient.query(sql)
	end
	eval ("check_table_#{$tablename}()")
	eval ("check_create_table_#{$tablename}()")
    while true
        eval("check_record_#{$tablename}()")
        if $maxid>$memaxid then
            eval("synchronization_#{$tablename}()")
        else
            break
        end
    end
end


#
#写log
#$file_log.syswrite("=================================================\n") 
def log(logstr)
	if $file_log.nil? then $file_log =File.new('222.log.txt','a+') end
	$file_log.syswrite logstr 
	$file_log.syswrite"\n"
end
#$file_log.close
def checkfun(name)
		##if not exists (select * from information_schema.VIEWS where table_schema ='aiyashop2017.617' and table_name='v_custmers0crm') 
		##begin	##end
	sql="select * from information_schema.TABLES where table_schema ='aiyashop2017.617' and table_name='#{name}';"
	result = $client.query(sql) 
	if result.size <1 then
		(1..3).each do puts "Table or View \"#{name}\" doesn't exists,please create it first!!!!" end
		exit
	end
end
def checkfield(a)
	if a != nil and a != '' then	
		if a.class == "String".class then
			a=a.delete("\'")
			a=a.delete("\r\n")
			a=a.delete("\t")
			a = a.gsub("\\", "/")
			a = a.strip()
		end
		if a=='' then
			a="NULL"
		else	
			a="\'" + a.to_s + "\'"
		end	
	else
		a="NULL"
	end
	return a
end	
def checkfield_null(a)
	if a != nil and a != '' then	
		if a.class == "String".class then
			a=a.delete("\'")
			a=a.delete("\r\n")
			a=a.delete("\t")
			a = a.gsub("\\", "/")
			a = a.strip()
		end
		if a=='' then
			a="NULL"
		else	
			a="\'" + a.to_s + "\'"
		end	
	else
		a="NULL"
	end
	return a
end
def checkfield_ex2(a)
	if a != nil and a != ''  then	
		if a.class == "String".class then
			a=a.delete("\'")
			a=a.delete("\r\n")
			a=a.delete("\t")
			a = a.gsub("\\", "/")
			a = a.strip()
		end
		if a=='' then
			a="NULL,"
		else	
			a="\'" + a.to_s + "\',"
		end	
	else
		a="NULL,"
	end
	return a
end
def checkfield_ex_null(a)
	if a != nil and a != ''  then	
		if a.class == "String".class then
			a=a.delete("\'")
			a=a.delete("\r\n")
			a=a.delete("\t")
			a = a.gsub("\\", "/")
			a = a.strip()
		end
		if a=='' then
			a="NULL,"
		else	
			a="\'" + a.to_s + "\',"
		end	
	else
		a="NULL,"
	end
	return a
end
def checkfield_ex_empty(a)
	if a != nil and a != ''  then	
		if a.class == "String".class then
			a=a.delete("\'")
			a=a.delete("\r\n")
			a=a.delete("\t")
			a = a.gsub("\\", "/")
			a = a.strip()
		end
		if a=='' then
			a="'',"
		else	
			a="\'" + a.to_s + "\',"
		end	
	else
		a="'',"
	end
	return a
end
def checkfield_empty(a)
	if a != nil and a != ''  then	
		if a.class == "String".class then
			a=a.delete("\'")
			a=a.delete("\r\n")
			a=a.delete("\t")
			a = a.gsub("\\", "/")
			a = a.strip()
		end
		if a=='' then
			a="''"	
		else
			a="\'" + a.to_s + "\'"
		end
	else
		a="''"
	end
	return a
end	

def checkfield_bit(a)
	if a != nil and a != ''  then	
		if a.class == "String".class then
			a=a.delete("\'")
			a=a.delete("\r\n")
			a=a.delete("\t")
			a = a.gsub("\\", "/")
			a = a.strip()
		end
		if a=='' then
			a="0"	
		else
			a= a.to_s 
		end
	else
		a="0"
	end
	return a
end	
def checkfield_ex_bit(a,is_nullable)
	if a != nil and a != ''  then	
		if a.class == "String".class then
			a=a.delete("\'")
			a=a.delete("\r\n")
			a=a.delete("\t")
			a = a.gsub("\\", "/")
			a = a.strip()
		end
		if a=='' then
			if is_nullable=='YES' then
				a = "NULL,"	
			else
				a = "0,"	
			end	
		else
			a = "'" + a.to_s + "',"
		end
	elsif is_nullable=='YES' then
		a = "NULL,"		
	else
		a="0,"
	end
	return a
end
def checkfield_ex_datetime(a,is_nullable)	
	if a != nil and a != ''  then	
		if a.class == "String".class then
			a=a.delete("\'")
			a=a.delete("\r\n")
			a=a.delete("\t")
			a = a.gsub("\\", "/")
			a = a.strip()
		end
		if a=='' then
			if is_nullable=='YES' then
				a = "NULL,"	
			else
				a = "'',"
			end	
		else    ## '2017-11-28 16:51:58 +0800'    
			a = "'" + a.to_s.gsub(/ \+[0-9]{4}/,"") + "',"
		end
	elsif is_nullable=='YES' then
		a = "NULL,"
	else
		a = "'',"
	end
	return a
end
def checkfield_ex(a,is_nullable)	
	if a != nil and a != ''  then	
		if a.class == "String".class then
			a=a.delete("\'")
			a=a.delete("\r\n")
			a=a.delete("\t")
			a = a.gsub("\\", "/")
			a = a.strip()
		end
		if a=='' then
			if is_nullable=='YES' then
				a = "NULL,"	
			else
				a = "'',"
			end	
		else    
			a = "'" + a.to_s + "',"
		end
	elsif is_nullable=='YES' then
		a = "NULL,"
	else
		a = "'',"
	end
	return a
end


def check_table_0(tablename)	
	eval ("def  check_table_#{tablename}()
		$fieldslist.clear
		$fieldslist_me.clear
		$fieldshash_type.clear
		$fieldshash_nullable.clear
		
		#puts (\"call check_table_#{$tablename} \");
		sql = \"select column_name,column_type,is_nullable,column_default,character_set_name,collation_name,column_comment,column_key,extra from INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA='\#{$aiyadb}' and TABLE_NAME='\#{$tablename}';\";
		result = $remotDbClient.query(sql)	
		result.each do |row|
			$fieldslist.push(row['column_name'])
			$fieldshash_type[row['column_name']] = row['column_type']
			$fieldshash_nullable[row['column_name']] = row['is_nullable']
		end	 
		#puts( $fieldslist );
		#puts( $fieldshash_type);
		#puts( $fieldshash_nullable);
		sql = \"select column_name,column_type,is_nullable,column_default,character_set_name,collation_name,column_comment,column_key,extra from INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA='\#{$database}' and TABLE_NAME='\#{$tablename}';\";
		result = $myclient.query(sql)	
		 
		result.each do |row|
			$fieldslist_me.push(row['column_name'])	
		end	
		 
		$gsql = \"insert  into `\#{$tablename}`(\";
		for f in $fieldslist
			$gsql += \"`\" + f + \"`,\";
		end
		$gsql = $gsql.chop	; ###chop:去掉字符串末尾的最后一个字符
		$gsql += \")VALUES(\";
		############################如果表结构已经变化，则重命名本地表，重新拉新表
		na= $fieldslist - $fieldslist_me 
		if na.length != 0 and result.size>0 then
			sql=\"ALTER TABLE \#{$tablename} RENAME \#{$tablename}\#{DateTime.now.strftime(\"%Y%m%d%H%M%S\")};\";
			result = $myclient.query(sql)
		end
		 
		end")
end
## str.gsub(/,\s+CONSTRAINT (.+\n){1,10}\) ENGINE=InnoDB/,\"\r\n\) ENGINE=InnoDB\")
def check_table_1(tablename)
	eval ("def check_create_table_#{tablename}()
		#puts (\"call check_create_table_#{$tablename} \");
		sql = \"select * from information_schema.TABLES where table_schema ='\#{$database}' and table_name='\#{$tablename}';\"
		result = $myclient.query(sql)
		if result.size < 1 then
			sql = \"show create table `\#{$aiyadb}`.`\#{$tablename}`;\"
			result = $remotDbClient.query(sql)			 
			result.each do |row|
				sql =  row['Create Table']
				#puts(sql);
				# 重置 自增
				sql = sql.gsub(/AUTO_INCREMENT=[0-9]+/,\"AUTO_INCREMENT=1 \")
				# 去除外键
				
				sql = sql.gsub(/,\\s+CONSTRAINT (.+\\n){1,10}\\) ENGINE=InnoDB/,\"\\r\\n\)ENGINE=InnoDB \");
				#puts(sql);

				#
				result = $myclient.query(sql)
				#puts($gsql);
				#puts('=============================')
			end
		end

		
		end")	
end 

def check_record_0(tablename)
	eval("def check_record_#{tablename}()
	#puts(\"call check_record_#{tablename}()\")
    sql = \"select id from \#{$tablename} order by id desc limit 1;\"
    result = $myclient.query(sql)
    if result.size > 0 then
        result.each do |row|
            $memaxid = row['id']
        end
    end
     
    result = $remotDbClient.query(sql)
    result.each do |row|
        $maxid = row['id']
    end
	# - 45 \ 45+47=92 - 92-47=45 / 45+2=47 - 47-2=45
	 
    #print (\"$maxid=\#{$maxid},$memaxid=\#{$memaxid} \");
	print (\"\#{$tablename}................\#{$asciiflag[$step%4]} [\#{$memaxid*100/$maxid}%] \\r\") if $maxid != 0;
	$step += 1
	#print (\"\#{$memaxid} \");
	
    end")
end

####如果字段类型是bit(1)，当为0时取回的数据为空
#####<Mysql2::Error: Incorrect datetime value: '2017-11-28 16:51:58 +0800' for column 'create_date' at row 1>
    
def synchronization_0(tablename)
	eval("def synchronization_#{tablename}() 
	#puts(\"call synchronization_#{tablename}() \");
	sql = \"select * from `\#{$tablename}` where id>'\#{$memaxid}' order by id limit 120;\";
	#puts(sql); 
	result = $remotDbClient.query(sql);
	 
	result.each do |row| 
		#puts (row);
		$memaxid = row[\"id\"] ;
		# puts(\"\#{row['id']} \");   ## eval 内打印语句需加分号表示结束，否则解释器可能会把下一行解释为打印内容。
		sql = $gsql;            
		for f in $fieldslist
			# puts(\"row['\#{f}'] =\#{row[\"\#{f}\"]}, \#{$fieldshash_type[\"\#{f}\"]}\");
			fv ='' ;
			if $fieldshash_type[\"\#{f}\"] == 'bit(1)' then
				fv = checkfield_ex_bit(row[\"\#{f}\"],$fieldshash_nullable[\"\#{f}\"]) ;
			elsif $fieldshash_type[\"\#{f}\"] == 'datetime' then	
				fv = checkfield_ex_datetime(row[\"\#{f}\"],$fieldshash_nullable[\"\#{f}\"]) ;
			else	
				fv = checkfield_ex(row[\"\#{f}\"],$fieldshash_nullable[\"\#{f}\"]) ;
			end	
			
			sql += fv ;
		end
		sql = sql.chop  ;
		sql += \");\" ;
		#puts(sql );
		$myclient.query(sql) 
	end 
   
	end")
end	

def synchronization_1(tablename)
	eval("def synchronization_1#{tablename}() 
	
	sql = \"select * from `\#{$tablename}` where id>'\#{$memaxid}' order by id limit 100;\";
	 
	result = $remotDbClient.query(sql)
	
	result.each do |row| 
		$memaxid = row[\"id\"] ;
		# print(\"\#{row['id']} \");   ## eval 内打印语句需加分号表示结束，否则解释器可能会把下一行解释为打印内容。
		sql = $gsql;            
		for f in $fieldslist
			# puts(\"row['\#{f}'] =\#{row[\"\#{f}\"]}, \#{$fieldshash_type[\"\#{f}\"]}\");
			fv ='' ;
			if $fieldshash_type[\"\#{f}\"] == 'bit(1)' then
				fv = checkfield_ex_bit(row[\"\#{f}\"],$fieldshash_nullable[\"\#{f}\"]) ;
			elsif $fieldshash_type[\"\#{f}\"] == 'datetime' then	
				fv = checkfield_ex_datetime(row[\"\#{f}\"],$fieldshash_nullable[\"\#{f}\"]) ;
			else	
				fv = checkfield_ex(row[\"\#{f}\"],$fieldshash_nullable[\"\#{f}\"]) ;
			end	
			
			sql += fv ;
		end
		sql = sql.chop  ;
		sql += \");\" ;
		# puts(sql );
		$myclient.query(sql) 
	end 
   
	end")
end	


####如果字段类型是bit(1)，当为0时取回的数据为空
#####<Mysql2::Error: Incorrect datetime value: '2017-11-28 16:51:58 +0800' for column 'create_date' at row 1>


def exportfile(filename,sqlstr,titlestr,sqlclient)
	fn ="1.csv"
    fsn = "\"d:/tmp/customer/#{fn}\"" 
	#filename = "#{$starttime_year_month}至#{$endtime_year_month}各品牌年度销量合计.csv"
	if File::exist?("D:\\tmp\\customer\\#{fn}")  then
	   File::delete("D:\\tmp\\customer\\#{fn}")
	end
	if File::exist?("D:\\tmp\\customer\\#{filename}")  then
	   File::delete("D:\\tmp\\customer\\#{filename}")
	end
	sqlstr = sqlstr.gsub(';',' ') + " "
	sql = sqlstr + 
		" INTO OUTFILE  #{fsn} 
		Fields terminated by ',' 
		enclosed by '' 
		lines TERMINATED by '\\r\\n';".force_encoding('utf-8'); 
	sqlclient.query( sql); 
	
	fsize = File::size("D:/tmp/customer/#{fn}")
	File::open("D:/tmp/customer/#{fn}",mode='r+'){|csv|
				old =csv.sysread(fsize)
                csv.reopen("D:/tmp/customer/#{fn}",mode='w')             
			    csv.puts titlestr 
				csv.puts old				 
			    }	
    File::rename("D:\\tmp\\customer\\#{fn}","D:\\tmp\\customer\\#{filename}")	
	puts "D:\\tmp\\customer\\#{filename}"
end
 
def dropifexists(type,tname,mysqlclient)
	puts "drop #{type} if exists #{tname}; ....#{Time.now}"
	mysqlclient.query("drop #{type} if exists #{tname};")
end 
def doquery(sqlstr,mysqlclient)
	puts "#{sqlstr}....#{Time.now}"
	mysqlclient.query(sqlstr)
end 

def checkifexists(tablename,mysqlclient)
	result = mysqlclient.query("SHOW TABLES LIKE '#{tablename}';")
	if result.size>0 then return true end
	return false
end

def gettablemaxfiled(column,tablename,mysqlclient)	
	result = mysqlclient.query("select max(`#{column}`) as maxcolumn from #{tablename};")
	if result.size>0 then
		result.each do |item|
			return item['maxcolumn'].to_s
		end
	end
end