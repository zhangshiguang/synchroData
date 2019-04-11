#!/usr/bin/ruby -w
#encoding:utf-8
#########################################################
# Sublime  ⌘+B keys 'build'this file ,is running the file
#  by jordan  2017-02 
# 环境：ruby2.2.6p396 by rubyinstaller2.2.6
# gem install writeexcel,mysql2,spreadsheet
#########################################################
# 同步WMS数据
#########################################################
require 'rubygems'
require 'mysql2'
require 'json'
require 'date'
require 'stringio'
require 'tiny_tds'    # mssql driver

require "./globlevar.rb"
$stdout.sync = true

$Update =  TRUE  #false #
$ALLTime     =1
$Last3Months =1
$Last1Months =1
$Last3Days   =1

if  $Update then
	$ALLTime  =9000
	$Last3Months =99
	$Last1Months =33
	$Last3Days   =3
end

END {  
  $myclient.close
  $remotDbClient.close 
}

begin

	$myclient = Mysql2::Client.new(
	  :host     => '127.0.0.1',
	  :port     => 3306,   
	  :username => 'root', 
	  :password => '12345678', 
	  :database => $localDB,
	  :encoding => 'utf8mb4'
	  )
	$remotDbClient = Mysql2::Client.new(
	  :host     =>  '192.168.1.4',  
	  :port     =>  3306,
	  :username =>  'reader', 
	  :password =>  'reader', 
	  :database =>  $remoteDB,  
	  :encoding => 'utf8mb4'          
	  )
	
	
 
    ## 同步数据到本地
	Transmission_mode = true         #全量同步
    #Transmission_mode = false       #增量同步
	##########################################################################################
	# ##show variables like '%sql_mode%';
	# ##STRICT_TRANS_TABLES,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION
	sql ="set @@sql_mode='NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION';"
	result = $myclient.query(sql) 
	sql ="SET @@global.sql_mode= '';"
	result = $myclient.query(sql) 
 
	synchronization_updatebydays("table_1",Transmission_mode) 
    synchronization_updatebydays("table_2",Transmission_mode) 
      
=begin	 
	sql ="delete from testable where isnull(username);"
	$myclient.query(sql) 
=end	
	 
 
 
rescue Exception => e
	puts "#{__FILE__} ...XXXXXXXXXXXXXXXXXXXXXXXX..."<<e.inspect
	puts $@
	puts $.
	log "#{__FILE__}  ..."<<e.inspect
	log $@
	log $.
end
