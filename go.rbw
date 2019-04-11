#!/usr/bin/ruby -w
#encoding:utf-8
#########################################################
# Sublime  ⌘+B keys 'build'this file ,is running the file
#  by jordan  2017-02 
# 环境：ruby2.2.6p396 by rubyinstaller2.2.6
# gem install writeexcel,mysql2,spreadsheet
#########################################################
#

require 'rubygems'

require "./globlevar.rb"
$stdout.sync = true

log "begin=================" + Time.now.strftime('%F')
END {
  #  
}

begin 
	th1=Thread.new{
	    ### 
	    success = system("ruby ./go_wms.rb")
		if success 	then 
			puts "---------------- go_wms.rb  ---------------- ok"
		else
			puts "---------------- go_wms.rb  ------X-X-X-X-X-X-X-X------ failed!!"
		end
		
	}
	th1.join
	
rescue Exception => e
	puts e.inspect
end

