#!/bin/bash
# 处理网站、内容组、站点对应的ID

cd /data/wwwroot/trackinganalytics/basedata
/usr/local/php/bin/php isreport_ids.php  	## 所有开通报告的网站ID
/usr/local/php/bin/php iswebsite_ids.php 	## 启用的站点ID
/usr/local/php/bin/php iscgroup_ids.php 	## 启用的内容组ID



