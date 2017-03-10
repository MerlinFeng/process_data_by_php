<?php
// +----------------------------------------------------------------------
// | Copyright (c) 2016 All rights reserved.
// +----------------------------------------------------------------------
// | Author: Jerry.fang <fangzq@yiche.com>
// +----------------------------------------------------------------------

// 注册 autoload 函数
spl_autoload_register(function ($name) {
    $f = '_base' . strtolower(preg_replace('/[A-Z]/', "/$0", $name)) . '.inc';
    include $f;
});

register_shutdown_function(function () {
    unset($_ENV);
});

MongoCursor::$timeout = -1;					// mongo操作链接不超时
header("Content-Type:text/html; charset=utf-8");
ini_set('date.timezone', 'Asia/Shanghai');
ini_set("max_execution_time", 60*60*24*3);	// 程序运行最大时长cd ..
ini_set("memory_limit", '20240M');

require '_base/db.php';
require '_base/mongodb.php';
require '_base/mongo.php';
require '_base/define.php';
require '_base/function.php';
error_reporting(E_ALL);
// 接收命令传参
if (isset($argv[1])) {
	$rtime = $argv[1];
} else {
	$rtime = '20170302';
}


// 简单验证下时间
/*if (date('Ymd') <= $time) {
	exit('日期不对');
}*/

$start_time = time();
//FilesClicks::mongoImport($rtime, 'dcclog', $_ENV['_db_configs']['_mongos']['dns_dcclog']);                       // 行为日志
//FilesClicks::mongoImport($rtime, 'leads', $_ENV['_db_configs']['_mongos']['dns_leads']);                 // 线索日志
$config = $_ENV['_db_configs'];
$mongo_config = $config['_mongos']['dns_dcclog_163'];

//DatasClick::dodata($rtime);			// 点击报告
//DatasEvent::dodata($rtime);              //事件报告
//DatasLeads::dodata($rtime);			// 线索报告
//DatasIntdrainagebegin::dodata($rtime, $mongo_config);	// 内部引流报告 初始来源
//DatasIntdrainage::dodata($rtime, $mongo_config);	// 内部引流报告 最终来源
//DatasWebsites::dodata($rtime, $mongo_config);		// 网站报告
//DatasSites::dodata($rtime, $mongo_config);              // 域名报告
//DatasDomains::dodata($rtime, $mongo_config);		// 域名报告
//DatasPages::dodata($rtime, $mongo_config);			// 页面报告
//DatasCgroup::dodata($rtime, $mongo_config);             // 内容组报告

//DatasSourcesearch::instance()->do_data($rtime, $mongo_config);
//DatasSourcedirect::instance()->do_data($rtime, $mongo_config);
//DatasPagesin::dodata($rtime, $mongo_config);			// 初始页面报告
//DatasSourcelink::instance()->do_data($rtime, $mongo_config, 'recomand_domain'); //外部链接url
//DatasSourcelink::instance()->do_data($rtime, $mongo_config, 'recomand_url'); //外部链接domain
DatasAutovalue::instance()->do_data($rtime, $mongo_config);
//线索数据
//DatasSourcesearch::instance()->do_data($rtime, $config['_mongos']['dns_leads']); //搜索引擎
//DatasSourcedirect::instance()->do_data($rtime, $config['_mongos']['dns_leads']);//直接输入
//DatasSourcelink::instance()->do_data($rtime, $config['_mongos']['dns_leads'], 'recomand_domain'); //外部链接url
//DatasSourcelink::instance()->do_data($rtime, $config['_mongos']['dns_leads'], 'recomand_url'); //外部链接domain



echo $start_time . '||' . time() . "\n";

