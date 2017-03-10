<?php
// +----------------------------------------------------------------------
// | Copyright (c) 2016 All rights reserved.
// +----------------------------------------------------------------------
// | *************
// +----------------------------------------------------------------------

// 注册 autoload 函数
spl_autoload_register(function ($name) {
    $f = '_base/' . strtolower(preg_replace('/[A-Z]/', "/$0", $name)) . '.inc';
    include $f;
});

register_shutdown_function(function () {
    unset($_ENV);
});

//MongoCursor::$timeout = -1;					// mongo操作链接不超时
header("Content-Type:text/html; charset=utf-8");
ini_set('date.timezone', 'Asia/Shanghai');
ini_set("max_execution_time", 60*60*24*3);	// 程序运行最大时长
ini_set("memory_limit", '28240M');

require '_base/db.php';
require '_base/mongodb.php';
require '_base/redis.php';
require '_base/define.php';
require '_base/function.php';

#jobkeeper start
$flag = 'ATquanPSDay_test';
define("atFLAG", $flag);
//error_reporting(E_ALL);


$url = JOBKEEPER_GET_URL."&job={$flag}";
$arrt =  file($url);
if(!$arrt[0]){
	err_log("jobkeeper|get error|{$url}", $flag);
	exit();
}
$arrF = json_decode($arrt[0]);

//判断当前任务是否在运行中，避免启动多个进程
if($arrF->data->lock){
	echo "jobkeeper|lock:1|date:{$arrF->data->date}|exit\n";
	exit();
}
$doDay = date('Y-m-d', strtotime($arrF->data->date)+86400);

//判断前置任务是否就绪
$arrt = explode(',', $arrF->data->parent);
foreach ($arrt as $vp){
	$url = JOBKEEPER_URL_GET."&job={$vp}";
	$arrtParent = file($url);
	if(!$arrtParent[0]){
		err_log("jobkeeper|{$doDay}|exit|parent {$vp} jobkeeper get error|{$url}", $flag);
		exit();
	}
	$arrFParent = json_decode($arrtParent[0]);
	echo "jobkeeper|parent|{$vp}:{$arrFParent->data->date}\n";
	
	if ($doDay > $arrFParent->data->date) {
		echo    "jobkeeper|{$doDay}|exit|{$vp}:{$arrFParent->data->date}|before|no ok\n";
		//err_log("jobkeeper|{$doDay}|exit|{$vp}:{$arrFParent->data->date}|before|no ok", $flag);
		exit; 
	}
}

err_log("jobkeeper|last date:{$arrF->data->date}", $flag);

//lock
$url = JOBKEEPER_PUT_URL."&job={$flag}&lock=1";
err_log("{$doDay}|jobkeeper|lock|{$url}", $flag);
file($url);
#jobkeeper end

$unixTime = strtotime($doDay);
$rtime = date('Ymd', $unixTime);	// 日期，到天
$time = date('YmdH', $unixTime);	// 日期，到小时
$hour = date('H', $unixTime);		// 日期，只有小时

$start_time = time();
err_log("{$doDay}|domongo|rtime:{$rtime}|start + + + + + + + + + + + + + + + + + + + +", $flag);

//全网数据单独处理
$config = $_ENV['_db_configs'];
$mongo_config = $config['_mongos']['dns_dcclog_10'];
//DatasIntdrainagebegin::dodata($rtime, $mongo_config);	// 内部引流报告 初始来源
//DatasIntdrainage::dodata($rtime, $mongo_config);	// 内部引流报告 最终来源
DatasWebsites::dodata($rtime, $mongo_config);		// 网站报告
//DatasSites::dodata($rtime, $mongo_config);              // 域名报告
//DatasDomains::dodata($rtime, $mongo_config);		// 域名报告
DatasSourcesearch::instance()->do_data($rtime, $mongo_config); //搜索引擎
DatasSourcedirect::instance()->do_data($rtime, $mongo_config);//直接输入
DatasPagesin::dodata($rtime, $mongo_config);			// 页面流量报告
DatasSourcelink::instance()->do_data($rtime, $mongo_config, 'recomand_domain'); //外部链接url
DatasSourcelink::instance()->do_data($rtime, $mongo_config, 'recomand_url'); //外部链接domain
DatasPagesa::dodata($rtime, $config['_mongos']['dns_dcclog_10']);			// 全网页面报告
//DatasAutovalue::instance()->do_data($rtime, $mongo_config);//自定义变量
//DatasPagesa::dodata($rtime, $mongo_config);			// 页面报告
//全网数据单独处理结束
//unlock
$url = JOBKEEPER_PUT_URL."&job={$flag}&lock=0&date=".(strtotime($doDay))."";
err_log("{$doDay}|jobkeeper|unlock|{$url}", $flag);
file($url);

err_log("{$doDay}|domongo|end", $flag);

