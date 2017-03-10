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
ini_set("memory_limit", '20240M');

require '_base/db.php';
require '_base/mongodb.php';
require '_base/redis.php';
require '_base/define.php';
require '_base/function.php';

$flag = 'ATmongoImport_test_hour';
define("atFLAG", $flag);

//error_reporting(E_ALL);
#jobkeeper start
$url = JOBKEEPER_GET_URL."&job={$flag}";
$arrt =  file($url);
if(!$arrt[0]){
	echo "jobkeeper|get error|{$url}\n";
	err_log("jobkeeper|get error|{$url}");
	exit();
}
$arrF = json_decode($arrt[0]);

//判断当前任务是否在运行中，避免启动多个进程
if($arrF->data->lock){
    echo "jobkeeper|lock:1|datetime:{$arrF->data->datetime}|exit\n";
    exit();
}

//err_log("jobkeeper|last date:{$arrF->data->date}", atFLAG);
$doHour = date('Y-m-d H:i:s', strtotime($arrF->data->datetime)+60*60);
//判断前置任务是否就绪
$arrt = explode(',', $arrF->data->parent);
foreach ($arrt as $vp){
    $url = JOBKEEPER_URL_GET."&job={$vp}";
    $arrtParent = file($url);
    if(!$arrtParent[0]){
        err_log("jobkeeper|{$doHour}|exit|parent {$vp} jobkeeper get error|{$url}", atFLAG);
        exit();
    }
    $arrFParent = json_decode($arrtParent[0]);
    echo "jobkeeper|parent|{$vp}:{$arrFParent->data->datetime}\n";

    if ($doHour > $arrFParent->data->datetime) {
        echo "{$doHour}|{$vp}:{$arrFParent->data->datetime}|before|no ok\n";
//		err_log("jobkeeper|{$doHour}|exit|{$vp}:{$arrFParent->data->datetime}|before|no ok", atFLAG);
        exit;
    }
}
echo "jobkeeper|{$flag}|{$doHour}\n";



$unixTime = strtotime($doHour);
$rtime = date('Ymd', $unixTime);	// 日期，到天
$time = date('YmdH', $unixTime);	// 日期，到小时
$start_time = time();
err_log("{$time}|mongoimport|start + + + + + + + + + + + + + + + + + + + +", atFLAG);

//lock
$url = JOBKEEPER_PUT_URL."&job={$flag}&lock=1";
err_log("{$time}|jobkeeper|lock|{$url}", atFLAG);
file($url);
#jobkeeper end


/**
 * 4个日志文件
 * dcclog数据量过大 入mongo拆分
 */

FilesClicks::mongoImport($time, 'dcclog', $_ENV['_db_configs']['_mongos']['dns_dcclog_10']);						// 行为日志
FilesClicks::mongoImport($time, 'dcclog', $_ENV['_db_configs']['_mongos']['dns_dcclog_163']);						// 行为日志
FilesClicks::mongoImport($time, 'dcclog', $_ENV['_db_configs']['_mongos']['dns_dcclog_other']);						// 行为日志
FilesClicks::mongoImport($time, 'event', $_ENV['_db_configs']['_mongos']['dns_event']);						// 事件日志

FilesClicks::mongoImport($time, 'leads', $_ENV['_db_configs']['_mongos']['dns_leads']);					// 线索日志
FilesClicks::mongoImport($time, 'click', $_ENV['_db_configs']['_mongos']['dns_click']);					// 点击日志
FilesClicks::mongoImport($time, 'impression', $_ENV['_db_configs']['_mongos']['dns_impression']);					// 点击日志
$dateday = $unixTime-86400;
if($hour == 23)
{
    $dateday = $unixTime - 82800;
}
//unlock
$url = JOBKEEPER_PUT_URL."&job={$flag}&datetime=".strtotime($doHour)."&lock=0&date=".$dateday."";
err_log("{$time}|jobkeeper|unlock|{$url}", atFLAG);
file($url);

err_log("{$time}|mongoimport|end", atFLAG);

