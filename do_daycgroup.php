<?php
// +----------------------------------------------------------------------
// | Copyright (c) 2016 All rights reserved.
// +----------------------------------------------------------------------
// | Author: Jerry.fang <fangzq@yiche.com>
// +----------------------------------------------------------------------

// 注册 autoload 函数
spl_autoload_register(function ($name) {
    $f = '_base/' . strtolower(preg_replace('/[A-Z]/', "/$0", $name)) . '.inc';
    include $f;
});

register_shutdown_function(function () {
    unset($_ENV);
});

MongoCursor::$timeout = -1;					// mongo操作链接不超时
header("Content-Type:text/html; charset=utf-8");
ini_set('date.timezone', 'Asia/Shanghai');
ini_set("max_execution_time", 60*60*24*3);	// 程序运行最大时长
ini_set("memory_limit", '28240M');

require '_base/db.php';
require '_base/mongodb.php';
require '_base/mongo.php';
require '_base/redis.php';
require '_base/define.php';
require '_base/function.php';

#jobkeeper start
$flag = 'ATcgroupPSDay_test';
define("atFLAG", $flag);
error_reporting(E_ALL);


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
openssl_get_privatekey($doDay);

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


/*****************多进程处理内容组***************************/
// 获取所有内容组
$web_ids = [143, 163];
foreach ($web_ids as $web_id) {
    //拆分数组
    for($i = 0; $i<5; $i++)
    {
        $str = "{$rtime}||{$web_id}||{$i}";
        $process = new swoole_process('worker_group');
        $pid = $process->start();
        $process->write($str); //进程通信
    }
}
err_log("cgroup_data success: {$rtime}");

//回收进程
while(1){
    $ret = swoole_process::wait();
    if ($ret){// $ret 是个数组 code是进程退出状态码，
        $pid = $ret['pid'];
        err_log("进程{$ret['pid']}回收成功", 'fff');
    }else{
        err_log("mongodb入mysql{$rtime}处理不完整", 'fff');
        break;
    }
}

function worker_group(swoole_process $worker){
    $str = $worker->read();
    $str = explode('||', $str);

    $group_ids = get_report_u_ids($str[0], $str[1],$str[2]);
    if (!$group_ids || empty($group_ids)) {
        err_log("cgroup_data success(no data): {$str[0]}");
    } else {
        foreach ($group_ids as $v) {
            $domain_ids = explode(',', $v['domain_ids']);
            $url_ids = explode(',', $v['url_ids']);
            DatasCgroup::_data_handle($str[0], $v['id'], $v['web_id'], $v['wb_id'], $domain_ids, $url_ids);
        }
        $jsonworker = json_encode((array)$worker);
        err_log("callback_file worker info {$jsonworker}", atFLAG);
    }
}
/*****************多进程处理内容组***************************/


//页面报告
$config = $_ENV['_db_configs'];
$mongo_config = $config['_mongos']['dns_dcclog_other'];


DatasPagesa::dodata($rtime, $config['_mongos']['dns_dcclog_other']);			// 其它页面报告
DatasPagesa::dodata($rtime, $config['_mongos']['dns_dcclog_163']);			// 163页面报告
DatasPagesin::dodata($rtime, $config['_mongos']['dns_dcclog_10']);			// 全网页面报告


//unlock
$url = JOBKEEPER_PUT_URL."&job={$flag}&lock=0&date=".(strtotime($doDay))."";
err_log("{$doDay}|jobkeeper|unlock|{$url}", $flag);
file($url);

err_log("{$doDay}|domongo|end", $flag);

