<?php
// 注册 autoload 函数
spl_autoload_register(function ($name) {
    $f = '_base/' . strtolower(preg_replace('/[A-Z]/', "/$0", $name)) . '.inc';
    include $f;
});

register_shutdown_function(function () {
    unset($_ENV);
});

//MongoCursor::$timeout = -1;                                     // mongo操作链接不超时
header("Content-Type:text/html; charset=utf-8");
ini_set('date.timezone', 'Asia/Shanghai');
ini_set("max_execution_time", 60*60*24*3);      // 程序运行最大时长
ini_set("memory_limit", '20240M');

//error_reporting(E_ERROR);
error_reporting(E_ALL);

require '_base/db.php';
require '_base/mongodb.php';
require '_base/redis.php';
require '_base/define.php';
require '_base/function.php';

require '_base/17monipdb/IP.class.php';//2016-08-29 莫立明
$ipclass = new IP();

$flag = 'ATdcclogPSHour_test';
$processFlag = 'dcclog_process';
define("atFLAG", $flag);
define("prFLAG", $processFlag);


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

//err_log("jobkeeper|last datetime:{$arrF->data->datetime}", atFLAG);
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
//lock
$url = JOBKEEPER_PUT_URL."&job={$flag}&lock=1";
err_log("jobkeeper|lock|{$url}", atFLAG);
file($url);
#jobkeeper end

$unixTime = strtotime($doHour);
$rtime = date('Ymd', $unixTime);        // 日期，到天
$time = date('YmdH', $unixTime);        // 日期，到小时
$hour = date('H', $unixTime);           // 日期，只有小时
$start_time = time();
err_log("{$doHour}|dofiles|start + + + + + + + + + + + + + + + + + + + +", atFLAG);


//行为日志单独处理 把文件拆分十个 用十个进程去跑 swoole_process
$dataArr = FilesDcclog::dofiles_hour_new($time);
if(is_array($dataArr))
{
    for($i = 0; $i<10; $i++)
    {
        $i = sprintf("%02d", $i);
        $str = "{$dataArr[0]}.{$i}||{$dataArr[1]}||{$time}||{$doHour}";
        $process = new swoole_process('worker_dcclog');
        $pid = $process->start();
        $process->write($str); //进程通信
    }
}
//回收进程
while(1){
    $ret = swoole_process::wait();
    if ($ret){// $ret 是个数组 code是进程退出状态码，
        $pid = $ret['pid'];
        err_log("进程{$ret['pid']}回收成功", atFLAG);
    }else{
            err_log("进程{$ret['pid']}回收失败", atFLAG);
            break;

    }
}


function worker_dcclog(swoole_process $worker)
{
    $str = $worker->read();
    $params = explode("||", $str);
    $jsonworker = json_encode((array)$worker);
    err_log("callback_file worker info {$jsonworker}", atFLAG);
    if(file_exists($params[0]))
    {
        err_log("start:: log:".$params[0]."||time:".$params[2]."||dohour:".$params[3], prFLAG);
        FilesDcclog::formatData2PutJsonFile($params[0], $params[1], $params[2], $worker->pid);
        err_log("end:: log:".$params[0]."||time:".$params[2]."||dohour:".$params[3], prFLAG);
    }
}

//unlock
$url = JOBKEEPER_PUT_URL."&job={$flag}&datetime=".strtotime($doHour)."&lock=0&date=".(strtotime($doHour)-86400)."";

err_log("jobkeeper|unlock|{$url}", atFLAG);
file($url);
err_log("{$doHour}|dofiles|end", atFLAG);
