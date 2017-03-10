<?php
// +----------------------------------------------------------------------
// | Copyright (c) 2015 All rights reserved.
// +----------------------------------------------------------------------
// | Author: Jerry.fang <fangzq@yiche.com>
// +----------------------------------------------------------------------

header("Content-Type:text/html; charset=utf-8");
ini_set('date.timezone', 'Asia/Shanghai');

require_once __DIR__ . '/../_base/define.php';
require_once __DIR__ . '/../_base/db.php';

$source = db::getInstance($_config['_dbs']['dns_autotracking']);
$db = db::getInstance($_config['_dbs']['dns_autotracking_data']);

// 每天执行
$date = date('Y-m-d');
$cids = '';
$ret = $source->getCol("SELECT `id` from `auto_web` WHERE `status` = '1' and ismoved = '0'");
if (!empty($ret)) {
	$cids = implode(',', $ret);
	$db->query("REPLACE INTO `auto_report_yc_container` VALUES ('{$date}', '{$cids}')");
}


