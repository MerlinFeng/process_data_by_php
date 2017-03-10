<?php
// +----------------------------------------------------------------------
// | Copyright (c) 2016 All rights reserved.
// +----------------------------------------------------------------------
// | Author: Jerry.fang <fangzq@yiche.com>
// +----------------------------------------------------------------------

header("Content-Type:text/html; charset=utf-8");
ini_set('date.timezone', 'Asia/Shanghai');

require_once __DIR__ . '/../_base/define.php';
require_once __DIR__ . '/../_base/db.php';

$source = db::getInstance($_config['_dbs']['dns_autotracking']);
$db = db::getInstance($_config['_dbs']['dns_autotracking_data']);

// 每天执行昨天的数据
$date = date('Y-m-d', strtotime('-1 day'));
$cids = '';
$ret = $source->getAll("SELECT `id`, `domain_id`, `web_id` from `auto_website` WHERE `status` = 1 and `ismoved` = 0");
//var_dump($ret);

if (!empty($ret)) {
	$_data = [];
	foreach ($ret as $_v) {
		$_data[] = "('{$date}', {$_v['id']}, {$_v['web_id']}, '{$_v['domain_id']}')";
		$_result[] = "({$_v['id']},'{$_v['domain_id']}', {$_v['web_id']})";
	}
	$db->query("REPLACE INTO `auto_report_yc_website` VALUES " . implode(',', $_data));
	$source->query("REPLACE INTO `auto_website_url` VALUES " . implode(',', $_result));
}


