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
$url = db::getInstance($_config['_dbs']['dns_url']);
$db = db::getInstance($_config['_dbs']['dns_autotracking_data']);

// 每天执行昨天的记录
$date = date('Y-m-d', strtotime('-1 day'));
$cids = '';
$ret = $source->getAll("SELECT `g`.`id`, `g`.`rule`, `g`.`web_id`, `g`.`wb_id`, `w`.`domain_id` from `auto_content_group` `g` JOIN `auto_website` `w` ON `g`.`wb_id` = `w`.`id` WHERE `g`.`status` = 1 and `g`.`ismoved` = 0");
//var_dump($ret);
if (!empty($ret)) {
	$_data = [];
	foreach ($ret as $_v) {
		$_cids = _do_rule($_v['web_id'], $_v['domain_id'], unserialize($_v['rule']));	// 根据规则，计算出所有符合规则的URL ID
		$cids  = '';
		$_cids && $cids = implode(',', $_cids);	// 转换为逗号分隔的字符串存储
		if(empty($cids)) continue;
		$_data[] = "('{$date}', {$_v['id']}, {$_v['web_id']}, {$_v['wb_id']}, '{$_v['domain_id']}', '{$cids}')";
		$_content[] = "({$_v['id']}, {$cids},{$_v['web_id']}, {$_v['wb_id']})";
		
	}

    if ($_data) {
    	$db->query("REPLACE INTO `auto_report_yc_cgroup` VALUES " . implode(',', $_data));
    }
	

	//把新规则添加到数据表中
	if ($_content) {
		$source->query("REPLACE INTO `auto_content_group_url` VALUES " . implode(',', $_content));
	}
	

}

/**
 * 通过内容组规则，处理出所有对应的页面ID
 * @param  array  $rule 规则数据
 * @return [type]       [description]
 */
function _do_rule($dcac = 0, $domain_ids = '', $rule = [])
{
	if (!$dcac || !$domain_ids || empty($rule) || !isset($rule['rule1'])) {
		return [];
	}
	$dcac = (int) $dcac;
	$domain_ids = explode(',', $domain_ids);
	$domain_ids = array_map('intval', $domain_ids);
	$domain_ids = implode(',', $domain_ids);

	global $url;
	
	$sql = '';
	// 处理规则，生成对应的SQL
	$count = count($rule['rule1']);
	$type_rule1 = ['title', 'url'];
	$type_rule2 = [0, 1, 2, 3, 4, 5, 6, 7, 8];
	$type_rule4 = [0, 1];
	foreach ($rule['rule1'] as $_k => $_v) {
		if (!in_array($_v, $type_rule1)
			|| (!isset($rule['rule2'][$_k]) || !in_array($rule['rule2'][$_k], $type_rule2))
			|| (!isset($rule['rule3'][$_k]) || !$rule['rule3'][$_k])
			|| (!isset($rule['rule4'][$_k]) || !in_array($rule['rule4'][$_k], $type_rule4))
			) {
			continue;
		}
		$sql .= _get_rtype($_v, $rule['rule2'][$_k], $rule['rule3'][$_k]);
		if ($_k + 1 < $count) {
			$sql .= ($rule['rule4'][$_k] == 0 ? 'AND' : 'OR');
		}
	}

	if ($sql) {
		$sql = "SELECT `id` FROM `auto_url_{$dcac}` WHERE  `d_id` IN ({$domain_ids}) AND " . $sql;
		//echo $sql.'\n';
		$_ret = $url->getCol($sql);	// 查询对应的ID
         //var_dump($_ret);
		return !empty($_ret) ? $_ret : [];
	} else {
		return [];
	}
}

/**
 * 获取8个规则类型
 * @param  integer $type [description]
 * @return [type]        [description]
 */
function _get_rtype($cname = 'url', $type = 0, $value = '')
{
	$_r = '';
	switch ($type) {
		case '0':	// 等于
			$_r = " `{$cname}` = '{$value}' ";
			break;
		case '1':	// 包含
			$_r = " `{$cname}` like '%{$value}%' ";
			break;
		case '2':	// 开头为
			$_r = " `{$cname}` like '{$value}%' ";
			break;
		case '3':	// 结尾为
			$_r = " `{$cname}` like '%{$value}' ";
			break;
		case '4':	// 不等于
			$_r = " `{$cname}` != '{$value}' ";
			break;
		case '5':	// 不包含
			$_r = " `{$cname}` not like '%{$value}%' ";
			break;
		case '6':	// 开始不是
			$_r = " `{$cname}` not like '{$value}%' ";
			break;
		case '7':	// 结尾不是
			$_r = " `{$cname}` not like '%{$value}' ";
			break;
		case '8':	// 与正则表达式匹配
				$_r = " `{$cname}` regexp '{$value}' ";
				break;
	}
	return $_r;
}


