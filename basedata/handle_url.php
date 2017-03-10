<?php
// +----------------------------------------------------------------------
// | Copyright (c) 2016 All rights reserved.
// +----------------------------------------------------------------------
// | *************
// +----------------------------------------------------------------------

// 注册 autoload 函数
spl_autoload_register(function ($name) {
    $f = '_base' . strtolower(preg_replace('/[A-Z]/', "/$0", $name)) . '.inc';
    include $f;
});

register_shutdown_function(function () {
    unset($_ENV);
});

header("Content-Type:text/html; charset=utf-8");
ini_set('date.timezone', 'Asia/Shanghai');
ini_set("max_execution_time", 60*60*24*3);	// 程序运行最大时长
ini_set("memory_limit", '20240M');

require '_base/db.php';
require '_base/define.php';

$start_time = time();

$db = db::getInstance($_ENV['_db_configs']['_mysqls']['dns_autotracking']);
$ret = $db->query("SELECT id, title FROM `auto_url`");
while ($v = $ret->fetch(PDO::FETCH_ASSOC)) {
	if (is_url($v['title'])) {
		$title = get_title($v['title']);
		if (!$title || !is_string($title)) {
			continue;
		}
		$db->query("update `auto_url` set `title` = '{$title}' where `id` = " . $v['id']);
	}
}

echo $start_time . '||' . time() . "\n";

function is_url($url = '')
{
    $rule = "/^(?:[A-za-z0-9-]+\.)+[A-za-z]{2,4}(:\d+)?(?:[\/\?#][\/=\?%\-&~`@[\]\':+!\.#\w]*)?$/";
    return preg_match($rule, $url) === 1;
}

function get_title($url = '')
{
	$content=file_get_contents($url);
	$pos = strpos($content,'utf-8');
	if($pos===false) {$content = iconv("gbk", "utf-8", $content);}
	$postb = strpos($content,'<title>')+7;
	$poste = strpos($content,'</title>');
	$length = $poste-$postb;
	$title = substr($content,$postb,$length);
	$title = trim($title, "
	    ");
	return $title;
}

