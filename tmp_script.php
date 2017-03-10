<?php
// 注册 autoload 函数
//分表脚本


spl_autoload_register(function ($name) {
    $f = '_base/' . strtolower(preg_replace('/[A-Z]/', "/$0", $name)) . '.inc';
    include $f;
});

register_shutdown_function(function () {
    unset($_ENV);
});

//MongoCursor::$timeout = -1;                 // mongo操作链接不超时
header("Content-Type:text/html; charset=utf-8");
ini_set('date.timezone', 'Asia/Shanghai');
ini_set("max_execution_time", 60 * 60 * 24 * 3);  // 程序运行最大时长
ini_set("memory_limit", '28240M');

require '_base/db.php';
require '_base/mongodb.php';
require '_base/redis.php';
require '_base/define.php';
require '_base/function.php';




function create_url_table($web_ids)
{
    $config = $_ENV['_db_configs'];
    $db = db::getInstance($config['_mysqls']['dns_autotracking']);
    foreach($web_ids as $web_id)
    {
        $config = $_ENV['_db_configs'];
        $url_db = db::getInstance($config['_mysqls']['dns_tmp']);
        $create_sql = "
        CREATE TABLE if not EXISTS `auto_url_{$web_id}` (
 `id` int(11) NOT NULL AUTO_INCREMENT,
 `md5` char(32) DEFAULT NULL,
 `d_id` int(11) DEFAULT NULL COMMENT '域名id',
 `url` varchar(255) DEFAULT NULL COMMENT '页面url',
 `title` varchar(50) DEFAULT NULL COMMENT '标题',
 `ismoved` tinyint(3) DEFAULT '0' COMMENT '0为可用，1为不可用',
 `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
 PRIMARY KEY (`id`),
 KEY `md5` (`d_id`,`md5`) USING BTREE
) ENGINE=MyISAM  DEFAULT CHARSET=utf8 ROW_FORMAT=FIXED COMMENT='url配置表'
            ";
        var_dump($web_id);
        $url_db->query($create_sql);
    }



}

//mysqldump导出其余url脚本
function dumpsql($web_ids)
{
    $start = date('Y-m-d H:i:s');
    foreach ($web_ids as $web_id) {
        if (!in_array($web_id, [10, 163])) {
            $left = date('Y-m-d H:i:s');
            err_log("web:{$web_id}||start time:{$left}", 'backsql');
            $shell = "/usr/local/mysql/bin/mysqldump -h172.21.1.56 -uautotracking_test -phTxG3Mmd7bdxphCu  -t auto_tracking auto_url -w 'web_id={$web_id} and id<125806272' >/data/wwwroot/sql/auto_url_{$web_id}.sql";
            exec($shell);
            $right = date('Y-m-d H:i:s');
            err_log("web:{$web_id}||end time:{$right}", 'backsql');
            err_log("----------------------------------------", 'backsql');
        } else {
            continue;
        }
    }
    $end = date('Y-m-d H:i:s');
    err_log("----------------------------------------", 'backsql');
    err_log("all start :{$start} || all end :{$end}", 'backsql');
}

/**
 * url表增删web_id字段 方便导入sql
 * @param $web_ids
 * @author fengqiang@yiche.com
 */
function add_field_wid($web_ids, $in = true)
{
    $config = $_ENV['_db_configs'];
    $url_db = db::getInstance($config['_mysqls']['dns_tmp']);
    foreach ($web_ids as $web_id) {

            if ($in) {
                $inport_sql = "delete from  `auto_url_{$web_id}` WHERE id > 125806272";
            } else {
                $inport_sql = "ALTER TABLE `auto_url_{$web_id}` DROP `web_id`;";
            }
            $url_db->query($inport_sql);
            var_dump($inport_sql);

    }

}

function dumpcsv($web_ids)
{
    $config = $_ENV['_db_configs'];
    $db = db::getInstance($config['_mysqls']['dns_autotracking']);
    $start = date('Y-m-d H:i:s');
    foreach ($web_ids as $web_id) {
        if (!in_array($web_id, [10, 163])) {
            $left = date('Y-m-d H:i:s');
            err_log("web:{$web_id}||start time:{$left}", 'backsql');
            $sql = "select `id`,`md5`,`d_id`,`url`,`title`,`ismoved`,`create_time` from `auto_url` WHERE id>125806272 AND id < 139799677 and web_id = {$web_id}";
            $result = $db->getAll($sql);
            foreach($result as $value)
            {
                $ret=$value['id'].",".
                    $value['md5'].",".
                    $value['d_id'].",".
                    $value['url'].",".
                    $value['title'].",".
                    $value['ismoved'].",".
                    $value['create_time'];
                $filename='/data/url_Sql/auto_url_'.$web_id.'.csv';
                file_put_contents($filename, $ret . "\r\n", FILE_APPEND);
            }
            $right = date('Y-m-d H:i:s');
            err_log("web:{$web_id}||end time:{$right}", 'backsql');
            err_log("----------------------------------------", 'backsql');
        } else {
            continue;
        }
    }
    $end = date('Y-m-d H:i:s');
    err_log("----------------------------------------", 'backsql');
    err_log("all start :{$start} || all end :{$end}", 'backsql');

}

function importcsv($web_ids)
{
    $config = $_ENV['_db_configs'];
    $db = db::getInstance($config['_mysqls']['dns_tmp']);
    $start = date('Y-m-d H:i:s');
    foreach ($web_ids as $web_id) {
        if (!in_array($web_id, [10, 163,105]) && file_exists("/data/url_Sql/auto_url_{$web_id}.csv")) {
            if($web_id<106)
            {
               continue;
            };
            $left = date('Y-m-d H:i:s');
            err_log("web:{$web_id}||start time:{$left}", 'backsql');
            $sql = "LOAD DATA INFILE '/data/url_sql/url_Sql/auto_url_{$web_id}.csv' INTO TABLE `auto_url_{$web_id}` FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\n';";
            $db->query($sql);
            $right = date('Y-m-d H:i:s');
            err_log("web:{$web_id}||end time:{$right}", 'backsql');
            err_log("----------------------------------------", 'backsql');
        } else {
            continue;
        }
    }
    $end = date('Y-m-d H:i:s');
    err_log("----------------------------------------", 'backsql');
    err_log("all start :{$start} || all end :{$end}", 'backsql');
}


function create_page_domain($web_ids)
{

//    create_url('auto_url_'.$w_id);
//    create_outurl('auto_url_'.$w_id);
//    //创建入口页面
//    create_page_in('auto_page_'.$w_id.'_0');
//    //创建外部链接
//    create_out_domain('auto_domain_lead_'.$w_id.'_0','DB_CREATE_OUT_DOMAIN_LEAD');
//    create_out_domain('auto_domain_look_'.$w_id.'_0','DB_CREATE_OUT_DOMAIN_LOOK');
//
//    create_out_url('auto_url_look_'.$w_id.'_0','DB_CREATE_OUT_URL_LOOK');
//    create_out_url('auto_url_lead_'.$w_id.'_0','DB_CREATE_OUT_URL_LEAD');

    $config = $_ENV['_db_configs'];
    $db_page = db::getInstance($config['_mysqls']['dns_autotracking_data_page']);
    $db_domain = db::getInstance($config['_mysqls']['dns_autotracking_data_domain']);
    foreach($web_ids as $web_id)
    {
        $province = $_ENV['_provinceid_list'];
        foreach ($province as $p){

            $sql = create_page('auto_page_'.$web_id.'_'.$p);
            $db_page->query($sql);
            $sql = create_domain('auto_domain_'.$web_id.'_'.$p);
            $db_domain->query($sql);

        }
//        var_dump($web_id);
    }


}
/**
 * 外部链接域名--浏览数据表
 */
function create_out_domain($t_name,$db)
{
    $sql = "

	CREATE TABLE IF NOT EXISTS `$t_name` (
	`time` date NOT NULL COMMENT '日期',
	`type` tinyint(3) NOT NULL DEFAULT '1' COMMENT '1为老访客，2为新访客',
	`d_id` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '域名id',
	`pv_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '浏览量PV',
	`uv_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '独立访客',
	`uv_new` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '新访客',
	`ip_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '独立访问IP',
	`visit_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '总访次',
	`visit_more` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '多次访问人数',
	`time_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '浏览总时长',
	`twojump_total` int(11) NOT NULL DEFAULT '0' COMMENT '二跳量',
	`create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	KEY `d_id` (`d_id`),
  	KEY `d_time` (`d_id`,`time`),
  	KEY `time` (`time`)
	) ENGINE=MyISAM DEFAULT CHARSET=utf8 ROW_FORMAT=FIXED COMMENT='外部链接域名报告'
	;";

    return  M('','',$db)->execute($sql);


}

/**
 * 外部链接url--数据数据表
 */
function create_out_url($t_name,$db)
{
    $sql = "

	CREATE TABLE IF NOT EXISTS `$t_name` (
	`time` date NOT NULL COMMENT '日期',
	`type` tinyint(3) NOT NULL DEFAULT '1' COMMENT '1为老访客，2为新访客',
	`url_id` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '页面url的id',
	`pv_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '浏览量PV',
	`uv_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '独立访客',
	`uv_new` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '新访客',
	`ip_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '独立访问IP',
	`visit_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '总访次',
	`visit_more` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '多次访问人数',
	`time_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '浏览总时长',
	`twojump_total` int(11) NOT NULL DEFAULT '0' COMMENT '二跳量',
	`create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	KEY `time` (`time`),
  	KEY `url_id` (`url_id`),
  	KEY `w_u_time` (`url_id`,`time`)
	) ENGINE=MyISAM DEFAULT CHARSET=utf8 ROW_FORMAT=FIXED COMMENT='外部链接页面报告'
	;";

    return  M('','',$db)->execute($sql);


}
/**
 * 创建page_in表
 */
function create_page_in($t_name)
{
    $sql = "
	CREATE TABLE IF NOT EXISTS `$t_name` (
	`time` date NOT NULL COMMENT '日期',
	`url_id` int(11) NOT NULL DEFAULT '0' COMMENT '页面url的id',
	`pv_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '浏览量PV',
	`uv_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '独立访客',
	`uv_new` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '新访客',
	`ip_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '独立访问IP',
	`visit_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '总访次',
	`visit_more` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '多次访问人数',
	`time_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '浏览总时长',
	`twojump_total` int(11) NOT NULL DEFAULT '0' COMMENT '二跳量',
	`create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	KEY `time` (`time`),
	KEY `url_id` (`url_id`),
	KEY `t_url` (`url_id`,`time`)
	) ENGINE=MyISAM DEFAULT CHARSET=utf8 ROW_FORMAT=FIXED COMMENT='入口页面报告'
	;";

    return  $sql;

}
/**
 * 创建page表
 */
function create_page($t_name)
{
    $sql = "
		
		CREATE TABLE IF NOT EXISTS `$t_name` (
		  `time` date NOT NULL COMMENT '日期',
		  `url_id` int(11) NOT NULL DEFAULT '0' COMMENT '页面url的id',
		  `pv_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '浏览量PV',
		  `uv_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '独立访客',
		  `uv_new` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '新访客',
		  `ip_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '独立访问IP',
		  `visit_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '总访次',
		  `visit_more` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '多次访问人数',
		  `time_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '浏览总时长',
		  `twojump_total` int(11) NOT NULL DEFAULT '0' COMMENT '二跳量',
		  `quit_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '退出量',
		  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		  KEY `time` (`time`),
		  KEY `url_id` (`url_id`),
		  KEY `t_url` (`url_id`,`time`)
		) ENGINE=MyISAM DEFAULT CHARSET=utf8 ROW_FORMAT=FIXED COMMENT='受访页面报告'
		;";

    return  $sql;

}
/**
 * 创建domain表
 */
function create_domain($t_name)
{

    $sql = "
		
		CREATE TABLE IF NOT EXISTS `$t_name` (
		  `time` date NOT NULL DEFAULT '0000-00-00' COMMENT '日期',
		  `d_id` int(11) NOT NULL DEFAULT '0' COMMENT '域名id',
		  `pv_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '浏览量PV',
		  `uv_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '独立访客',
		  `uv_new` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '新访客',
		  `ip_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '独立访问IP',
		  `visit_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '总访次',
		  `visit_more` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '多次访问人数',
		  `time_total` float unsigned NOT NULL DEFAULT '0' COMMENT '浏览总时长',
		  `twojump_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '二跳量',
		  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		  KEY `time` (`time`),
		  KEY `p_d_id` (`d_id`,`time`),
		 KEY `d_id` (`d_id`)
		) ENGINE=MyISAM DEFAULT CHARSET=utf8 ROW_FORMAT=FIXED COMMENT='网站整体流量报告【明细】';
		";

    return $sql;


}
/**
 * 创建url表(页面报告，入口页面报告)
 */
function create_url($t_name)
{

    $sql = "

	CREATE TABLE IF NOT EXISTS `$t_name` (

	  `id` int(11) NOT NULL AUTO_INCREMENT,
	  `md5` char(32) DEFAULT NULL,
	  `d_id` int(11) DEFAULT NULL COMMENT '域名id',
	  `url` varchar(255) DEFAULT NULL COMMENT '页面url',
	  `title` varchar(50) DEFAULT NULL COMMENT '标题',
	  `ismoved` tinyint(3) DEFAULT '0' COMMENT '0为可用，1为不可用',
	  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	  PRIMARY KEY (`id`),
	  KEY `md5` (`d_id`,`md5`) USING BTREE
	) ENGINE=MyISAM DEFAULT CHARSET=utf8 ROW_FORMAT=FIXED COMMENT='url配置表';
	";

    return M('','','DB_CREATE_URL')->execute($sql);
}

/**
 * 外部链接url表
 */
function create_outurl($t_name)
{

    $sql = "

	CREATE TABLE IF NOT EXISTS `$t_name` (

	`id` int(11) NOT NULL AUTO_INCREMENT,
	`md5` char(32) DEFAULT NULL,
	`d_id` int(11) DEFAULT NULL COMMENT '域名id',
	`url` varchar(255) DEFAULT NULL COMMENT '页面url',
	`title` varchar(50) DEFAULT NULL COMMENT '标题',
	`ismoved` tinyint(3) DEFAULT '0' COMMENT '0为可用，1为不可用',
	`create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	PRIMARY KEY (`id`),
	KEY `md5` (`d_id`,`md5`) USING BTREE
	) ENGINE=MyISAM DEFAULT CHARSET=utf8 ROW_FORMAT=FIXED COMMENT='url配置表';
	";

    return M('','','DB_CREATE_OUT_URL')->execute($sql);
}

// 分表脚本
$config = $_ENV['_db_configs'];
$db = db::getInstance($config['_mysqls']['dns_autotracking']);
$get_webid_sql = "select id from `auto_web` ORDER by id";
$web_ids = $db->getCol($get_webid_sql);

//create_page_domain($web_ids);
function create_table($web_ids, $table)
{
    $config = $_ENV['_db_configs'];
    foreach($web_ids as $web_id)
    {
        switch ($table){
            case 'pagein':
                $table_name = "auto_page_{$web_id}_0";
                $db = db::getInstance($config['_mysqls']['dns_autotracking_data_page_in']);
                $sql = "
	CREATE TABLE IF NOT EXISTS `{$table_name}` (
	`time` date NOT NULL COMMENT '日期',
	`url_id` int(11) NOT NULL DEFAULT '0' COMMENT '页面url的id',
	`pv_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '浏览量PV',
	`uv_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '独立访客',
	`uv_new` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '新访客',
	`ip_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '独立访问IP',
	`visit_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '总访次',
	`visit_more` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '多次访问人数',
	`time_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '浏览总时长',
	`twojump_total` int(11) NOT NULL DEFAULT '0' COMMENT '二跳量',
	`create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	KEY `time` (`time`),
	KEY `url_id` (`url_id`),
	KEY `t_url` (`url_id`,`time`)
	) ENGINE=MyISAM DEFAULT CHARSET=utf8 ROW_FORMAT=FIXED COMMENT='入口页面报告'
	;";
                $db->query($sql);
                break;
            case 'outurl_look':
                $db = db::getInstance($config['_mysqls']['dns_autotracking_data_outurl']);
                $sql = "
                CREATE TABLE IF NOT EXISTS `auto_url_look_{$web_id}_0` (
 `time` date NOT NULL COMMENT '日期',
 `type` tinyint(3) NOT NULL DEFAULT '1' COMMENT '1为老访客，2为新访客',
 `url_id` int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'url_id',
 `pv_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '浏览量PV',
 `uv_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '独立访客',
 `uv_new` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '新访客',
 `ip_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '独立访问IP',
 `visit_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '总访次',
 `visit_more` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '多次访问人数',
 `time_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '浏览总时长',
 `twojump_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '二跳量',
 `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
 KEY `time` (`time`),
 KEY `url_id` (`url_id`),
 KEY `w_u_time` (`url_id`,`time`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 ROW_FORMAT=FIXED COMMENT='外部链接页面《浏览数据》报告'
                ";
                $db->query($sql);
                break;
            case 'outurl_lead':
                $db = db::getInstance($config['_mysqls']['dns_autotracking_lead_outurl']);
                $sql = "
                CREATE TABLE IF NOT EXISTS `auto_url_lead_{$web_id}_0` (
 `time` date NOT NULL COMMENT '日期',
 `type` tinyint(3) NOT NULL DEFAULT '1' COMMENT '1为老访客，2为新访客',
 `url_id` int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'url_id',
 `pv_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '浏览量PV',
 `uv_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '独立访客',
 `uv_new` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '新访客',
 `ip_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '独立访问IP',
 `visit_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '总访次',
 `visit_more` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '多次访问人数',
 `time_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '浏览总时长',
 `twojump_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '二跳量',
 `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
 KEY `time` (`time`),
 KEY `url_id` (`url_id`),
 KEY `w_u_time` (`url_id`,`time`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 ROW_FORMAT=FIXED COMMENT='外部链接页面《线索数据》报告';
                ";
                $db->query($sql);
                break;
            case 'outdomain_look':
                $db = db::getInstance($config['_mysqls']['dns_autotracking_lead_outdomain']);
                $sql = "
                CREATE TABLE IF NOT EXISTS `auto_domain_lead_{$web_id}_0` (
 `time` date NOT NULL COMMENT '日期',
 `type` tinyint(3) NOT NULL DEFAULT '1' COMMENT '1为老访客，2为新访客',
 `d_id` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '域名id',
 `pv_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '浏览量PV',
 `uv_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '独立访客',
 `uv_new` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '新访客',
 `ip_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '独立访问IP',
 `visit_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '总访次',
 `visit_more` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '多次访问人数',
 `time_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '浏览总时长',
 `twojump_total` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '二跳量',
 `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
 KEY `d_id` (`d_id`),
 KEY `d_time` (`d_id`,`time`),
 KEY `time` (`time`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 ROW_FORMAT=FIXED COMMENT='外部链接域名《线索数据》报告'
                ";
                $db->query($sql);
                break;
        }
        var_dump($web_id);
    }
}
//create_table($web_ids, 'outdomain_look');

//add_field_wid($web_ids);
//create_url_table($web_ids);
//importcsv($web_ids);
//add_field_wid($web_ids);
//putincsv($web_ids);
//importcsv($web_ids);

function dofile($file){
    $arrProvince['北京'] = '110000';
    $arrProvince['天津'] = '120000';
    $arrProvince['河北'] = '130000';
    $arrProvince['山西'] = '140000';
    $arrProvince['内蒙古'] = '150000';
    $arrProvince['辽宁'] = '210000';
    $arrProvince['吉林'] = '220000';
    $arrProvince['黑龙江'] = '230000';
    $arrProvince['上海'] = '310000';
    $arrProvince['江苏'] = '320000';
    $arrProvince['浙江'] = '330000';
    $arrProvince['安徽'] = '340000';
    $arrProvince['福建'] = '350000';
    $arrProvince['江西'] = '360000';
    $arrProvince['山东'] = '370000';
    $arrProvince['河南'] = '410000';
    $arrProvince['湖北'] = '420000';
    $arrProvince['湖南'] = '430000';
    $arrProvince['广东'] = '440000';
    $arrProvince['广西'] = '450000';
    $arrProvince['海南'] = '460000';
    $arrProvince['重庆'] = '500000';
    $arrProvince['四川'] = '510000';
    $arrProvince['贵州'] = '520000';
    $arrProvince['云南'] = '530000';
    $arrProvince['西藏'] = '540000';
    $arrProvince['陕西'] = '610000';
    $arrProvince['甘肃'] = '620000';
    $arrProvince['青海'] = '630000';
    $arrProvince['宁夏'] = '640000';
    $arrProvince['新疆'] = '650000';
    $arrProvince['台湾'] = '710000';
    $arrProvince['香港'] = '810000';
    $arrProvince['澳门'] = '820000';
    $arrProvince = array_flip($arrProvince);
    $fp = fopen($file,"r");
    while(!feof($fp))
    {
        //fgets() Read row by row
        $str = fgets($fp);
        $str = explode(',', $str);
        if($arrProvince[trim($str[2],"\n")])
        {
            $str[2] = trim($str[2],"\n");
            $pro = $arrProvince[$str[2]];
            if(!$pro)
            {
                continue;
            }
            $str[3]= $pro;
            var_dump($str);
            err_log(join(',', $str), $file);
        }


    }
    fclose($fp);

}









