<?php
// +----------------------------------------------------------------------
// | Copyright (c) 2015 All rights reserved.
// +----------------------------------------------------------------------
// | *************
// +----------------------------------------------------------------------


/**
 * 验证时间格式
 * @param  string $time   时间
 * @param  string $format 时间格式
 * @return [type]         [description]
 */
function check_time($time = '', $format = 'Ymd')
{
    $unixTime = strtotime($time);
    if (!$unixTime) {
        return false;
    }
    return date($format, $unixTime) == $time;
}

/**
 * 将所有字符转换为UTF-8
 * @param  [type] $str [description]
 * @return [type]      [description]
 */
function do_string_type($str = '')
{
    if (!$str) return '';
    $_type = detect_encoding($str);
    return $_type != 'UTF-8' ? mb_convert_encoding($str, 'UTF-8', $_type) : $str;
}

/**
 * 获取字符串编码
 * @param  [type] $str [description]
 * @return [type]      [description]
 */
function detect_encoding($str) {
    foreach (['UTF-8', 'GB2312', 'GBK', 'BIG5', 'ASCII'] as $v) {
        if ($str === iconv($v, $v . '//IGNORE', $str)) {
            return $v;
        } else {
            return 'Unicode';
        }
    }
}

/**
 * 记录日志，主要用于记录各项指标
 * @param  string $dataStr 记录的内容
 * @return [type]          [description]
 */
function err_log($dataStr = '', $flag = '')
{
    if(!$flag) $flag = 'log';
    $filename = LOCAL_DIR . 'alog/'.date("Ymd").'.' . $flag;
    @file_put_contents($filename, date("Y-m-d H:i:s") . "|{$dataStr}\r\n", FILE_APPEND);
}





// =========================== 读取mysql数据库配置信息 =================

/**
 * 获取当前日期已开通报告的网站ID
 * @return [type] [description]
 */
function get_report_w_ids($time = '')
{
    $db = db::getInstance($_ENV['_db_configs']['_mysqls']['dns_autotracking_data']);
    $report_w_ids = $db->getCol("SELECT `cids` from `auto_report_yc_container` WHERE `time` = '" . date('Y-m-d', strtotime($time)) . "'");
    if (!$report_w_ids || $report_w_ids[0] == '') {
        err_log("websites no to data: {$time}");
        exit('暂无开通报告的网站');
    } else {
        $report_w_ids = explode(',', $report_w_ids[0]);
    }
    return $report_w_ids ? $report_w_ids : [];
}

/**
 * 【站点】获取当前日期需要跑数据的站点数据
 * @param  string $time [description]
 * @return [type]       [description]
 */
function get_report_d_ids($time = '')
{
    $db = db::getInstance($_ENV['_db_configs']['_mysqls']['dns_autotracking_data']);
    $ids = $db->getAll("SELECT * FROM `auto_report_yc_website` WHERE `time` = '" . date('Y-m-d', strtotime($time)) . "'");
    if (!$ids || empty($ids)) {
        err_log("sites no to data: {$time}");
        return false;
    } else {
        return $ids;
    }
}

/**
 * 【内容组】获取当前日期需要跑数据的URL数据
 * @param  string $time [description]
 * @return [type]       [description]
 */
function get_report_u_ids($time = '', $web_id, $limit)
{
    $db = db::getInstance($_ENV['_db_configs']['_mysqls']['dns_autotracking_data']);
    $time = date('Y-m-d', strtotime($time));
    $count = $db->getOne("SELECT count(*) as total FROM `auto_report_yc_cgroup` WHERE `web_id` = {$web_id} AND `time` = '{$time}'");
    //五个进程跑内容组
    $num = ceil($count['total']/5);
    $from = $num * $limit;
    $ids = $db->getAll("SELECT * FROM `auto_report_yc_cgroup` WHERE `web_id` = {$web_id} AND `time` = '{$time}' ORDER by id limit {$from}, {$num} ");
//    var_dump($ids);
    if (!$ids || empty($ids)) {
        err_log("cgroup no to data: {$time}");
        return false;
    }
    //按groupid拆分
    return $ids;

}

/**
 * ip地址转换成省份ID
 * @param  integer $ip [description]
 * @return [type]      [description]
 */
function ip2provinceid($ip = 0)
{
    $db = db::getInstance($_ENV['_db_configs']['_mysqls']['dns_autotracking']);
    $_provinceid = -1;
    $_col12 = $db->getCol("SELECT `cityid` FROM `auto_yc_ip_list` WHERE `start_ip` <= {$ip} AND `end_ip` >= {$ip} limit 1");
    if (isset($_col12[0]) && !is_null($_col12[0])) {
        $_provinceid = substr($_col12[0], 0, 2) . '0000';
        $_provinceid = in_array($_provinceid, $_ENV['_provinceid_list']) ? $_provinceid : -1; // 确认省份ID存在
    }
    return  $_provinceid + 0;
}
function ip2provinceid2($ip = 0)
{
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
    global $ipclass;
    $a = $ipclass->find($ip);
    //echo "{$ip} {$a[0]} {$a[1]} {$a[2]} {$a[3]} \n";
    $_provinceid = $arrProvince[$a[1]];
    if(!$_provinceid) $_provinceid = -1;
    return  $_provinceid + 0;
}

/**
 * 获取域名对应的ID号，没有将入库并返回ID
 * @param  integer $dcac   网站ID
 * @param  string  $domain 域名
 * @return [type]          [description]
 */

function get_domain_id($dcac = 0, $domain = '', $isout = false)
{
    $dcac = (int) $dcac;
    $domain_md5 = md5($domain);

    $id = false;
    $cluster = RedisC::getRedis();
    $_key = "dm".$dcac.$domain_md5;
    $id = $cluster->HGET($_key, 'id');
    if(!$id){
        $db = db::getInstance($_ENV['_db_configs']['_mysqls']['dns_autotracking_t']);
        $table = $isout?'auto_outlink_domain':'auto_domain';
        $ret = $db->getOne("SELECT `id`, `ismoved` FROM `{$table}` WHERE `web_id` = {$dcac} AND `md5` = '{$domain_md5}' and ismoved=0");
        if ($ret && isset($ret['id'])) {
            $id = $ret['id'];
        } else {
            $id = $db->add("INSERT INTO `{$table}` (`md5`, `web_id`, `domain`, `ismoved`, `create_time`) values ('{$domain_md5}', {$dcac}, '{$domain}', 0, CURRENT_TIMESTAMP)");
            !$id && $id = false;
        }

        if($id){
            $t = $cluster->HSET($_key, 'id', $id);
            $t = $cluster->HSET($_key, 'd', $domain);
            $cluster->EXPIRE($_key, 604800);//=86400*7
        }
    }

    return $id ? $id : false;
}

/**
 * 获取域名对应的ID号，没有将入库并返回ID
 * @param  integer $dcac   网站ID
 * @param  string  $domain 域名
 * @param  string  $url    页面URL
 * @param  string  $url_title 页面标题
 * @return [type]          [description]
 */

function get_url_id($dcac = 0, $domain_id = 0, $url = '', $url_title = '', $isout = false)
{
    $cluster = RedisC::getRedis();

    $dcac = (int) $dcac;
    $domain_id = (int) $domain_id;
    $url_md5 = md5($url);
    $id = false;

    $_key = md5($dcac . $domain_id . $url_md5);
//    $url_info = $cluster->get($_key);
    $url_info = '';
    $mysql_config = $isout?$_ENV['_db_configs']['_mysqls']['dns_outurl']:$_ENV['_db_configs']['_mysqls']['dns_url'];
    if ($url_info) {
        $id = $url_info+0;
    } else {
        $db = db::getInstance($mysql_config);
        $sql = "select id from `auto_url_{$dcac}` WHERE d_id = {$domain_id} AND md5 = '{$url_md5}' and `ismoved` =0 ORDER BY `id` ASC limit 1";
        $id = $db->getCol($sql);
        $id = $id?$id[0]:'';
        if(!$id){
            if (!get_magic_quotes_gpc()) {
                $url = addslashes($url);
                $url_title = addslashes($url_title);
            }
            $db = db::getInstance($mysql_config);
            $sql = "INSERT INTO `auto_url_{$dcac}` (`md5`, `d_id`, `url`, `title`, `ismoved`, `create_time`) values ('{$url_md5}', {$domain_id}, '{$url}', '{$url_title}', 0, CURRENT_TIMESTAMP)";
            $id = $db->add($sql);
        }
        $id = $id + 0;
//        $t=$cluster->setex($_key, 86400, $id);
    }
    return $id ? $id : false;
}

// /**
//  * 将所有url数据导入到redis
//  * @return [type] [description]
//  */
// function url_2_redis()
// {
//     $db = db::getInstance($_ENV['_db_configs']['_mysqls']['dns_autotracking']);
//     $mod = $db->query("SELECT `id`, `md5`, `web_id`, `d_id`, `ismoved` FROM `auto_url`");
//     while ($v = $mod->fetch(PDO::FETCH_ASSOC)) {
//         RedisC::set(md5($v['web_id'] . $v['d_id'] . $v['md5']), json_encode(['id' => $v['id'] + 0, 'ismoved' => $v['ismoved'] + 0]));
//     }
// }

// /**
//  * 处理url中domain_id为空的记录
//  * @return [type] [description]
//  */
// function do_url_domainid()
// {
//     $db = db::getInstance($_ENV['_db_configs']['_mysqls']['dns_autotracking']);
//     $mod = $db->query("SELECT `id`, `web_id`, `d_id`, `url`, `title` FROM `auto_url`");
//     while ($v = $mod->fetch(PDO::FETCH_ASSOC)) {
//         if ($v['d_id'] == 0 && is_url($v['url'])) {
//             $v['url'] = 'http://' . $v['url'];
//             $_parse_url = parse_url($v['url']);
//             $d_id = get_domain_id($v['web_id'], $_parse_url['host']);
//             if ($d_id) {
//                 $db->query("UPDATE `auto_url` SET  `d_id` = {$d_id} WHERE `id` = {$v['id']}");
//             }
//         }
//     }
//     return 111;
// }

// /**
//  * 验证URL
//  * @return [type] [description]
//  */
// function is_url($url = '')
// {
//     $rule = "/^(?:[A-za-z0-9-]+\.)+[A-za-z]{2,4}(:\d+)?(?:[\/\?#][\/=\?%\-&~`@[\]\':+!\.#\w]*)?$/";
//     return preg_match($rule, $url)===1;
// }



/**
 * 获取所有线索代码的类型【线索或支付】
 * @return [type] [description]
 */
function get_leads_type()
{
    $db = db::getInstance($_ENV['_db_configs']['_mysqls']['dns_tagmanager']);
    $codeTypes = $db->getAll("SELECT `id`, `codetype` FROM `auto_events` WHERE `ismoved` = '0'");
    $code_types = [];
    foreach ($codeTypes as $_c) {
        $code_types[$_c['id']] = $_c['codetype'];
    }
    return $code_types;
}


function microtime_float()
{
    list($usec, $sec) = explode(" ", microtime());
    return ((float)$usec + (float)$sec);
}

/**
 * 二维数组按照指定字段排序
 * @param $list
 * @param $field
 * @param string $sortby
 * @return array|bool
 * @author fengqiang@yiche.com
 */
function list_sort_by($list, $field, $sortby='asc') {
    if(is_array($list)){
        $refer = $resultSet = array();
        foreach ($list as $i => $data)
            $refer[$i] = &$data[$field];
        switch ($sortby) {
            case 'asc': // 正向排序
                asort($refer);
                break;
            case 'desc':// 逆向排序
                arsort($refer);
                break;
            case 'nat': // 自然排序
                natcasesort($refer);
                break;
        }
        foreach ( $refer as $key=> $val)
            $resultSet[][$key] = &$list[$key];
        return $resultSet;
    }
    return false;
}
//获取全部网站id
function get_all_wids()
{
    $config = $_ENV['_db_configs'];
    $db = db::getInstance($config['_mysqls']['dns_autotracking']);
    $sql = "SELECT id FROM `auto_web` WHERE `ismoved` = 0";
    $web_ids = $db->getCol($sql);
    return $web_ids;
}

