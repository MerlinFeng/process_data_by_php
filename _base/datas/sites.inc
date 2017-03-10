<?php
// +----------------------------------------------------------------------
// | Copyright (c) 2016 All rights reserved.
// +----------------------------------------------------------------------
// | *************
// +----------------------------------------------------------------------

/**
 * 站点报告
 */
class DatasSites
{
    /**
     * 循环执行每个站点
     * @param  string $time [description]
     * @return [type]       [description]
     */
    public static function dodata($time = '', $config_mongo)
    {
        //echo $time.'+++++++++';
        if (!check_time($time)) {
            return false;
        }
        // 获取所有站点
        $site_ids = get_report_d_ids($time);
        if (!$site_ids || empty($site_ids)) {
            err_log("site_data success(no data): {$time}", $config_mongo['dbname']);
        } else {
            foreach ($site_ids as $v) {
                $domain_ids = explode(',', $v['domain_ids']);
                //echo '<pre>';
                //var_dump($v);
                self::_data_handle($time, $v['id'], $v['web_id'], $domain_ids, $config_mongo);
            }
            err_log("site_data success: {$time}", $config_mongo['dbname']);
        }
        return true;
    }

    /**
     * 执行跑数逻辑
     * @param  string  $time       日期
     * @param  integer $siteid     站点ID
     * @param  integer $dcac     	网站ID
     * @param  array   $domain_ids 包含的域名ID集合
     * @return [type]              [description]
     */
    private static function _data_handle($time = '', $siteid = 0, $dcac = 0, $domain_ids = [], $config_mongo)
    {
        if (empty($domain_ids)) {
            return false;
        }
        // 将域名ID集合 处理为字符类型集合[mongo中忘记处理为整型了]
        $domain_ids = array_map('strval', $domain_ids);
        $dcac = (int) $dcac;
        $siteid = (int) $siteid;

        // 配置信息
        $config = $_ENV['_db_configs'];

        $mongo = new mongodbdo($config_mongo);
        $collection_name = 'dcclog' . $time;

        $data_pv = [];

        // 1、PV
        $ops_pv = [
            ['$match' => ['dcac' => $dcac, 'domain_id' => ['$in' => $domain_ids]]],
            ['$group' => ['_id' => '$provinceid', 'value' => ['$sum' => 1]]]
        ];
        $result_pv = $mongo->collection($collection_name)->aggregate($ops_pv, true);
        if(!$result_pv)
        {
            return;
        }
        foreach ($result_pv as $v) {
            $data_pv[$v->_id] = $v->value;
        }

        // 2、独立访客UV
        $ops_uv = [
            ['$match' => ['dcac' => $dcac, 'domain_id' => ['$in' => $domain_ids]]],
            ['$group' => ['_id'	=> ['provinceid' => '$provinceid', 'cookieid' => '$cookieid']]],
            ['$group' => ['_id'	=> '$_id.provinceid', 'value' => ['$sum' => 1]]],
        ];
        $result_uv = $mongo->collection($collection_name)->aggregate($ops_uv, true);
        foreach ($result_uv as $v) {
            $data_uv[$v->_id] = $v->value;
        }

        // 3、首次访问UV
        $ops_new_uv = [
            ['$match' => ['dcac' => $dcac, 'domain_id' => ['$in' => $domain_ids], 'dcv_0' => 1, 'dcv_1' => 1]],
            ['$group' => ['_id'	=> ['provinceid' => '$provinceid', 'cookieid' => '$cookieid']]],
            ['$group' => ['_id'	=> '$_id.provinceid', 'value' => ['$sum' => 1]]],
//            ['$group' => ['_id'	=> '$provinceid', 'value'	=> ['$sum' => 1]]]
        ];
        $result_new_uv = $mongo->collection($collection_name)->aggregate($ops_new_uv, true);
        foreach ($result_new_uv as $v) {
            $data_new_uv[$v->_id] = $v->value;
        }

        // 4、独立IP
        $ops_ip = [
            ['$match' => ['dcac' => $dcac, 'domain_id' => ['$in' => $domain_ids]]],
            ['$group' => ['_id'	=> ['provinceid' => '$provinceid', 'ip' => '$ip']]],
            ['$group' => ['_id'	=> '$_id.provinceid', 'value' => ['$sum' => 1]]],
        ];
        $result_ip = $mongo->collection($collection_name)->aggregate($ops_ip, true);
        foreach ($result_ip as $v) {
            $data_ip[$v->_id] = $v->value;
        }

        // 5、访次VV
        $ops_vv = [
            ['$match' => ['dcac' => $dcac, 'domain_id' => ['$in' => $domain_ids]]],
            ['$group' => ['_id'	=> ['provinceid' => '$provinceid', 'cookieid' => '$cookieid', 'dcv_0' => '$dcv_0']]],
//            ['$group' => ['_id'	=> ['provinceid' => '$_id.provinceid', 'cookieid' => '$_id.cookieid'], 'count' => ['$sum' => 1]]],
//            ['$group' => ['_id'	=> '$_id.provinceid', 'value' => ['$sum' => '$count']]],
            ['$group' => ['_id'	=> '$_id.provinceid', 'value' => ['$sum' => 1]]],
        ];
        $result_vv = $mongo->collection($collection_name)->aggregate($ops_vv, true);
        foreach ($result_vv as $v) {
            $data_vv[$v->_id] = $v->value;
        }

        // 6、访次VV
        $ops_more_vv = [
            ['$match' => ['dcac' => $dcac, 'domain_id' => ['$in' => $domain_ids]]],
            ['$group' => ['_id'	=> ['provinceid' => '$provinceid', 'cookieid' => '$cookieid', 'dcv_0' => '$dcv_0']]],
            ['$group' => ['_id'	=> ['provinceid' => '$_id.provinceid', 'cookieid' => '$_id.cookieid'], 'count' => ['$sum' => 1]]],
            ['$match' => ['count' => ['$gt' => 1]]],
            ['$group' => ['_id'	=> '$_id.provinceid', 'value' => ['$sum' => 1]]],
        ];
        $result_more_vv = $mongo->collection($collection_name)->aggregate($ops_more_vv, true);
        foreach ($result_more_vv as $v) {
            $data_more_vv[$v->_id] = $v->value;
        }

        // 7、时长
        $ops_time = [
            ['$match' => ['dcac' => $dcac, 'domain_id' => ['$in' => $domain_ids]]],
            ['$group' => [
                '_id'	=> ['provinceid' => '$provinceid', 'cookieid' => '$cookieid', 'dcv_0' => '$dcv_0'],
                'mintime' => ['$min' => '$time'],
                'maxtime' => ['$max' => '$time']
            ]],
            ['$group' => [
                '_id'	=> ['provinceid' => '$_id.provinceid', 'cookieid' => '$_id.cookieid', 'dcv_0' => '$_id.dcv_0'],
                'count' => ['$sum' => ['$subtract' => ['$maxtime', '$mintime']]]
            ]],
            ['$group' => [
                '_id'	=> '$_id.provinceid',
                'value' => ['$sum' => '$count']
            ]],
        ];
        $result_time = $mongo->collection($collection_name)->aggregate($ops_time, true);
        foreach ($result_time as $v) {
            $data_time[$v->_id] = $v->value;
        }

        // 8、二跳量（PV=2的cookie的个数）
        $ops_twojump = [
            ['$match' => ['dcac' => $dcac, 'domain_id' => ['$in' => $domain_ids], 'dcv_1' => 2]],
            ['$group' => ['_id'	=> '$provinceid', 'value'	=> ['$sum' => 1]]]
        ];
        $result_twojump = $mongo->collection($collection_name)->aggregate($ops_twojump, true);
        foreach ($result_twojump as $v) {
            $data_twojump[$v->_id] = $v->value;
        }

        if (!empty($data_pv)) {
            $db = db::getInstance($config['_mysqls']['dns_autotracking_data_t']);

            $time = date('Y-m-d', strtotime($time));
            $create_time = date('Y-m-d H:i:s');
            $sqlArr = $nation_datas = [];
            foreach ($data_pv as $k => $v) {
                $dcac 				= (int) $dcac;
                $provinceid 		= (int) $k;
                $site_id			= (int) $siteid;
                $data_uv[$k] 		= isset($data_uv[$k]) ? $data_uv[$k] + 0 : 0;
                $data_new_uv[$k] 	= isset($data_new_uv[$k]) ? $data_new_uv[$k] + 0 : 0;
                $data_ip[$k] 		= isset($data_ip[$k]) ? $data_ip[$k] + 0 : 0;
                $data_vv[$k] 		= isset($data_vv[$k]) ? $data_vv[$k] + 0 : 0;
                $data_more_vv[$k]	= isset($data_more_vv[$k]) ? $data_more_vv[$k] + 0 : 0;
                $data_time[$k] 		= isset($data_time[$k]) ? $data_time[$k] + 0 : 0;
                $data_twojump[$k] 	= isset($data_twojump[$k]) ? $data_twojump[$k] + 0 : 0;

				if($data_new_uv[$k] > $data_uv[$k])
                {
                    $data_new_uv[$k] = $data_uv[$k];
                }
				
                $sqlArr[] = "({$dcac}, '{$time}', {$provinceid}, {$site_id}, {$v}, {$data_uv[$k]}, {$data_new_uv[$k]}, {$data_ip[$k]}, {$data_vv[$k]}, {$data_more_vv[$k]}, {$data_time[$k]}, {$data_twojump[$k]}, '{$create_time}')";
                // 用于全国数据的统计
                $nation_datas['pv'][] 		= $v;
                $nation_datas['uv'][] 		= $data_uv[$k];
                $nation_datas['new_uv'][]	= $data_new_uv[$k];
                $nation_datas['ip'][] 		= $data_ip[$k];
                $nation_datas['vv'][] 		= $data_vv[$k];
                $nation_datas['more_vv'][] 	= $data_more_vv[$k];
                $nation_datas['time'][] 	= $data_time[$k];
                $nation_datas['twojump'][]	= $data_twojump[$k];
            }
            // 全国
            $_pv 		= array_sum($nation_datas['pv']);
            $_uv 		= array_sum($nation_datas['uv']);
            $_new_uv	= array_sum($nation_datas['new_uv']);
            $_ip 		= array_sum($nation_datas['ip']);
            $_vv 		= array_sum($nation_datas['vv']);
            $_more_vv 	= array_sum($nation_datas['more_vv']);
            $_time 		= array_sum($nation_datas['time']);
            $_twojump 	= array_sum($nation_datas['twojump']);
            $nation_sqlArr = "({$dcac}, '{$time}', 0, {$site_id}, {$_pv}, {$_uv}, {$_new_uv}, {$_ip}, {$_vv}, {$_more_vv}, {$_time}, {$_twojump}, '{$create_time}')";

            $SQL = implode(',', $sqlArr) . "," . $nation_sqlArr;
            $db->query("REPLACE INTO `auto_report_website` VALUES {$SQL}");
            return true;
        } else {
            return false;
        }

    }
}

