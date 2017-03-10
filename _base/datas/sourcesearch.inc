<?php

/**
 * Created by PhpStorm.
 * 网站来源统计分类之搜索引擎
 * User: fengqiang
 * Date: 2016/12/30
 * Time: 14:29
 */
class DatasSourcesearch
{
    private static $self = NULL;

    public static function instance()
    {
        if (self::$self == NULL) {
            self::$self = new self;
        }
        return self::$self;
    }

    public function do_data($time = '', $config_mongo)
    {
        $this->do_handle($time, $config_mongo, true);
        $this->do_handle($time, $config_mongo);
    }

    public function do_handle($time = '', $config_mongo, $is_new = false)
    {
        if (!check_time($time)) {
            return false;
        }
        $config = $_ENV['_db_configs'];
        $mongo = new mongodbdo($config_mongo);
        $collection_name = $config_mongo['dbname'] == 'leads_test'?'leads'.$time:'dcclog' . $time;
        $search_name = "source{$time}_search"; //搜索引擎

        /**
         * 更新dcv=1.1用户添加字段dcisnw
         */
        /*$cookieids = $mongo->collection($collection_name)->findAll(['dcv'=>'1.1'],['cookieid']);
        $cookieids = array_column($cookieids, 'cookieid');
        $count = count($cookieids);
        $cookieidarr = array_chunk($cookieids, ceil($count/2));
        foreach($cookieidarr as $cooki)
        {
            $mongo->where_in('cookieid', $cooki);
            $mongo->setUpdates(['$push'=> ['dcisnw'=> 1]]);
            $mongo->collection($collection_name)->update_all();
        }*/


        if($is_new) //新用户
        {
            $direct_out = [
                ['$match' => ['dctp' => 2, 'dcisnw' => 1]],
                ['$out' => $search_name]    // 先存入到临时结果集里
            ];
            $type = 2;
        }
        else //老用户
        {
            $direct_out = [
                ['$match' => ['dctp' => 2, 'dcisnw' => ['$ne' => 1]]],
                ['$out' => $search_name]    // 先存入到临时结果集里
            ];
            $type = 1;
        }
        $mongo->collection($collection_name)->aggregate($direct_out, true);
        $mongo = new mongodbdo($config_mongo);

        // 1、PV
        $ops_pv = [
            ['$group' => ['_id' => ['dcac' => '$dcac', 'searchengine' => '$searchengine'], 'value' => ['$sum' => 1]]]
        ];
        $result_pv = $mongo->collection($search_name)->aggregate($ops_pv);
        foreach ($result_pv as $v) {
            $data_pv[implode('||', (array)$v->_id)] = $v->value;
        }
        err_log("search|pv|end", $config_mongo['dbname']);

        // 2、独立访客UV
        $ops_uv = [
            ['$group' => ['_id' => ['dcac' => '$dcac', 'searchengine' => '$searchengine', 'cookieid' => '$cookieid']]],
            ['$group' => ['_id' => ['dcac' => '$_id.dcac', 'searchengine' => '$_id.searchengine'], 'value' => ['$sum' => 1]]],
        ];
        $result_uv = $mongo->collection($search_name)->aggregate($ops_uv, true);
        foreach ($result_uv as $v) {
            $data_uv[implode('||', (array)$v->_id)] = $v->value;
        }
        err_log("search|独立访客UV|end", $config_mongo['dbname']);

        // 3、首次访问UV
        $ops_new_uv = [
            ['$match' => ['dcv_0' => 1, 'dcv_1' => 1]],
            ['$group' => ['_id' => ['dcac' => '$dcac', 'searchengine' => '$searchengine', 'cookieid' => '$cookieid']]],
            ['$group' => ['_id' => ['dcac' => '$_id.dcac', 'searchengine' => '$_id.searchengine'], 'value' => ['$sum' => 1]]],
        ];
        $result_new_uv = $mongo->collection($search_name)->aggregate($ops_new_uv, true);
        foreach ($result_new_uv as $v) {
            $data_new_uv[implode('||', (array)$v->_id)] = $v->value;
        }
        err_log("search|首次访问UV|end", $config_mongo['dbname']);

        // 4、独立IP
        $ops_ip = [
            ['$group' => ['_id' => ['dcac' => '$dcac', 'searchengine' => '$searchengine', 'ip' => '$ip']]],
            ['$group' => ['_id' => ['dcac' => '$_id.dcac', 'searchengine' => '$_id.searchengine'], 'value' => ['$sum' => 1]]],
        ];
        $result_ip = $mongo->collection($search_name)->aggregate($ops_ip, true);
        foreach ($result_ip as $v) {
            $data_ip[implode('||', (array)$v->_id)] = $v->value;
        }
        err_log("search|独立IP|end", $config_mongo['dbname']);
        // 5、访次VV
        $ops_vv = [
            ['$group' => ['_id' => ['dcac' => '$dcac', 'searchengine' => '$searchengine', 'cookieid' => '$cookieid', 'dcv_0' => '$dcv_0']]],
            ['$group' => ['_id' => ['dcac' => '$_id.dcac', 'searchengine' => '$_id.searchengine', 'cookieid' => '$_id.cookieid'], 'count' => ['$sum' => 1]]],
            ['$group' => ['_id' => ['dcac' => '$_id.dcac', 'searchengine' => '$_id.searchengine'], 'value' => ['$sum' => '$count']]],
        ];
        $result_vv = $mongo->collection($search_name)->aggregate($ops_vv, true);
        foreach ($result_vv as $v) {
            $data_vv[implode('||', (array)$v->_id)] = $v->value;
        }
        err_log("search|访次VV|end", $config_mongo['dbname']);

        // 6、多访次VV
        $ops_more_vv = [
            ['$group' => ['_id' => ['dcac' => '$dcac', 'searchengine' => '$searchengine', 'cookieid' => '$cookieid', 'dcv_0' => '$dcv_0']]],
            ['$group' => ['_id' => ['dcac' => '$_id.dcac', 'searchengine' => '$_id.searchengine', 'cookieid' => '$_id.cookieid'], 'count' => ['$sum' => 1]]],
            ['$match' => ['count' => ['$gt' => 1]]],
            ['$group' => ['_id' => ['dcac' => '$_id.dcac', 'searchengine' => '$_id.searchengine'], 'value' => ['$sum' => 1]]],
        ];
        $result_more_vv = $mongo->collection($search_name)->aggregate($ops_more_vv, true);
        foreach ($result_more_vv as $v) {
            $data_more_vv[implode('||', (array)$v->_id)] = $v->value;
        }
        err_log("search|多访次VV|end", $config_mongo['dbname']);

        // 7、时长

        $ops_time = [
            ['$group' => [
                '_id' => ['dcac' => '$dcac', 'searchengine' => '$searchengine', 'cookieid' => '$cookieid', 'dcv_0' => '$dcv_0'],
                'mintime' => ['$min' => '$time'],
                'maxtime' => ['$max' => '$time']
            ]],
            ['$group' => [
                '_id' => ['dcac' => '$_id.dcac', 'searchengine' => '$_id.searchengine', 'cookieid' => '$_id.cookieid', 'dcv_0' => '$_id.dcv_0'],
                'count' => ['$sum' => ['$subtract' => ['$maxtime', '$mintime']]]
            ]],
            ['$group' => [
                '_id' => ['dcac' => '$_id.dcac', 'searchengine' => '$_id.searchengine'],
                'value' => ['$sum' => '$count']
            ]],
        ];
        $result_time = $mongo->collection($search_name)->aggregate($ops_time, true);
        foreach ($result_time as $v) {
            $data_time[implode('||', (array)$v->_id)] = $v->value;
        }
        err_log("search|时长|end", $config_mongo['dbname']);

        // 8、二跳量（PV=2的cookie的个数）
        $ops_twojump = [
            ['$match' => ['dcv_1' => 2]],
            ['$group' => ['_id' => ['dcac' => '$dcac', 'searchengine' => '$searchengine',], 'value' => ['$sum' => 1]]]
        ];
        $result_twojump = $mongo->collection($search_name)->aggregate($ops_twojump, true);
        foreach ($result_twojump as $v) {
            $data_twojump[implode('||', (array)$v->_id)] = $v->value;
        }

        if ($data_pv) {
            err_log("{$time}|search|mysql|start", atFLAG);
            $db = db::getInstance($config['_mysqls']['dns_autotracking_data_t']);

            $time = date('Y-m-d', strtotime($time));
            $create_time = date('Y-m-d H:i:s');
            $sqlArr = $nation_datas = $nation_sqlArr = [];
            foreach ($data_pv as $k => $v) {
                list($dcac, $searchengine) = explode('||', $k, 2);
                $dcac 				= (int) $dcac;
                $data_uv[$k] 		= isset($data_uv[$k]) ? $data_uv[$k] + 0 : 0;
                $data_new_uv[$k] 	= isset($data_new_uv[$k]) ? $data_new_uv[$k] + 0 : 0;
                $data_ip[$k] 		= isset($data_ip[$k]) ? $data_ip[$k] + 0 : 0;
                $data_vv[$k] 		= isset($data_vv[$k]) ? $data_vv[$k] + 0 : 0;
                $data_more_vv[$k]	= isset($data_more_vv[$k]) ? $data_more_vv[$k] + 0 : 0;
                $data_time[$k] 		= isset($data_time[$k]) ? $data_time[$k] + 0 : 0;
                $data_twojump[$k] 	= isset($data_twojump[$k]) ? $data_twojump[$k] + 0 : 0;
                $searchengine =$searchengine?$searchengine:'qita';
                if($data_new_uv[$k] > $data_uv[$k])
                {
                    $data_new_uv[$k] = $data_uv[$k];
                }
                $sqlArr[$dcac][] = "({$dcac}, '{$time}', {$type}, '{$searchengine}', {$v}, {$data_uv[$k]}, {$data_new_uv[$k]}, {$data_ip[$k]}, {$data_vv[$k]}, {$data_more_vv[$k]}, {$data_time[$k]}, {$data_twojump[$k]}, '{$create_time}')";
            }

            if(!$sqlArr)
            {
                return;
            }
            $tableName = $config_mongo['dbname'] == 'leads_test'? 'auto_report_search_lead':'auto_report_search_look';
            //插入数据之前先删除当天数据
            foreach($sqlArr as $dcac => $sqlarr)
            {
                $SQL = "delete FROM {$tableName} WHERE `web_id` = {$dcac} AND `time` = '{$time}' AND `type` = {$type}";
                err_log("{$time}|search|mysql|{$SQL}", $config_mongo['dbname']);
                $db->query($SQL);

                $sqls = join(',', $sqlarr);
                $SQL_1 = "INSERT INTO `{$tableName}` VALUES " .$sqls ;
                $db->query($SQL_1);
            }

//                err_log("{$time}|search|mysql|{$SQL}", atFLAG.'.sql');

            err_log("{$time}|search|mysql|end", $config_mongo['dbname']);

            //删除缓存表
            $mongo->collection($search_name)->drop();
            return true;
        } else {
            return false;
        }
    }
}