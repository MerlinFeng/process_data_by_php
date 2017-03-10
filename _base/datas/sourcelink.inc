<?php
/**
 * Created by PhpStorm.
 * 网站来源统计分类之外部链接
 * User: fengqiang
 * Date: 2016/12/30
 * Time: 14:29
 */
class DatasSourcelink
{
    private static $self = NULL;
    public static function instance()
    {
        if (self::$self == NULL) {
            self::$self = new self;
        }
        return self::$self;
    }

    public function do_data($time = '', $config_mongo, $type = 'recomand_domain')
    {
//        $this->do_handle($time, $config_mongo, $type, true);//新用户
        $this->do_handle($time, $config_mongo, $type);//老用户
    }

    public function do_handle($time = '', $config_mongo, $type, $is_new = false)
    {
        if (!check_time($time)) {
            return false;
        }
        $config = $_ENV['_db_configs'];
        // 配置信息
        $mongo = new mongodbdo($config_mongo);
        $collection_name = $config_mongo['dbname'] == 'leads_test'?'leads'.$time:'dcclog'.$time;
        //第一步按；来源分类输出缓存
        $link_name = "source{$time}_link";//外部链接

        if($is_new) //新用户
        {
            $direct_out = [
                ['$match' => ['dctp' => 1, 'dcisnw' => 1]],
                ['$out' => $link_name]    // 先存入到临时结果集里
            ];
            $new = 2;
        }
        else //老用户
        {
            $direct_out = [
                ['$match' => ['dctp' => 1, 'dcisnw' => ['$ne' => 1]]],
                ['$out' => $link_name]    // 先存入到临时结果集里
            ];
            $new = 1;
        }
        $mongo->collection($collection_name)->aggregate($direct_out, true);
        $mongo = new mongodbdo($config_mongo);;

        // 1、PV
        $ops_pv = [
            ['$match' => [$type => ['$exists' => true]]],
            ['$group' => ['_id'	=> ['dcac' => '$dcac', $type => '$'.$type], 'value'	=> ['$sum' => 1]]],
//            ['$sort' => ['value',-1]],
//            ['$limit' => 10],
        ];
        $result_pv = $mongo->collection($link_name)->aggregate($ops_pv, true);
//        $result_pv = list_sort_by(array($result_pv), 'value', 'desc');

        foreach ($result_pv as $v) {
            $data_pv[implode('||', (array)$v->_id)] = $v->value;
        }
//        arsort($data_pv);
//        $data_pv = array_slice($data_pv, 0, 5000);
        err_log("link{$type}|pv|end", $config_mongo['dbname']);

        // 2、独立访客UV
        $ops_uv = [
            ['$group' => ['_id'	=> ['dcac' => '$dcac',$type => '$'.$type, 'cookieid' => '$cookieid']]],
            ['$group' => ['_id'	=> ['dcac' => '$_id.dcac', $type => '$_id.'.$type], 'value' => ['$sum' => 1]]],
        ];
        $result_uv = $mongo->collection($link_name)->aggregate($ops_uv,  true);
        foreach ($result_uv as $v) {
            $data_uv[implode('||', (array)$v->_id)] = $v->value;
        }
        err_log("link{$type}|独立访客UV|end", $config_mongo['dbname']);

        // 3、首次访问UV
        $ops_new_uv = [
            ['$match' => ['dcv_0' => 1, 'dcv_1' => 1]],
            ['$group' => ['_id'	=> ['dcac' => '$dcac',$type => '$'.$type, 'cookieid' => '$cookieid']]],
            ['$group' => ['_id'	=> ['dcac' => '$_id.dcac',$type => '$_id.'.$type], 'value' => ['$sum' => 1]]],
        ];
        $result_new_uv = $mongo->collection($link_name)->aggregate($ops_new_uv,  true);
        foreach ($result_new_uv as $v) {
            $data_new_uv[implode('||', (array)$v->_id)] = $v->value;
        }
        err_log("link{$type}|首次访问UV|end", $config_mongo['dbname']);

        // 4、独立IP
        $ops_ip = [
            ['$group' => ['_id'	=> ['dcac' => '$dcac',$type => '$'.$type, 'ip' => '$ip']]],
            ['$group' => ['_id'	=> ['dcac' => '$_id.dcac',$type => '$_id.'.$type], 'value' => ['$sum' => 1]]],
        ];
        $result_ip = $mongo->collection($link_name)->aggregate($ops_ip, true);
        foreach ($result_ip as $v) {
            $data_ip[implode('||', (array)$v->_id)] = $v->value;
        }
        err_log("link{$type}|独立IP|end", $config_mongo['dbname']);
        // 5、访次VV
        $ops_vv = [
            ['$group' => ['_id'	=> ['dcac' => '$dcac', $type => '$'.$type, 'cookieid' => '$cookieid', 'dcv_0' => '$dcv_0']]],
            ['$group' => ['_id'	=> ['dcac' => '$_id.dcac', $type => '$_id.'.$type, 'cookieid' => '$_id.cookieid'], 'count' => ['$sum' => 1]]],
            ['$group' => ['_id'	=> ['dcac' => '$_id.dcac', $type => '$_id.'.$type], 'value' => ['$sum' => '$count']]],
        ];
        $result_vv = $mongo->collection($link_name)->aggregate($ops_vv, true);
        foreach ($result_vv as $v) {
            $data_vv[implode('||', (array)$v->_id)] = $v->value;
        }
        err_log("link{$type}|访次VV|end", $config_mongo['dbname']);

        // 6、多访次VV
        $ops_more_vv = [
            ['$group' => ['_id'	=> ['dcac' => '$dcac',$type => '$'.$type, 'cookieid' => '$cookieid', 'dcv_0' => '$dcv_0']]],
            ['$group' => ['_id'	=> ['dcac' => '$_id.dcac', $type => '$_id.'.$type, 'cookieid' => '$_id.cookieid'], 'count' => ['$sum' => 1]]],
            ['$match' => ['count' => ['$gt' => 1]]],
            ['$group' => ['_id'	=> ['dcac' => '$_id.dcac', $type => '$_id.'.$type], 'value' => ['$sum' => 1]]],
        ];
        $result_more_vv = $mongo->collection($link_name)->aggregate($ops_more_vv, true);
        foreach ($result_more_vv as $v) {
            $data_more_vv[implode('||', (array)$v->_id)] = $v->value;
        }
        err_log("link{$type}|多访次VV|end", $config_mongo['dbname']);

        // 7、时长
        $ops_time = [
            ['$group' => [
                '_id'	=> ['dcac' => '$dcac',$type => '$'.$type, 'cookieid' => '$cookieid', 'dcv_0' => '$dcv_0'],
                'mintime' => ['$min' => '$time'],
                'maxtime' => ['$max' => '$time']
            ]],
            ['$group' => [
                '_id'	=> ['dcac' => '$_id.dcac', $type => '$_id.'.$type, 'cookieid' => '$_id.cookieid', 'dcv_0' => '$_id.dcv_0'],
                'count' => ['$sum' => ['$subtract' => ['$maxtime', '$mintime']]]
            ]],
            ['$group' => [
                '_id'	=> ['dacac' => '$_id.dcac', $type => '$_id.'.$type],
                'value' => ['$sum' => '$count']
            ]],
        ];
        $result_time = $mongo->collection($link_name)->aggregate($ops_time, true);
        foreach ($result_time as $v) {
            $data_time[implode('||', (array)$v->_id)] = $v->value;
        }
        err_log("link{$type}|时长|end", $config_mongo['dbname']);

        // 8、二跳量（PV=2的cookie的个数）
        $ops_twojump = [
            ['$match' => ['dcv_1' => 2]],
            ['$group' => ['_id'	=> ['dcac' => '$dcac',$type => '$'.$type], 'value'	=> ['$sum' => 1]]]
        ];
        $result_twojump = $mongo->collection($link_name)->aggregate($ops_twojump, true);
        foreach ($result_twojump as $v) {
            $data_twojump[implode('||', (array)$v->_id)] = $v->value;
        }

        if($data_pv)
        {
            err_log("{$time}|domain|mysql|start", atFLAG);
            if($config_mongo['dbname'] == 'leads_test')
            {
                $mysqlconfig = $type == 'recomand_domain'?$config['_mysqls']['dns_autotracking_lead_outdomain']:$config['_mysqls']['dns_autotracking_lead_outurl'];
            }
            else
            {
                $mysqlconfig = $type == 'recomand_domain'?$config['_mysqls']['dns_autotracking_data_outdomain']:$config['_mysqls']['dns_autotracking_data_outurl'];
            }

            $db = db::getInstance($mysqlconfig);
            $time = date('Y-m-d', strtotime($time));
            $create_time = date('Y-m-d H:i:s');
            $sqlArr = $nation_datas = $nation_sqlArr = [];
            foreach ($data_pv as $k => $v) {
                list($dcac, $domain_id) = explode('||', $k, 2);
                $dcac 				= (int) $dcac;
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

                if($config_mongo['dbname'] == 'leads_test')
                {
                    $tableName = $type == 'recomand_domain' ? "auto_domain_lead_{$dcac}_0" : "auto_url_lead_{$dcac}_0";
                }
                else
                {
                    $tableName = $type == 'recomand_domain' ? "auto_domain_look_{$dcac}_0" : "auto_url_look_{$dcac}_0";
                }

                $sqlArr[$tableName][] = "( '{$time}', {$new}, '{$domain_id}', {$v}, {$data_uv[$k]}, {$data_new_uv[$k]}, {$data_ip[$k]}, {$data_vv[$k]}, {$data_more_vv[$k]}, {$data_time[$k]}, {$data_twojump[$k]}, '{$create_time}')";


            }

            if(!$sqlArr)
            {
                return;
            }

            foreach ($sqlArr as $table => $sqls)
            {
                //插入数据之前先删除当天数据
                $SQL = "delete FROM {$table} WHERE `time` = '{$time}' AND  `type` = {$new}";
                err_log("{$time}|domain|mysql|{$SQL}", $config_mongo['dbname']);

                $db->query($SQL);
                $sqls = join(',', $sqls);
                $SQL_1 = "INSERT INTO `{$table}` VALUES " .$sqls ;
                $db->query($SQL_1);
//                err_log("{$time}|domain|mysql|{$SQL}", atFLAG.'.sql');

                err_log("{$time}|link{$type}|mysql|end", $config_mongo['dbname']);
            }
            $mongo->collection($link_name)->drop();
            return true;
        } else {
            return false;
        }

    }
}