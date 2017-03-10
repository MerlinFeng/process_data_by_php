<?php
/**
 * Created by PhpStorm.
 * User: fengqiang
 * Date: 2017/1/16
 * Time: 10:38
 */
class DatasAutovalue
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
        $this->do_handle($time, $config_mongo);
    }

    public function do_handle($time = '', $config_mongo)
    {
        if (!check_time($time)) {
            return false;
        }
        $config = $_ENV['_db_configs'];
        // 配置信息
        $mongo = new mongodo($config_mongo);
        $collection_name = 'dcclog' . $time;

        //第一步按；来源分类输出缓存
        $auto_name = "autovalue{$time}";//外部链接

        $tmp_name_pv = $auto_name."pv";
        // 1、PV
        $ops_pv = [
            ['$match' => ['dccv' => ['$exists' => true]]],
            ['$group' => ['_id'	=> ['dcac' => '$dcac', 'dccv' => '$dccv'], 'value'	=> ['$sum' => 1]]],
            ['$out'=>$tmp_name_pv],
        ];
        $mongo->collection($collection_name)->aggregate($ops_pv, ['allowDiskUse' => true]);
        $cur = $mongo->collection($tmp_name_pv)->find();
        while ($cur->hasNext()) {
            $v = $cur->getNext();
            //page全部数据
            $data_pv[implode('||', $v['_id'])] = $v['value'];

        }
        unset($cur);
        if(!$data_pv)
        {
            return;
        }
        $mongo->collection($tmp_name_pv)->drop();

        err_log("{$auto_name}|pv|end", $config_mongo['dbname']);

        // 2、独立访客UV
        $tmp_name_uv = $auto_name."uv";
        $ops_uv = [
            ['$match' => ['dccv' => ['$exists' => true]]],
            ['$group' => ['_id'	=> ['dcac' => '$dcac', 'dccv' => '$dccv', 'cookieid' => '$cookieid']]],
            ['$group' => ['_id'	=> ['dcac' => '$_id.dcac','dccv' => '$_id.dccv'], 'value' => ['$sum' => 1]]],
            ['$out' => $tmp_name_uv]
        ];

        $mongo->collection($collection_name)->aggregate($ops_uv,  ['allowDiskUse' => true]);
        $cur = $mongo->collection($tmp_name_uv)->find();
        while ($cur->hasNext()) {
            $v = $cur->getNext();
            $data_uv[implode('||', $v['_id'])] = $v['value'];

        }
        $mongo->collection($tmp_name_uv)->drop();
        err_log("{$auto_name}|独立访客UV|end", $config_mongo['dbname']);

        // 3、首次访问UV
        $tmp_name_nv = $auto_name."nv";
        $ops_new_uv = [
            ['$match' => ['dccv' => ['$exists' => true]]],
            ['$match' => ['dcv_0' => 1, 'dcv_1' => 1]],
            ['$group' => ['_id'	=> ['dcac' => '$dcac', 'dccv' => '$dccv', 'cookieid' => '$cookieid']]],
            ['$group' => ['_id'	=> ['dcac' => '$_id.dcac','dccv' => '$_id.dccv'], 'value' => ['$sum' => 1]]],
            ['$out' => $tmp_name_nv]
        ];
        $mongo->collection($collection_name)->aggregate($ops_new_uv,  ['allowDiskUse' => true]);
        $cur = $mongo->collection($tmp_name_nv)->find();
        while ($cur->hasNext()) {
            $v = $cur->getNext();
            $data_new_uv[implode('||', $v['_id'])] = $v['value'];

        }
        $mongo->collection($tmp_name_nv)->drop();
        err_log("{$auto_name}|首次访问UV|end", $config_mongo['dbname']);

        // 4、独立IP
        $tmp_name_ip = $auto_name."ip";
        $ops_ip = [
            ['$match' => ['dccv' => ['$exists' => true]]],
            ['$group' => ['_id'	=> ['dcac' => '$dcac', 'dccv' => '$dccv', 'ip' => '$ip']]],
            ['$group' => ['_id'	=> ['dcac' => '$_id.dcac', 'dccv' => '$_id.dccv'], 'value' => ['$sum' => 1]]],
            ['$out' => $tmp_name_ip]
        ];
        $mongo->collection($collection_name)->aggregate($ops_ip, ['allowDiskUse' => true]);
        $cur = $mongo->collection($tmp_name_ip)->find();
        while ($cur->hasNext()) {
            $v = $cur->getNext();
            $data_ip[implode('||', $v['_id'])] = $v['value'];

        }
        $mongo->collection($tmp_name_ip)->drop();
        err_log("{$auto_name}|独立IP|end", $config_mongo['dbname']);
        // 5、访次VV
        $tmp_name_vv = $auto_name."vv";
        $ops_vv = [
            ['$match' => ['dccv' => ['$exists' => true]]],
            ['$group' => ['_id'	=> ['dcac' => '$dcac', 'dccv' => '$dccv', 'cookieid' => '$cookieid', 'dcv_0' => '$dcv_0']]],
            ['$group' => ['_id'	=> ['dcac' => '$_id.dcac', 'dccv' => '$_id.dccv', 'cookieid' => '$_id.cookieid'], 'count' => ['$sum' => 1]]],
            ['$group' => ['_id'	=> ['dcac' => '$_id.dcac', 'dccv' => '$_id.dccv'], 'value' => ['$sum' => '$count']]],
            ['$out' => $tmp_name_vv]
        ];
        $mongo->collection($collection_name)->aggregate($ops_vv, ['allowDiskUse' => true]);
        $cur = $mongo->collection($tmp_name_vv)->find();
        while ($cur->hasNext()) {
            $v = $cur->getNext();
            $data_vv[implode('||', $v['_id'])] = $v['value'];

        }
        $mongo->collection($tmp_name_vv)->drop();
        err_log("{$auto_name}|访次VV|end", $config_mongo['dbname']);

        // 6、多访次VV
        $tmp_name_mvv = $auto_name."mvv";
        $ops_more_vv = [
            ['$match' => ['dccv' => ['$exists' => true]]],
            ['$group' => ['_id'	=> ['dcac' => '$dcac', 'dccv' => '$dccv', 'cookieid' => '$cookieid', 'dcv_0' => '$dcv_0']]],
            ['$group' => ['_id'	=> ['dcac' => '$_id.dcac', 'dccv' => '$_id.dccv', 'cookieid' => '$_id.cookieid'], 'count' => ['$sum' => 1]]],
            ['$match' => ['count' => ['$gt' => 1]]],
            ['$group' => ['_id'	=> ['dcac' => '$_id.dcac', 'dccv' => '$_id.dccv'], 'value' => ['$sum' => 1]]],
            ['$out' => $tmp_name_mvv]
        ];
        $mongo->collection($collection_name)->aggregate($ops_more_vv, ['allowDiskUse' => true]);
        $cur = $mongo->collection($tmp_name_mvv)->find();
        while ($cur->hasNext()) {
            $v = $cur->getNext();
            $data_more_vv[implode('||', $v['_id'])] = $v['value'];

        }
        $mongo->collection($tmp_name_mvv)->drop();
        err_log("{$auto_name}|多访次VV|end", $config_mongo['dbname']);

        // 7、时长
        $tmp_name_time = $auto_name."time";
        $ops_time = [
            ['$match' => ['dccv' => ['$exists' => true]]],
            ['$group' => [
                '_id'	=> ['dcac' => '$dcac', 'dccv' => '$dccv', 'cookieid' => '$cookieid', 'dcv_0' => '$dcv_0'],
                'mintime' => ['$min' => '$time'],
                'maxtime' => ['$max' => '$time']
            ]],
            ['$group' => [
                '_id'	=> ['dcac' => '$_id.dcac', 'dccv' => '$_id.dccv', 'cookieid' => '$_id.cookieid', 'dcv_0' => '$_id.dcv_0'],
                'count' => ['$sum' => ['$subtract' => ['$maxtime', '$mintime']]]
            ]],
            ['$group' => [
                '_id'	=> ['dcac' => '$_id.dcac', 'dccv' => '$_id.dccv'],
                'value' => ['$sum' => '$count']
            ]],
            ['$out' => $tmp_name_time],
        ];
        $mongo->collection($collection_name)->aggregate($ops_time, ['allowDiskUse' => true]);
        $cur = $mongo->collection($tmp_name_time)->find();
        while ($cur->hasNext()) {
            $v = $cur->getNext();
            $data_time[implode('||', $v['_id'])] = $v['value'];

        }
        $mongo->collection($tmp_name_time)->drop();
        err_log("{$auto_name}|时长|end", $config_mongo['dbname']);

        // 8、二跳量（PV=2的cookie的个数）
        $tmp_name_jump = $auto_name."jump";
        $ops_twojump = [
            ['$match' => ['dccv' => ['$exists' => true]]],
            ['$match' => ['dcv_1' => 2]],
            ['$group' => ['_id'	=> ['dcac' => '$dcac', 'dccv' => '$dccv'], 'value'	=> ['$sum' => 1]]],
            ['$out' => $tmp_name_jump]
        ];
        $mongo->collection($collection_name)->aggregate($ops_twojump, ['allowDiskUse' => true]);
        $cur = $mongo->collection($tmp_name_jump)->find();
        while ($cur->hasNext()) {
            $v = $cur->getNext();
            $data_twojump[implode('||', $v['_id'])] = $v['value'];

        }
        $mongo->collection($tmp_name_jump)->drop();

        if($data_pv)
        {
            err_log("{$time}|{$auto_name}|mysql|start", atFLAG);
            $mysqlconfig = $config['_mysqls']['dns_autotracking_data_t'];
            $db = db::getInstance($mysqlconfig);
            $time = date('Y-m-d', strtotime($time));
            $create_time = date('Y-m-d H:i:s');
            $sqlArr = $nation_datas = $nation_sqlArr = [];
            foreach ($data_pv as $k => $v) {
                list($dcac, $dccv) = explode('||', $k, 2);
                list($auto_type, $auto_name, $auto_value, $auto_range) = explode('|', $dccv, 4);
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
                $sqlArr[] = "( '{$dcac}', '{$time}', '{$auto_type}', '{$auto_name}', '{$auto_value}', {$v}, {$data_uv[$k]}, {$data_new_uv[$k]}, {$data_ip[$k]}, {$data_vv[$k]}, {$data_more_vv[$k]}, {$data_time[$k]}, {$data_twojump[$k]}, '{$create_time}')";
            }
            if(!$sqlArr)
            {
                return;
            }

            //插入数据之前先删除当天数据
            $SQL = "delete FROM `auto_report_define` WHERE `time` = '{$time}'";
            err_log("{$time}|{$auto_name}|mysql|{$SQL}", $config_mongo['dbname']);

            $db->query($SQL);
            $sqls = join(',', $sqlArr);
            $SQL_1 = "INSERT INTO `auto_report_define` VALUES " .$sqls ;
            $db->query($SQL_1);
//                err_log("{$time}|{$auto_name}|mysql|{$SQL}", atFLAG.'.sql');
            err_log("{$time}|{$auto_name}|mysql|end", $config_mongo['dbname']);
            return true;
        } else {
            return false;
        }

    }
}