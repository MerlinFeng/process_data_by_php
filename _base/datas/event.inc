<?php

/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2016/8/28
 * Time: 22:32
 */

/**
 * Class DatasEvent
 * 事件报告
 */
class DatasEvent
{
    /**
     * 事件报告
     * @param  string $time [description]
     * @return [type]       [description]
     */
    public static function dodata($time = '')
    {
        if (!check_time($time)) {
            return false;
        }

        // 配置信息
        $config = $_ENV['_db_configs'];

        $mongo = new mongodbdo($config['_mongos']['dns_event']);
        $collection_name = 'event' . $time;

        // 1、PV
        $ops_pv = [
            ['$match' => ['dce' => ['$exists' => 'true']]],
            ['$group' => ['_id'	=> ['dcac' => '$dcac', 'dce' => '$dce'], 'value'	=> ['$sum' => 1]]]
        ];
        $result_pv = $mongo->collection($collection_name)->aggregate($ops_pv);
        foreach ($result_pv as $v) {
            $data_pv[implode('||', (array)$v->_id)] = $v->value;
        }

        // 2、独立访客UV
        $ops_uv = [
            ['$match' => ['dce' => ['$exists' => 'true']]],
            ['$group' => ['_id'	=> ['dcac' => '$dcac', 'dce' => '$dce', 'cookieid' => '$cookieid']]],
            ['$group' => ['_id'	=> ['dcac' => '$_id.dcac', 'dce' => '$_id.dce'], 'value' => ['$sum' => 1]]],
        ];
        $result_uv = $mongo->collection($collection_name)->aggregate($ops_uv, true);
        foreach ($result_uv as $v) {
            $data_uv[implode('||', (array)$v->_id)] = $v->value;
        }
        if ($data_pv) {
            $db = db::getInstance($config['_mysqls']['dns_autotracking_data_t']);
            $time = date('Y-m-d', strtotime($time));
            $create_time = date('Y-m-d H:i:s');
            $sqlArr = [];
            foreach ($data_pv as $k => $v) {
                list($dcac, $dce) = explode('||', $k, 2);
                $dcac 				= (int) $dcac;
                if($dce)
                {
                    $aDces = explode('|', $dce);
                    if(!empty(array_filter($aDces)) && count($aDces) < 4)
                    {
                        array_push($aDces, "");
                    }
                    list($sort, $action, $tag, $value) = $aDces;
                    $sqlArr[] = "({$dcac}, '{$time}', '{$sort}', '{$action}', '{$tag}', '{$value}', {$v}, {$data_uv[$k]},'{$create_time}')";
                }
            }
            $SQL = "delete FROM `auto_report_affair` WHERE `time` = '{$time}'";
            err_log("{$time}|{$SQL}", atFLAG);
            $db->query($SQL);

            $SQL = implode(',', $sqlArr);
            $db->query("INSERT INTO `auto_report_affair` VALUES {$SQL}");

            err_log("intdrainage_data success: {$time}");
            return true;
        } else {
            return false;
        }
    }
}
