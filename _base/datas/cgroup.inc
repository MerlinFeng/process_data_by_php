<?php
// +----------------------------------------------------------------------
// | Copyright (c) 2016 All rights reserved.
// +----------------------------------------------------------------------
// | Author: Jerry.fang <fangzq@yiche.com>
// +----------------------------------------------------------------------

/**
 * 内容组报告
 */
class DatasCgroup
{
    /**
     * 循环执行每个内容组
     * @param  string $time [description]
     * @return [type]       [description]
     */
    /*public static function dodata($time = '', $mongo_config)
    {
        if (!check_time($time)) {
            return false;
        }
        // 获取所有内容组
        $group_ids = get_report_u_ids($time);
        if (!$group_ids || empty($group_ids)) {
            err_log("cgroup_data success(no data): {$time}");
        } else {
            foreach ($group_ids as $web_id => $groups) {
                $domain_ids = explode(',', $v['domain_ids']);
                $url_ids = explode(',', $v['url_ids']);
                self::_data_handle($time, $mongo_config, $v['id'], $v['web_id'], $v['wb_id'], $domain_ids, $url_ids);
            }
            err_log("cgroup_data success: {$time}");
        }
        return true;
    }*/

    /**
     * 执行跑数逻辑【没有增加domain_id的限制】
     * @param  string  $time       日期
     * @param  integer $cgroupid   内容组ID
     * @param  integer $dcac       网站ID
     * @param  integer $site_id    站点ID
     * @param  array   $domain_ids 所属域名ID集合
     * @param  array   $url_ids    包含页面ID集合
     * @return [type]              [description]
     */
    public static function _data_handle($time = '', $cgroupid = 0, $dcac = 0, $site_id = 0, $domain_ids = [], $url_ids = [])
    {
        if (empty($domain_ids) || empty($url_ids)) {
            return false;
        }
        // 将域名ID集合 处理为字符类型集合[mongo中忘记处理为整型了]
        $domain_ids = array_map('strval', $domain_ids);
        $url_ids	= array_map('intval', $url_ids);
        $cgroupid	= (int) $cgroupid;
        $dcac		= (int) $dcac;
        $site_id	= (int) $site_id;

        // 配置信息
        $config = $_ENV['_db_configs'];
        $mongo_config = $dcac == 163 ? $config['_mongos']['dns_dcclog_163'] : $config['_mongos']['dns_dcclog_other'];
        $mongo = new mongodo($mongo_config);
        $collection_name = 'dcclog' . $time;

        $data_pv = $data_uv = $data_new_uv = $data_ip = $data_vv = $data_more_vv = $data_times = $data_twojump = $data_out = 0;

        // 1、PV
        $ops_pv = [
            ['$match' => ['dcac' => $dcac, 'url_id' => ['$in' => $url_ids]]],
            ['$group' => ['_id'	=> null, 'value' => ['$sum' => 1]]],
        ];
        $result_pv = $mongo->collection($collection_name)->aggregate($ops_pv, ['allowDiskUse' => true]);
        if(count($result_pv)<1)
        {
            return;
        }
        if ($result_pv && isset($result_pv['result'][0]['value'])) {
            $data_pv = $result_pv['result'][0]['value'];
        }
        err_log("pv end", "163log");

        // 2、独立访客UV
        $ops_uv = [
            ['$match' => ['dcac' => $dcac, 'url_id' => ['$in' => $url_ids]]],
            ['$group' => ['_id'	=> ['provinceid' => '$provinceid', 'cookieid' => '$cookieid']]],
            ['$group' => ['_id'	=> null, 'value' => ['$sum' => 1]]],
        ];
        $result_uv = $mongo->collection($collection_name)->aggregate($ops_uv, ['allowDiskUse' => true]);
        if ($result_uv && isset($result_uv['result'][0]['value'])) {
            $data_uv = $result_uv['result'][0]['value'];
        }

        err_log("uv end".count($data_uv), "163log");
        // 3、首次访问UV
        $ops_new_uv = [
            ['$match' => ['dcac' => $dcac, 'url_id' => ['$in' => $url_ids], 'dcv_0' => 1, 'dcv_1' => 1]],
            ['$group' => ['_id'	=> null, 'value'	=> ['$sum' => 1]]],
        ];
        $result_new_uv = $mongo->collection($collection_name)->aggregate($ops_new_uv, ['allowDiskUse' => true]);
        if ($result_new_uv && isset($result_new_uv['result'][0]['value'])) {
            $data_new_uv = $result_new_uv['result'][0]['value'];
        }
        err_log("new uv end".count($data_new_uv), "163log");
        // 4、独立IP
        $ops_ip = [
            ['$match' => ['dcac' => $dcac, 'url_id' => ['$in' => $url_ids]]],
            ['$group' => ['_id'	=> ['provinceid' => '$provinceid', 'ip' => '$ip']]],
            ['$group' => ['_id'	=> null, 'value' => ['$sum' => 1]]],
        ];
        $result_ip = $mongo->collection($collection_name)->aggregate($ops_ip, ['allowDiskUse' => true]);
        if ($result_ip && isset($result_ip['result'][0]['value'])) {
            $data_ip = $result_ip['result'][0]['value'];
        }
        err_log("i[ end".count($data_ip), "163log");

        // 5、访次VV
        $ops_vv = [
            ['$match' => ['dcac' => $dcac, 'url_id' => ['$in' => $url_ids]]],
            ['$group' => ['_id'	=> ['provinceid' => '$provinceid', 'cookieid' => '$cookieid', 'dcv_0' => '$dcv_0']]],
            ['$group' => ['_id'	=> ['provinceid' => '$_id.provinceid', 'cookieid' => '$_id.cookieid'], 'count' => ['$sum' => 1]]],
            ['$group' => ['_id'	=> null, 'value' => ['$sum' => '$count']]],
        ];
        $result_vv = $mongo->collection($collection_name)->aggregate($ops_vv, ['allowDiskUse' => true]);
        if ($result_vv && isset($result_vv['result'][0]['value'])) {
            $data_vv = $result_vv['result'][0]['value'];
        }
        err_log("data vv".count($data_vv), "163log");

        // 6、多访次VV
        $ops_more_vv = [
            ['$match' => ['dcac' => $dcac, 'url_id' => ['$in' => $url_ids]]],
            ['$group' => ['_id'	=> ['provinceid' => '$provinceid', 'cookieid' => '$cookieid', 'dcv_0' => '$dcv_0']]],
            ['$group' => ['_id'	=> ['provinceid' => '$_id.provinceid', 'cookieid' => '$_id.cookieid'], 'count' => ['$sum' => 1]]],
            ['$match' => ['count' => ['$gt' => 1]]],
            ['$group' => ['_id'	=> null, 'value' => ['$sum' => 1]]],
        ];
        $result_more_vv = $mongo->collection($collection_name)->aggregate($ops_more_vv, ['allowDiskUse' => true]);
        if ($result_more_vv && isset($result_more_vv['result'][0]['value'])) {
            $data_more_vv = $result_more_vv['result'][0]['value'];
        }
        err_log("data more vv".count($data_more_vv), "163log");

        // 7、时长【需要特殊处理】
        // ------ 分组 -------------
        $c_name_times = "dcclog{$time}_cgroup{$cgroupid}_time";
        $ops_times = [
            ['$match' => ['dcac' => $dcac, 'url_id' => ['$in' => $url_ids]]],
//            ['$match' => ['dcac' => $dcac]],
            ['$sort' 	=> ['time' => 1]],
            ['$group' 	=> [
                '_id'		=> ['provinceid' => '$provinceid', 'cookieid' => '$cookieid', 'dcv_0' => '$dcv_0'],
                'url_times' => ['$push' => ['url_id' => '$url_id', 'time' => '$time']],
                'count' 	=> ['$sum' => 1]
            ]],
            ['$match'	=> ['count' => ['$gt' => 1]]],
            ['$out'		=> $c_name_times]
        ];
        $mongo->collection($collection_name)->aggregate($ops_times, ['allowDiskUse' => true]);
        // ------ 计算 -------------
        $c_map_times = new MongoCode("function() { emit(this._id, this.url_times); }");
        $c_reduce_times = new MongoCode("function(key, values) { return values; }");
        $c_finalize_times = new MongoCode("
			function(key, out) {
				var times = [];
				for (i=0; i<out.length; i++) {
				    if (out[i+1] !== undefined) {
						times[i] = {
							dectime: out[i+1]['time'] - out[i]['time'],
							url_id: out[i]['url_id']
						};
					}
				}
				return times;
			}");
        $mongo->getDb()->command([
            "mapreduce"	=> $c_name_times,
            'map'		=> $c_map_times,
            'reduce'	=> $c_reduce_times,
            'finalize'	=> $c_finalize_times,
            'out'		=> $c_name_times . '2'
        ]);
        unset($c_map_times, $c_reduce_times, $c_finalize_times);
        // ------ 统计 -------------
        $ops_times3 = [
            ['$unwind' 	=> '$value'],
//            ['$match' => ['value.url_id' => ['$in' => $url_ids]]],
            ['$group' 	=> [
                '_id'		=> null,
                'value' 	=> ['$sum' => '$value.dectime']
            ]]
        ];

        $result_times = $mongo->collection($c_name_times . '2')->aggregate($ops_times3, ['allowDiskUse' => true]);
        if ($result_times && isset($result_times['result'][0]['value'])) {
            $data_times = $result_times['result'][0]['value'];
        }
        err_log("data times".count($data_times), "163log");
        $mongo->collection($c_name_times)->drop();
        $mongo->collection($c_name_times . '2')->drop();

        // 8、二跳量（PV=2的cookie的个数）
        $ops_twojump = [
            ['$match' => ['dcac' => $dcac, 'url_id' => ['$in' => $url_ids], 'dcv_1' => 2]],
            ['$group' => ['_id'	=> null, 'value' => ['$sum' => 1]]]
        ];
        $result_twojump = $mongo->collection($collection_name)->aggregate($ops_twojump, ['allowDiskUse' => true]);
        if ($result_twojump && isset($result_twojump['result'][0]['value'])) {
            $data_twojump = $result_twojump['result'][0]['value'];
        }
        err_log("data_twojump".count($data_twojump), "163log");

        // 9、退出量
        $ops_out = [
            ['$match' => ['dcac' => $dcac]],
            ['$sort'  => ['dcv_1' => 1]],
            ['$group' => ['_id'	=> ['provinceid' => '$provinceid', 'cookieid' => '$cookieid', 'dev_0' => '$dev_0'], 'url_id'	=> ['$last' => '$url_id']]],
            ['$match' => ['url_id' => ['$in' => $url_ids]]],
            ['$group' => ['_id' => null, 'value' => ['$sum' => 1]]]
        ];
        $result_out = $mongo->collection($collection_name)->aggregate($ops_out, ['allowDiskUse' => true]);
        if ($result_out && isset($result_out['result'][0]['value'])) {
            $data_out = $result_out['result'][0]['value'];
        }

        // 不分地域，只有PV大于0，才保存[下个版本参照站点报告增加地域]
        if ($data_pv) {
            $db = db::getInstance($config['_mysqls']['dns_autotracking_data_t']);

            $time 			= date('Y-m-d', strtotime($time));
            $create_time 	= date('Y-m-d H:i:s');
            $dcac 			= (int) $dcac;
            $cgroupid		= (int) $cgroupid;
            $data_pv		= (int) $data_pv;
            $data_uv		= (int) $data_uv;
            $data_new_uv	= (int) $data_new_uv;
            $data_ip		= (int) $data_ip;
            $data_vv		= (int) $data_vv;
            $data_more_vv	= (int) $data_more_vv;
            $data_times		= (int) $data_times;
            $data_twojump	= (int) $data_twojump;
            $data_out		= (int) $data_out;

            $sql = "({$dcac}, '{$time}', 0, {$site_id}, {$cgroupid}, {$data_pv}, {$data_uv}, {$data_new_uv}, {$data_ip}, {$data_vv}, {$data_more_vv}, {$data_times}, {$data_twojump}, {$data_out}, '{$create_time}')";

            $SQL = "delete FROM `auto_report_cgroup` WHERE `time` = '{$time}' and web_id ={$dcac} and wb_id={$site_id} and 	cg_id ={$cgroupid}";
            //echo $SQL."\n";

            $db->query($SQL);
            err_log("{$time}|INSERT INTO `auto_report_cgroup` VALUES {$sql}", atFLAG);
            //echo "INSERT INTO `auto_report_cgroup` VALUES {$sql}\n";
            $db->query("INSERT INTO `auto_report_cgroup` VALUES {$sql}");
            return true;
        } else {
            return false;
        }

    }
}


