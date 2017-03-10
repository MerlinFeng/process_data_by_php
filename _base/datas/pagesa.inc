<?php
/**
 * 页面流量报告
 * Created by PhpStorm.
 * User: fengqiang
 * Date: 2016/12/1
 * Time: 18:30
 */
class DatasPagesa
{
    /**
     * 处理数据【dcac, url_id, provinceid】
     * @param  string $time [description]
     * @return [type]       [description]
     */
    public static function dodata($time = '', $config_mongo)
    {
//	    $webids = [10];
        if (!check_time($time)) {
            return false;
        }

        // 配置信息
        $config = $_ENV['_db_configs'];
        $mongo = new mongodo($config_mongo);
        $collection_name = 'dcclog' . $time;

        err_log("{$time}|pages|mongo|start","page");

        // 1、PV
        $c_name_pv = "dcclog{$time}_url_pv";
        $ops_pv = [
//		    ['$match' => [ 'dcac' => ['$nin'=> $webids]]],
            ['$group' => ['_id'	=> ['dcac' => '$dcac', 'provinceid' => '$provinceid', 'url_id' => '$url_id'], 'value'	=> ['$sum' => 1]]],
            ['$sort' => ['value' => -1]],
            ['$out' => $c_name_pv]	// 先存入到临时结果集里
        ];
        $mongo->collection($collection_name)->aggregate($ops_pv, ['allowDiskUse' => true]);
        $cur = $mongo->collection($c_name_pv)->find();
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
        $mongo->collection($c_name_pv)->drop();
        err_log("{$time}|pages|mongo|pv|end","page");
        $countpv = count($data_pv);
        err_log("{$time}|pages|mongo|pv|count:{$countpv}","page");
        // 2、独立访客UV
        $c_name_uv = "dcclog{$time}_url_uv";
        $ops_uv = [
//            ['$match' => [ 'dcac' => ['$nin'=> $webids]]],
            ['$group' => ['_id'	=> ['dcac' => '$dcac', 'provinceid' => '$provinceid', 'url_id' => '$url_id', 'cookieid' => '$cookieid']]],
            ['$group' => ['_id'	=> ['dcac' => '$_id.dcac', 'provinceid' => '$_id.provinceid', 'url_id' => '$_id.url_id'], 'value' => ['$sum' => 1]]],
            ['$out' => $c_name_uv]	// 先存入到临时结果集里
        ];
        $mongo->collection($collection_name)->aggregate($ops_uv, ['allowDiskUse' => true]);
        $cur = $mongo->collection($c_name_uv)->find();
        while ($cur->hasNext()) {
            $v = $cur->getNext();
            $data_uv[implode('||', $v['_id'])] = $v['value'];
        }
        unset($cur);
        $mongo->collection($c_name_uv)->drop();
        err_log("{$time}|pages|mongo|uv|end","page");
        $countuv = count($data_uv);
        err_log("{$time}|pages|mongo|uv|count:{$countuv}","page");

        // 3、首次访问UV
        $c_name_new_uv = "dcclog{$time}_url_new_uv";
        $ops_new_uv = [
            ['$match' => ['dcv_0' => 1, 'dcv_1' => 1]],
            ['$group' => ['_id'	=> ['dcac' => '$dcac', 'provinceid' => '$provinceid', 'url_id' => '$url_id', 'cookieid' => '$cookieid']]],
            ['$group' => ['_id'	=> ['dcac' => '$_id.dcac', 'provinceid' => '$_id.provinceid', 'url_id' => '$_id.url_id'], 'value' => ['$sum' => 1]]],
            //['$group' => ['_id'	=> ['dcac' => '$dcac', 'provinceid' => '$provinceid', 'url_id' => '$url_id'], 'value'	=> ['$sum' => 1]]],
            ['$out' => $c_name_new_uv]	// 先存入到临时结果集里
        ];
        $mongo->collection($collection_name)->aggregate($ops_new_uv, ['allowDiskUse' => true]);
        $cur = $mongo->collection($c_name_new_uv)->find();
        while ($cur->hasNext()) {
            $v = $cur->getNext();
            $data_new_uv[implode('||', $v['_id'])] = $v['value'];
        }
        unset($cur);
        $mongo->collection($c_name_new_uv)->drop();

        err_log("{$time}|pages|mongo|new_uv|end","page");
        $countpv = count($data_new_uv);
        err_log("{$time}|pages|mongo|count:{$countpv}","page");

        // 4、独立IP
        $c_name_ip = "dcclog{$time}_url_ip";
        $ops_ip = [
//            ['$match' => [ 'dcac' => ['$nin'=> $webids]]],
            ['$group' => ['_id'	=> ['dcac' => '$dcac', 'provinceid' => '$provinceid', 'url_id' => '$url_id', 'ip' => '$ip']]],
            ['$group' => ['_id'	=> ['dcac' => '$_id.dcac', 'provinceid' => '$_id.provinceid', 'url_id' => '$_id.url_id'], 'value' => ['$sum' => 1]]],
            ['$out' => $c_name_ip]	// 先存入到临时结果集里
        ];
        $mongo->collection($collection_name)->aggregate($ops_ip, ['allowDiskUse' => true]);
        $cur = $mongo->collection($c_name_ip)->find();
        while ($cur->hasNext()) {
            $v = $cur->getNext();
            $data_ip[implode('||', $v['_id'])] = $v['value'];
        }
        unset($cur);
        $mongo->collection($c_name_ip)->drop();
        err_log("{$time}|pages|mongo|ip|end","page");
        $countpv = count($data_ip);
        err_log("{$time}|pages|mongo|count:{$countpv}","page");
        // 5、访次VV
        $c_name_vv = "dcclog{$time}_url_vv";
        $ops_vv = [
//            ['$match' => [ 'dcac' => ['$nin'=> $webids]]],
            ['$group' => ['_id'	=> ['dcac' => '$dcac', 'provinceid' => '$provinceid', 'url_id' => '$url_id', 'cookieid' => '$cookieid', 'dcv_0' => '$dcv_0']]],
            ['$group' => ['_id'	=> ['dcac' => '$_id.dcac', 'provinceid' => '$_id.provinceid', 'url_id' => '$_id.url_id', 'cookieid' => '$_id.cookieid'], 'count' => ['$sum' => 1]]],
            ['$group' => ['_id'	=> ['dcac' => '$_id.dcac', 'provinceid' => '$_id.provinceid', 'url_id' => '$_id.url_id'], 'value' => ['$sum' => '$count']]],
            ['$out' => $c_name_vv]	// 先存入到临时结果集里
        ];
        $mongo->collection($collection_name)->aggregate($ops_vv, ['allowDiskUse' => true]);
        $cur = $mongo->collection($c_name_vv)->find();
        while ($cur->hasNext()) {
            $v = $cur->getNext();
            $data_vv[implode('||', $v['_id'])] = $v['value'];
        }
        unset($cur);
        $mongo->collection($c_name_vv)->drop();
        err_log("{$time}|pages|mongo|vv|end","page");
        $countpv = count($data_vv);
        err_log("{$time}|pages|mongo|count:{$countpv}","page");
        // 6、多访次VV
        $c_name_more_vv = "dcclog{$time}_url_more_vv";
        $ops_more_vv = [
            ['$group' => ['_id'	=> ['dcac' => '$dcac', 'provinceid' => '$provinceid', 'url_id' => '$url_id', 'cookieid' => '$cookieid', 'dcv_0' => '$dcv_0']]],
            ['$group' => ['_id'	=> ['dcac' => '$_id.dcac', 'provinceid' => '$_id.provinceid', 'url_id' => '$_id.url_id', 'cookieid' => '$_id.cookieid'], 'count' => ['$sum' => 1]]],
            ['$match' => ['count' => ['$gt' => 1]]],
            ['$group' => ['_id'	=> ['dcac' => '$_id.dcac', 'provinceid' => '$_id.provinceid', 'url_id' => '$_id.url_id'], 'value' => ['$sum' => 1]]],
            ['$out' => $c_name_more_vv]	// 先存入到临时结果集里
        ];
        $mongo->collection($collection_name)->aggregate($ops_more_vv, ['allowDiskUse' => true]);
        $cur = $mongo->collection($c_name_more_vv)->find();
        while ($cur->hasNext()) {
            $v = $cur->getNext();
            $data_more_vv[implode('||', $v['_id'])] = $v['value'];
        }
        unset($cur);
        $mongo->collection($c_name_more_vv)->drop();
        err_log("{$time}|pages|mongo|more_vv|end","page");
        $countpv = count($data_more_vv);
        err_log("{$time}|pages|mongo|count:{$countpv}","page");
        // 7、时长【需要特殊处理】
        // ------ 分组 -------------
        $c_name_times = "dcclog{$time}_url_time";
        $ops_times = [
            ['$sort' 	=> ['time' => 1]],
            ['$group' 	=> [
                '_id'		=> ['dcac' => '$dcac', 'provinceid' => '$provinceid', 'cookieid' => '$cookieid', 'dcv_0' => '$dcv_0'],
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
        // ------ 统计 -------------
        $ops_times3 = [
            ['$unwind' 	=> '$value'],
            ['$group' 	=> [
                '_id'		=> ['dcac' => '$_id.dcac', 'provinceid' => '$_id.provinceid', 'url_id' => '$value.url_id'],
                'value' 	=> ['$sum' => '$value.dectime']
            ]],
            ['$out'		=> $c_name_times . '3']
        ];
        $mongo->collection($c_name_times . '2')->aggregate($ops_times3, ['allowDiskUse' => true]);

        $cur = $mongo->collection($c_name_times . '3')->find();
        while ($cur->hasNext()) {
            $v = $cur->getNext();
            $data_times[implode('||', $v['_id'])] = $v['value'];
        }
        unset($cur, $c_map_times, $c_reduce_times, $c_finalize_times);
        $mongo->collection($c_name_times)->drop();
        $mongo->collection($c_name_times . '2')->drop();
        $mongo->collection($c_name_times . '3')->drop();
        err_log("{$time}|pages|mongo|time|end","page");
        $countpv = count($data_times);
        err_log("{$time}|pages|mongo|count:{$countpv}","page");
        // 8、二跳量（PV=2的cookie的个数）
        $c_name_twojump = "dcclog{$time}_url_twojump";
        $ops_twojump = [
            ['$match' => ['dcv_1' => 2]],
            ['$group' => ['_id'	=> ['dcac' => '$dcac', 'provinceid' => '$provinceid', 'url_id' => '$url_id'], 'value'	=> ['$sum' => 1]]],
            ['$out'	  => $c_name_twojump]
        ];
        $mongo->collection($collection_name)->aggregate($ops_twojump, ['allowDiskUse' => true]);
        $cur = $mongo->collection($c_name_twojump)->find();
        while ($cur->hasNext()) {
            $v = $cur->getNext();
            $data_twojump[implode('||', $v['_id'])] = $v['value'];
        }
        unset($cur);
        $mongo->collection($c_name_twojump)->drop();
        err_log("{$time}|pages|mongo|jump|end","page");
        $countpv = count($data_twojump);
        err_log("{$time}|pages|mongo|count:{$countpv}","page");
        // 9、退出量
        $c_name_out = "dcclog{$time}_url_out";
        $ops_out = [
//            ['$match' => [ 'dcac' => ['$nin'=> $webids]]],
            ['$sort'  => ['dcv_1' => 1]],
            ['$group' => ['_id'	=> ['dcac' => '$dcac', 'provinceid' => '$provinceid', 'cookieid' => '$cookieid', 'dcv_0' => '$dcv_0'], 'url_id'	=> ['$last' => '$url_id']]],
            ['$group' => ['_id' => ['dcac' => '$_id.dcac', 'provinceid' => '$_id.provinceid', 'url_id' => '$url_id'], 'value' => ['$sum' => 1]]],
            ['$out'   => $c_name_out]
        ];
        $mongo->collection($collection_name)->aggregate($ops_out, ['allowDiskUse' => true]);
        $cur = $mongo->collection($c_name_out)->find();
        while ($cur->hasNext()) {
            $v = $cur->getNext();
            $data_out[implode('||', $v['_id'])] = $v['value'];
        }
        unset($cur);
        $mongo->collection($c_name_out)->drop();
        err_log("{$time}|pages|mongo|out|end","page");
        $countpv = count($data_out);
        err_log("{$time}|pages|mongo|count:{$countpv}","page");
        err_log("{$time}|pages|mongo|end|muse:".ceil(memory_get_usage()/(1024*1024))."M","page");

        //page全部数据
        if ($data_pv) {
            err_log("{$time}|pages|mysql|start","page");
            $db = db::getInstance($config['_mysqls']['dns_autotracking_data_page']);
            $time = date('Y-m-d', strtotime($time));

            $create_time = date('Y-m-d H:i:s');
            $nation_datas = [];
            $formatProvince = array();


            //分批十万条处理一次入库
            $pvcount = count($data_pv);
            $num = ceil($pvcount/100000);
            $flag5000 = [];

            for($i = 0; $i < $num; $i++)
            {
                $start = $i*100000;
                err_log("{$time}|pages|mysql|limit{$start}","page");
                $data_pv_slice = array_slice($data_pv, $start, 100000, true);
                $maxRow = count($data_pv_slice);
                $j = 1;
                foreach ($data_pv_slice as $k => $v) {
                    list($dcac, $provinceid, $url_id) = explode('||', $k, 4);
                    $dcac 				= (int) $dcac;
                    $provinceid 		= (int) $provinceid;
                    $url_id				= (int) $url_id;
                    $data_uv[$k] 		= isset($data_uv[$k]) ? $data_uv[$k] + 0 : 0;
                    $data_new_uv[$k] 	= isset($data_new_uv[$k]) ? $data_new_uv[$k] + 0 : 0;
                    $data_ip[$k] 		= isset($data_ip[$k]) ? $data_ip[$k] + 0 : 0;
                    $data_vv[$k] 		= isset($data_vv[$k]) ? $data_vv[$k] + 0 : 0;
                    $data_more_vv[$k]	= isset($data_more_vv[$k]) ? $data_more_vv[$k] + 0 : 0;
                    $data_times[$k] 	= isset($data_times[$k]) ? $data_times[$k] + 0 : 0;
                    $data_twojump[$k] 	= isset($data_twojump[$k]) ? $data_twojump[$k] + 0 : 0;
                    $data_out[$k] 		= isset($data_out[$k]) ? $data_out[$k] + 0 : 0;


                    if($data_new_uv[$k] > $data_uv[$k])
                    {
                        $data_new_uv[$k] = $data_uv[$k];
                    }

                    $sqlStr = "('{$time}', {$url_id}, {$v}, {$data_uv[$k]}, {$data_new_uv[$k]}, {$data_ip[$k]}, {$data_vv[$k]}, {$data_more_vv[$k]}, {$data_times[$k]}, {$data_twojump[$k]}, {$data_out[$k]}, '{$create_time}')";
                    $tableName = "auto_page_{$dcac}_{$provinceid}";
                    $formatProvince[$tableName][] = $sqlStr;

                    //防止数据过多 5000条一次处理
                    if(($j%5000) == 0 || $j == $maxRow)
                    {
                        foreach ($formatProvince as $table => $sqlArr)
                        {
                            $flag5000['flag'] = true;
                            err_log("{$table} 插入数据","page.process");
                            $exist = $db->getCol("SELECT table_name FROM information_schema.TABLES WHERE TABLE_NAME ='{$table}' and TABLE_SCHEMA = 'auto_tracking_data_page'");
                            if(!$exist)
                            {
                                err_log("{$table} not exists","page.notable");
                                continue;
                            }
                            if(!$sqlArr)
                            {
                                continue;
                            }
                            $sqls = join(',', $sqlArr);
                            $SQL_1 = "REPLACE INTO `{$table}` VALUES " .$sqls ;
//                            err_log("{$SQL_1}", "page.sql");
                            $db->query($SQL_1);

                            /**********************前5000条数据单独存放到一个数据表 start*****************************************/
                            $flag5000[$table] += count($sqlArr);

                            if(in_array($config_mongo['dbname'], ['dcclogs_163', 'dcclogs_10']))
                            {
                                if($flag5000[$table] > 5000 && $flag5000['flag'])
                                {
                                    $num_limit = count($sqlArr)+5000-$flag5000[$table];
                                    $sqlArr5000 = array_slice($sqlArr, 0, $num_limit);
                                    if(!$sqlArr5000){
                                        continue;
                                    }
                                    $sqls = join(',', $sqlArr5000);
                                    $SQL_1 = "REPLACE INTO `{$table}` VALUES " .$sqls ;
                                    $db_page24 = db::getInstance($config['_mysqls']['dns_autotracking_data_page_v24']);
                                    $db_page24->query($SQL_1);
                                    $flag5000['flag'] = false;
                                }

                                if($flag5000['flag'])
                                {
                                    $db_page24 = db::getInstance($config['_mysqls']['dns_autotracking_data_page_v24']);
                                    $db_page24->query($SQL_1);
                                }


                            }
                            $formatProvince[$table] = [];
                            unset($formatProvince[$table]);
                            /**********************前5000条数据单独存放到一个数据表 end*****************************************/
                        }
                    }
                    $j++;
                    $nation_datas[$dcac . '||' . $url_id]['pv'] += $v;
                    $nation_datas[$dcac . '||' . $url_id]['uv'] += $data_uv[$k];
                    $nation_datas[$dcac . '||' . $url_id]['new_uv'] += $data_new_uv[$k];
                    $nation_datas[$dcac . '||' . $url_id]['ip'] += $data_ip[$k];
                    $nation_datas[$dcac . '||' . $url_id]['vv'] += $data_vv[$k];
                    $nation_datas[$dcac . '||' . $url_id]['more_vv'] += $data_more_vv[$k];
                    $nation_datas[$dcac . '||' . $url_id]['time'] += $data_times[$k];
                    $nation_datas[$dcac . '||' . $url_id]['twojump'] += $data_twojump[$k];
                    $nation_datas[$dcac . '||' . $url_id]['out'] += $data_out[$k];
                }
            }

            err_log("{$time}|pages|mysql|全国数据开始","page");
            unset($j, $data_pv, $data_uv, $data_new_uv, $data_ip, $data_vv, $data_more_vv, $data_times, $data_twojump, $data_out);
            // 整合全国数据，并保存
            $h = 1;
            $newNations = array();
            $maxRow = count($nation_datas);
            foreach ($nation_datas as $_k => $_v) {
                list($_dcac, $_url_id) = explode('||', $_k, 2);
                $_dcac 		= (int) $_dcac;
                $_url_id	= (int) $_url_id;

                $nation_sqlStr = "( '{$time}',  {$_url_id}, {$_v['pv']}, {$_v['uv']}, {$_v['new_uv']}, {$_v['ip']}, {$_v['vv']}, {$_v['more_vv']}, {$_v['time']}, {$_v['twojump']}, {$_v['out']}, '{$create_time}')";
                $tableName_0 = "auto_page_{$_dcac}_0";
                $newNations[$tableName_0][] = $nation_sqlStr;

                if(($h%5000) == 0 || $h == $maxRow)
                {
                    err_log(" 全国数据数据 {$h}个 100000","page.process");
                    foreach ($newNations as $table => $sqlArr)
                    {
                        err_log("{$table} 插入数据","page.process");
                        if(strstr($table, '258'))
                        {
                            err_log("||auto_page_258_0||","ppppp");
                            err_log("||{$table}||","ppppp");
                            err_log("SELECT table_name FROM information_schema.TABLES WHERE TABLE_NAME ='{$table}' and TABLE_SCHEMA = 'auto_tracking_data_page'", "ppppp");
                        }
                        $exist = $db->getCol("SELECT table_name FROM information_schema.TABLES WHERE TABLE_NAME ='{$table}' and TABLE_SCHEMA = 'auto_tracking_data_page'");
                        if(!$exist)
                        {
                            err_log("{$table} not exists","page.notable");
                            continue;
                        }
                        if(!$sqlArr)
                        {
                            continue;
                        }
                        $sqls = join(',', $sqlArr);
                        $SQL_1 = "REPLACE INTO `{$table}` VALUES " .$sqls ;
                        /*********前5000条数据**********/
                        if(in_array($config_mongo['dbname'], ['dcclogs_163', 'dcclogs_10']) && $h == 5000)
                        {
                            $db_page24 = db::getInstance($config['_mysqls']['dns_autotracking_data_page_v24']);
                            $db_page24->query($SQL_1);
                        }
                        /**********前5000条数据*********/

                        //    err_log("{$SQL_1}", "page.sql");
                        $db->query($SQL_1);
                        $newNations[$table] = [];
                        unset($newNations[$table]);
                    }
                }
                $h++;
            }
            err_log("{$time}|pages|mysql|end|muse:".ceil(memory_get_usage()/(1024*1024))."M","page");
            err_log("{$time}|全结束","page");
            err_log("{$time}|pages|mysql|end","page");

            return true;
        } else {
            return false;
        }
    }

    public static  function create_page($t_name)
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

        return $sql;

    }
}


