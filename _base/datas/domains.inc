<?php
// +----------------------------------------------------------------------
// | Copyright (c) 2016 All rights reserved.
// +----------------------------------------------------------------------
// | *************
// +----------------------------------------------------------------------

/**
 * 网站域名报告
 */
class DatasDomains
{
	/**
	 * 处理数据【dcac, domain_id, provinceid】
	 * @param  string $time [description]
	 * @return [type]       [description]
	 */
	public static function  dodata($time = '', $config_mongo)
	{
		if (!check_time($time)) {
			return false;
		}

		// 配置信息
		$config = $_ENV['_db_configs'];

		$mongo = new mongodbdo($config_mongo);
		$collection_name = 'dcclog' . $time;
		
		err_log("{$time}|domain|mongo|start", $config_mongo['dbname']);
        err_log("domain|pv|start", $config_mongo['dbname']);
		// 1、PV
		$ops_pv = [
			['$group' => ['_id'	=> ['dcac' => '$dcac', 'domain_id' => '$domain_id', 'provinceid' => '$provinceid'], 'value'	=> ['$sum' => 1]]]
		];
		$result_pv = $mongo->collection($collection_name)->aggregate($ops_pv);
		foreach ($result_pv as $v) {
			$data_pv[implode('||', (array)$v->_id)] = $v->value;
		}
        err_log("domain|pv|end", $config_mongo['dbname']);

		// 2、独立访客UV
		$ops_uv = [
			['$group' => ['_id'	=> ['dcac' => '$dcac', 'domain_id' => '$domain_id', 'provinceid' => '$provinceid', 'cookieid' => '$cookieid']]],
			['$group' => ['_id'	=> ['dcac' => '$_id.dcac', 'domain_id' => '$_id.domain_id', 'provinceid' => '$_id.provinceid'], 'value' => ['$sum' => 1]]],
		];
		$result_uv = $mongo->collection($collection_name)->aggregate($ops_uv,  true);
		foreach ($result_uv as $v) {
			$data_uv[implode('||', (array)$v->_id)] = $v->value;
		}
        err_log("domain|独立访客UV|end", $config_mongo['dbname']);

		// 3、首次访问UV
		$ops_new_uv = [
			['$match' => ['dcv_0' => 1, 'dcv_1' => 1]],
            ['$group' => ['_id'	=> ['dcac' => '$dcac', 'domain_id' => '$domain_id', 'provinceid' => '$provinceid', 'cookieid' => '$cookieid']]],
            ['$group' => ['_id'	=> ['dcac' => '$_id.dcac', 'domain_id' => '$_id.domain_id', 'provinceid' => '$_id.provinceid'], 'value' => ['$sum' => 1]]],
//            ['$group' => ['_id'	=> ['dcac' => '$dcac', 'domain_id' => '$domain_id', 'provinceid' => '$provinceid'], 'value'	=> ['$sum' => 1]]]
		];
		$result_new_uv = $mongo->collection($collection_name)->aggregate($ops_new_uv,  true);
		foreach ($result_new_uv as $v) {
			$data_new_uv[implode('||', (array)$v->_id)] = $v->value;
		}
        err_log("domain|首次访问UV|end", $config_mongo['dbname']);

		// 4、独立IP
		$ops_ip = [
			['$group' => ['_id'	=> ['dcac' => '$dcac', 'domain_id' => '$domain_id', 'provinceid' => '$provinceid', 'ip' => '$ip']]],
			['$group' => ['_id'	=> ['dcac' => '$_id.dcac', 'domain_id' => '$_id.domain_id', 'provinceid' => '$_id.provinceid'], 'value' => ['$sum' => 1]]],
		];
		$result_ip = $mongo->collection($collection_name)->aggregate($ops_ip, true);
		foreach ($result_ip as $v) {
			$data_ip[implode('||', (array)$v->_id)] = $v->value;
		}
        err_log("domain|独立IP|end", $config_mongo['dbname']);
		// 5、访次VV
		$ops_vv = [
			['$group' => ['_id'	=> ['dcac' => '$dcac', 'domain_id' => '$domain_id', 'provinceid' => '$provinceid', 'cookieid' => '$cookieid', 'dcv_0' => '$dcv_0']]],
			['$group' => ['_id'	=> ['dcac' => '$_id.dcac', 'domain_id' => '$_id.domain_id', 'provinceid' => '$_id.provinceid', 'cookieid' => '$_id.cookieid'], 'count' => ['$sum' => 1]]],
			['$group' => ['_id'	=> ['dcac' => '$_id.dcac', 'domain_id' => '$_id.domain_id', 'provinceid' => '$_id.provinceid'], 'value' => ['$sum' => '$count']]],
		];
		$result_vv = $mongo->collection($collection_name)->aggregate($ops_vv, true);
		foreach ($result_vv as $v) {
			$data_vv[implode('||', (array)$v->_id)] = $v->value;
		}
        err_log("domain|访次VV|end", $config_mongo['dbname']);

		// 6、访次VV
		$ops_more_vv = [
			['$group' => ['_id'	=> ['dcac' => '$dcac', 'domain_id' => '$domain_id', 'provinceid' => '$provinceid', 'cookieid' => '$cookieid', 'dcv_0' => '$dcv_0']]],
			['$group' => ['_id'	=> ['dcac' => '$_id.dcac', 'domain_id' => '$_id.domain_id', 'provinceid' => '$_id.provinceid', 'cookieid' => '$_id.cookieid'], 'count' => ['$sum' => 1]]],
			['$match' => ['count' => ['$gt' => 1]]],
			['$group' => ['_id'	=> ['dcac' => '$_id.dcac', 'domain_id' => '$_id.domain_id', 'provinceid' => '$_id.provinceid'], 'value' => ['$sum' => 1]]],
		];
		$result_more_vv = $mongo->collection($collection_name)->aggregate($ops_more_vv, true);
		foreach ($result_more_vv as $v) {
			$data_more_vv[implode('||', (array)$v->_id)] = $v->value;
		}
        err_log("domain|多访次VV|end", $config_mongo['dbname']);

		// 7、时长
		$ops_time = [
			['$group' => [
				'_id'	=> ['dcac' => '$dcac', 'domain_id' => '$domain_id', 'provinceid' => '$provinceid', 'cookieid' => '$cookieid', 'dcv_0' => '$dcv_0'],
				'mintime' => ['$min' => '$time'],
				'maxtime' => ['$max' => '$time']
			]],
			['$group' => [
				'_id'	=> ['dcac' => '$_id.dcac', 'domain_id' => '$_id.domain_id', 'provinceid' => '$_id.provinceid', 'cookieid' => '$_id.cookieid', 'dcv_0' => '$_id.dcv_0'],
				'count' => ['$sum' => ['$subtract' => ['$maxtime', '$mintime']]]
			]],
			['$group' => [
				'_id'	=> ['dcac' => '$_id.dcac', 'domain_id' => '$_id.domain_id', 'provinceid' => '$_id.provinceid'],
				'value' => ['$sum' => '$count']
			]],
		];
		$result_time = $mongo->collection($collection_name)->aggregate($ops_time, true);
		foreach ($result_time as $v) {
			$data_time[implode('||', (array)$v->_id)] = $v->value;
		}
        err_log("domain|时长|end", $config_mongo['dbname']);

		// 8、二跳量（PV=2的cookie的个数）
		$ops_twojump = [
			['$match' => ['dcv_1' => 2]],
			['$group' => ['_id'	=> ['dcac' => '$dcac', 'domain_id' => '$domain_id', 'provinceid' => '$provinceid'], 'value'	=> ['$sum' => 1]]]
		];
		$result_twojump = $mongo->collection($collection_name)->aggregate($ops_twojump, true);
		foreach ($result_twojump as $v) {
			$data_twojump[implode('||', (array)$v->_id)] = $v->value;
		}
        err_log("domain|二跳量|end", $config_mongo['dbname']);
		err_log("{$time}|domain|mongo|end", atFLAG);

		if ($data_pv) {
			err_log("{$time}|domain|mysql|start", atFLAG);
			$db = db::getInstance($config['_mysqls']['dns_autotracking_data_domain']);

			$time = date('Y-m-d', strtotime($time));
			$create_time = date('Y-m-d H:i:s');
			$sqlArr = $nation_datas = $nation_sqlArr = [];
            $formatProvince = array();
            $formatChina = array();
			foreach ($data_pv as $k => $v) {
				list($dcac, $domain_id, $provinceid) = explode('||', $k, 3);
				$dcac 				= (int) $dcac;
				$provinceid 		= (int) $provinceid;
				$domain_id			= (int) $domain_id;
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

                $sqlArr = "('{$time}', {$domain_id}, {$v}, {$data_uv[$k]}, {$data_new_uv[$k]}, {$data_ip[$k]}, {$data_vv[$k]}, {$data_more_vv[$k]}, {$data_time[$k]}, {$data_twojump[$k]}, '{$create_time}')";
                $tableName = "auto_domain_{$dcac}_{$provinceid}";
                $formatProvince[$tableName][] = $sqlArr;

                // 用于全国数据的统计
				$nation_datas[$dcac . '||' . $domain_id]['pv'][] 		= $v;
				$nation_datas[$dcac . '||' . $domain_id]['uv'][] 		= $data_uv[$k];
				$nation_datas[$dcac . '||' . $domain_id]['new_uv'][]	= $data_new_uv[$k];
				$nation_datas[$dcac . '||' . $domain_id]['ip'][] 		= $data_ip[$k];
				$nation_datas[$dcac . '||' . $domain_id]['vv'][] 		= $data_vv[$k];
				$nation_datas[$dcac . '||' . $domain_id]['more_vv'][] 	= $data_more_vv[$k];
				$nation_datas[$dcac . '||' . $domain_id]['time'][] 		= $data_time[$k];
				$nation_datas[$dcac . '||' . $domain_id]['twojump'][]	= $data_twojump[$k];
			}
			
			// 整合全国数据
			foreach ($nation_datas as $_k => $_v) {
				list($_dcac, $_domain_id) = explode('||', $_k, 2);
				$_dcac 		= (int) $_dcac;
				$_domain_id	= (int) $_domain_id;

				$_pv 		= array_sum($_v['pv']);
				$_uv 		= array_sum($_v['uv']);
				$_new_uv	= array_sum($_v['new_uv']);
				$_ip 		= array_sum($_v['ip']);
				$_vv 		= array_sum($_v['vv']);
				$_more_vv 	= array_sum($_v['more_vv']);
				$_time 		= array_sum($_v['time']);
				$_twojump 	= array_sum($_v['twojump']);

                $nation_sqlArr = "('{$time}',{$_domain_id}, {$_pv}, {$_uv}, {$_new_uv}, {$_ip}, {$_vv}, {$_more_vv}, {$_time}, {$_twojump}, '{$create_time}')";
                $tableName = "auto_domain_{$_dcac}_0";
                $formatChina[$tableName][] = $nation_sqlArr;

			}

            $formatAll = array_merge($formatChina, $formatProvince);


            //插入省份数据
            foreach($formatAll as $table=>$sqls)
            {
                err_log("{$table} 插入数据",$config_mongo['dbname']);
                $exist = $db->getCol("SELECT table_name FROM information_schema.TABLES WHERE TABLE_NAME ='{$table}' and TABLE_SCHEMA = 't_auto_tracking_data_domain'");
                if(!$exist)
                {
                    err_log("{$table} not exists", "domain.notable");
                    continue;
                }
                if(!$sqlArr)
                {
                    continue;
                }
                //插入数据之前先删除当天数据
                $SQL = "delete FROM `{$table}` WHERE `time` = '{$time}'";
                err_log("{$time}|domain|mysql|{$SQL}", $config_mongo['dbname']);
                $db->query($SQL);

                //分批插入 数量过多 mysql 失去连接
                $sql_num = count($sqls);
                if($sql_num>600) //二百条插入一次
                {
                    $limit_num = ceil($sql_num/600);
                    for ($k = 0; $k < $limit_num; $k++)
                    {
                        $insert_sqls = array_slice($sqls,$k * 600, 600);
                        $insert_sqls = implode(',', $sqls);
                        $SQL_1 = "INSERT INTO `{$table}` VALUES " .$insert_sqls ;
                        $db->query($SQL_1);
                    }
                }
                else
                {
                    $sqls = implode(',', $sqls);
                    $SQL_1 = "INSERT INTO `{$table}` VALUES " .$sqls ;
                    $db->query($SQL_1);
                }

//                err_log("{$time}|domain|mysql|{$SQL}", atFLAG.'.sql');
            }

			err_log("{$time}|domain|mysql|end", $config_mongo['dbname']);
			return true;
		} else {
			return false;
		}
	}



    /**
     * 创建domain表
     */
    public static function create_domain($t_name)
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
}

