<?php
// +----------------------------------------------------------------------
// | Copyright (c) 2016 All rights reserved.
// +----------------------------------------------------------------------
// | Author: Jerry.fang <fangzq@yiche.com>
// +----------------------------------------------------------------------

/**
 * 网站报告
 */
class DatasWebsites
{
	/**
	 * 处理数据【dcac, provinceid】
	 * @param  string $time [description]
	 * @return [type]       [description]
	 */
	public static function dodata($time = '', $config_mongo)
	{
		err_log("{$time}|dodata|websites_data|start", $config_mongo['dbname']);
		if (!check_time($time)) {
			return false;
		}

		// 配置信息
		$config = $_ENV['_db_configs'];

		$mongo = new mongodbdo($config_mongo);
		$collection_name = 'dcclog' . $time;
		
		err_log("{$time}|websites|mongo|start", $config_mongo['dbname']);

		// 1、PV
		$ops_pv = [
//			['$match' => ['dcac' => 256]],
			['$group' => ['_id'	=> ['dcac' => '$dcac', 'provinceid' => '$provinceid'], 'value'	=> ['$sum' => 1]]]
		];
		$result_pv = $mongo->collection($collection_name)->aggregate($ops_pv);
		err_log("{$time}|dodata|websites_data|{$collection_name}|result count:".count($result_pv)."|PV", $config_mongo['dbname']);

		if(!count($result_pv)){
			err_log("{$time}|dodata|websites_data|{$collection_name}|result null|exit", $config_mongo['dbname']);
			$url = "http://openapi.autodmp.com/jobkeeper/v1.0/jobtask/status/put?key=9b8619251a19057cff70779273e95aa6&group=Autotracking&job=".atFLAG."&lock=0";
			err_log("{$time}|jobkeeper|unlock|{$url}", $config_mongo['dbname']);
			file($url);
			exit();
		}
		foreach ($result_pv as $v) {
			$data_pv[implode('||', (array)$v->_id)] = $v->value;
		}

		// 2、独立访客UV
		$ops_uv = [
//			['$match' => ['dcac' => 256]],
			['$group' => ['_id'	=> ['dcac' => '$dcac', 'provinceid' => '$provinceid', 'cookieid' => '$cookieid']]],
			['$group' => ['_id'	=> ['dcac' => '$_id.dcac', 'provinceid' => '$_id.provinceid'], 'value' => ['$sum' => 1]]],
		];
		$result_uv = $mongo->collection($collection_name)->aggregate($ops_uv, true);
		err_log("{$time}|dodata|websites_data|{$collection_name}|result count:".count($result_uv)."|UV", $config_mongo['dbname']);
		foreach ($result_uv as $v) {
			$data_uv[implode('||', (array)$v->_id)] = $v->value;
		}

		// 3、首次访问UV
		$ops_new_uv = [
			//['$match' => ['dcv_0' => 1, 'dcv_1' => 1, 'dcac' => 256]],
			['$match' => ['dcv_0' => 1, 'dcv_1' => 1]],
            ['$group' => ['_id'	=> ['dcac' => '$dcac', 'provinceid' => '$provinceid', 'cookieid' => '$cookieid']]],
            ['$group' => ['_id'	=> ['dcac' => '$_id.dcac', 'provinceid' => '$_id.provinceid'], 'value' => ['$sum' => 1]]],
//			['$group' => ['_id'	=> ['dcac' => '$dcac', 'provinceid' => '$provinceid'], 'value'	=> ['$sum' => 1]]]
		];
		$result_new_uv = $mongo->collection($collection_name)->aggregate($ops_new_uv, true);
		err_log("{$time}|dodata|websites_data|{$collection_name}|result count:".count($result_new_uv)."|new_uv", $config_mongo['dbname']);
		foreach ($result_new_uv as $v) {
			$data_new_uv[implode('||', (array)$v->_id)] = $v->value;
		}

		// 4、独立IP
		$ops_ip = [
		//	['$match' => ['dcac' => 256]],
			['$group' => ['_id'	=> ['dcac' => '$dcac', 'provinceid' => '$provinceid', 'ip' => '$ip']]],
			['$group' => ['_id'	=> ['dcac' => '$_id.dcac', 'provinceid' => '$_id.provinceid'], 'value' => ['$sum' => 1]]],
		];
		$result_ip = $mongo->collection($collection_name)->aggregate($ops_ip, true);
		err_log("{$time}|dodata|websites_data|{$collection_name}|result count:".count($result_ip)."|IP", $config_mongo['dbname']);
		foreach ($result_ip as $v) {
			$data_ip[implode('||', (array)$v->_id)] = $v->value;
		}

		// 5、访次VV
		$ops_vv = [
		//	['$match' => ['dcac' => 256]],
			['$group' => ['_id'	=> ['dcac' => '$dcac', 'provinceid' => '$provinceid', 'cookieid' => '$cookieid', 'dcv_0' => '$dcv_0']]],
//			['$group' => ['_id'	=> ['dcac' => '$_id.dcac', 'provinceid' => '$_id.provinceid', 'cookieid' => '$_id.cookieid'], 'count' => ['$sum' => 1]]],
//			['$group' => ['_id'	=> ['dcac' => '$_id.dcac', 'provinceid' => '$_id.provinceid'], 'value' => ['$sum' => '$count']]],
			['$group' => ['_id'	=> ['dcac' => '$_id.dcac', 'provinceid' => '$_id.provinceid'], 'value' => ['$sum' => 1]]],
		];
		$result_vv = $mongo->collection($collection_name)->aggregate($ops_vv, true);
		err_log("{$time}|dodata|websites_data|{$collection_name}|result count:".count($result_vv)."|visit", $config_mongo['dbname']);
		foreach ($result_vv as $v) {
			$data_vv[implode('||', (array)$v->_id)] = $v->value;
		}

		// 6、多访次VV
		$ops_more_vv = [
		//	['$match' => ['dcac' => 256]],
			['$group' => ['_id'	=> ['dcac' => '$dcac', 'provinceid' => '$provinceid', 'cookieid' => '$cookieid', 'dcv_0' => '$dcv_0']]],
			['$group' => ['_id'	=> ['dcac' => '$_id.dcac', 'provinceid' => '$_id.provinceid', 'cookieid' => '$_id.cookieid'], 'count' => ['$sum' => 1]]],
			['$match' => ['count' => ['$gt' => 1]]],
			['$group' => ['_id'	=> ['dcac' => '$_id.dcac', 'provinceid' => '$_id.provinceid'], 'value' => ['$sum' => 1]]],
		];
		$result_more_vv = $mongo->collection($collection_name)->aggregate($ops_more_vv, true);
		err_log("{$time}|dodata|websites_data|{$collection_name}|result count:".count($result_more_vv)."|visit more", $config_mongo['dbname']);
		foreach ($result_more_vv as $v) {
			$data_more_vv[implode('||', (array)$v->_id)] = $v->value;
		}

		// 7、时长
		$ops_time = [
		//	['$match' => ['dcac' => 256]],
			['$group' => [
				'_id'	=> ['dcac' => '$dcac', 'provinceid' => '$provinceid', 'cookieid' => '$cookieid', 'dcv_0' => '$dcv_0'],
				'mintime' => ['$min' => '$time'],
				'maxtime' => ['$max' => '$time']
			]],
			['$group' => [
				'_id'	=> ['dcac' => '$_id.dcac', 'provinceid' => '$_id.provinceid', 'cookieid' => '$_id.cookieid', 'dcv_0' => '$_id.dcv_0'],
				'count' => ['$sum' => ['$subtract' => ['$maxtime', '$mintime']]]
			]],
			['$group' => [
				'_id'	=> ['dcac' => '$_id.dcac', 'provinceid' => '$_id.provinceid'],
				'value' => ['$sum' => '$count']
			]],
		];
		$result_time = $mongo->collection($collection_name)->aggregate($ops_time, true);
		err_log("{$time}|dodata|websites_data|{$collection_name}|result count:".count($result_time)."|time", $config_mongo['dbname']);
		foreach ($result_time as $v) {
			$data_time[implode('||', (array)$v->_id)] = $v->value;
		}

		// 8、二跳量（PV=2的cookie的个数）
		$ops_twojump = [
			//['$match' => ['dcv_1' => 2,'dcac' => 256]],
			['$match' => ['dcv_1' => 2]],
			['$group' => ['_id'	=> ['dcac' => '$dcac', 'provinceid' => '$provinceid'], 'value'	=> ['$sum' => 1]]]
		];
		$result_twojump = $mongo->collection($collection_name)->aggregate($ops_twojump, true);
		err_log("{$time}|dodata|websites_data|{$collection_name}|result count:".count($result_twojump)."|jump", $config_mongo['dbname']);
		foreach ($result_twojump as $v) {
			$data_twojump[implode('||', (array)$v->_id)] = $v->value;
		}
		err_log("{$time}|websites|mongo|end", $config_mongo['dbname']);

		if ($data_pv) {
			$db = db::getInstance($config['_mysqls']['dns_autotracking_data_t']);

			err_log("{$time}|websites|mysql|start", $config_mongo['dbname']);

			$time = date('Y-m-d', strtotime($time));
			$create_time = date('Y-m-d H:i:s');
			$sqlArr = $nation_datas = $nation_sqlArr = [];
			foreach ($data_pv as $k => $v) {
				list($dcac, $provinceid) = explode('||', $k, 2);
				$dcac 				= (int) $dcac;
				$provinceid 		= (int) $provinceid;
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


				$sqlArr[$dcac][] = "({$dcac}, '{$time}', {$provinceid}, {$v}, {$data_uv[$k]}, {$data_new_uv[$k]}, {$data_ip[$k]}, {$data_vv[$k]}, {$data_more_vv[$k]}, {$data_time[$k]}, {$data_twojump[$k]}, '{$create_time}')";
				// 用于全国数据的统计
				$nation_datas[$dcac]['pv'][] 		= $v;
				$nation_datas[$dcac]['uv'][] 		= $data_uv[$k];
				$nation_datas[$dcac]['new_uv'][]	= $data_new_uv[$k];
				$nation_datas[$dcac]['ip'][] 		= $data_ip[$k];
				$nation_datas[$dcac]['vv'][] 		= $data_vv[$k];
				$nation_datas[$dcac]['more_vv'][] 	= $data_more_vv[$k];
				$nation_datas[$dcac]['time'][] 		= $data_time[$k];
				$nation_datas[$dcac]['twojump'][]	= $data_twojump[$k];
			}
			// 整合全国数据
			foreach ($nation_datas as $_dcac => $_v) {
				$_pv 		= array_sum($_v['pv']);
				$_uv 		= array_sum($_v['uv']);
				$_new_uv	= array_sum($_v['new_uv']);
				$_ip 		= array_sum($_v['ip']);
				$_vv 		= array_sum($_v['vv']);
				$_more_vv 	= array_sum($_v['more_vv']);
				$_time 		= array_sum($_v['time']);
				$_twojump 	= array_sum($_v['twojump']);
                $sqlArr[$dcac][] = "({$_dcac}, '{$time}', 0, {$_pv}, {$_uv}, {$_new_uv}, {$_ip}, {$_vv}, {$_more_vv}, {$_time}, {$_twojump}, '{$create_time}')";
			}

            foreach($sqlArr as $web_id=>$sqls)
            {
                $SQL = "delete FROM `auto_report_web` WHERE  `web_id` = {$web_id} AND `time` = '{$time}'";
                err_log("{$time}|websites|{$SQL}", $config_mongo['dbname']);
                $db->query($SQL);

                $SQL = 'INSERT INTO `auto_report_web` VALUES '.implode(',', $sqls);
                err_log("{$time}|{$SQL}", "ATmongoPSDay.websites.sql");
                $db->query($SQL);
            }



			err_log("{$time}|websites|mysql|end", $config_mongo['dbname']);
			return true;
		} else {
			err_log("{$time}|websites|data_pv false", $config_mongo['dbname']);
			return false;
		}

		err_log("{$time}|websites|end", $config_mongo['dbname']);
	}

}


