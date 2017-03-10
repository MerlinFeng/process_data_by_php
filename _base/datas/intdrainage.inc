<?php
// +----------------------------------------------------------------------
// | Copyright (c) 2016 All rights reserved.
// +----------------------------------------------------------------------
// | Author: Jerry.fang <fangzq@yiche.com>
// +----------------------------------------------------------------------

/**
 * 内部引流报告 最终来源
 */
class DatasIntdrainage
{
	/**
	 * 内部引流报告 最终来源
	 * @param  string $time [description]
	 * @return [type]       [description]
	 */
	public static function dodata($time = '', $config_mongo)
	{
		if (!check_time($time)) {
			return false;
		}

		// 配置信息
		$config = $_ENV['_db_configs'];

		$mongo = new mongodbdo($config_mongo);
		$collection_name = 'dcclog' . $time;

		// 1、PV
		$ops_pv = [
			['$match' => ['dcin' => ['$exists' => true]]],
			['$group' => ['_id'	=> ['dcac' => '$dcac', 'dcin' => '$dcin'], 'value'	=> ['$sum' => 1]]]
		];
		$result_pv = $mongo->collection($collection_name)->aggregate($ops_pv);
		foreach ($result_pv as $v) {
			$data_pv[implode('||', (array)$v->_id)] = $v->value;
		}

		// 2、独立访客UV
		$ops_uv = [
			['$match' => ['dcin' => ['$exists' => true]]],
			['$group' => ['_id'	=> ['dcac' => '$dcac', 'dcin' => '$dcin', 'cookieid' => '$cookieid']]],
			['$group' => ['_id'	=> ['dcac' => '$_id.dcac', 'dcin' => '$_id.dcin'], 'value' => ['$sum' => 1]]],
		];
		$result_uv = $mongo->collection($collection_name)->aggregate($ops_uv, true);
		foreach ($result_uv as $v) {
			$data_uv[implode('||', (array)$v->_id)] = $v->value;
		}

		// 3、访次VV
		$ops_vv = [
			['$match' => ['dcin' => ['$exists' => true]]],
			['$group' => ['_id'	=> ['dcac' => '$dcac', 'dcin' => '$dcin', 'cookieid' => '$cookieid', 'dcv_0' => '$dcv_0']]],
			['$group' => ['_id'	=> ['dcac' => '$_id.dcac', 'dcin' => '$_id.dcin', 'cookieid' => '$_id.cookieid'], 'count' => ['$sum' => 1]]],
			['$group' => ['_id'	=> ['dcac' => '$_id.dcac', 'dcin' => '$_id.dcin'], 'value' => ['$sum' => '$count']]],
		];
		$result_vv = $mongo->collection($collection_name)->aggregate($ops_vv, true);
		foreach ($result_vv as $v) {
			$data_vv[implode('||', (array)$v->_id)] = $v->value;
		}

		// 4、二跳量（PV=2的cookie的个数）
		$ops_twojump = [
			['$match' => ['dcv_1' => 2, 'dcin' => ['$exists' => true]]],
			['$group' => ['_id'	=> ['dcac' => '$dcac', 'dcin' => '$dcin'], 'value'	=> ['$sum' => 1]]]
		];
		$result_twojump = $mongo->collection($collection_name)->aggregate($ops_twojump, true);
		foreach ($result_twojump as $v) {
			$data_twojump[implode('||', (array)$v->_id)] = $v->value;
		}

		if ($data_pv) {
			// 4、线索数
			$mongo_leads = new mongodbdo($config['_mongos']['dns_leads']);
			$collection_name_leads = 'leads' . $time;
			$ops_leads = [
				['$match' => ['dcin' => ['$exists' => true]]],
				['$group' => ['_id'	=> ['dcac' => '$dcac', 'dcin' => '$dcin', 'codetype' => '$codetype'], 'value' => ['$sum' => 1]]]
			];
			$result_leads = $mongo_leads->collection($collection_name_leads)->aggregate($ops_leads);
			foreach ($result_leads as $v) {
				$data_leads[$v->_id->dcac . '||' . $v->_id->dcin][$v->_id->codetype] = $v->value;
			}

            // 5、线索数 uv

            $ops_leads_uv = [
                ['$match' => ['dcin' => ['$exists' => true]]],
                ['$group' => ['_id'	=> ['dcac' => '$dcac', 'dcin' => '$dcin', 'codetype' => '$codetype','cookieid' => '$cookieid']]],
                ['$group' => ['_id'	=> ['dcac' => '$_id.dcac', 'dcin' => '$_id.dcin', 'codetype' => '$_id.codetype'], 'value' => ['$sum' => 1]]]
            ];
            $result_leads_uv = $mongo_leads->collection($collection_name_leads)->aggregate($ops_leads_uv);
            foreach ($result_leads_uv as $v) {
                $data_leads_uv[$v->_id->dcac . '||' . $v->_id->dcin][$v->_id->codetype] = $v->value;
            }

            //6、线索数 cookieid url去重
            $ops_leads_url = [
                ['$match' => ['dcin' => ['$exists' => true]]],
                ['$group' => ['_id'	=> ['dcac' => '$dcac', 'dcin' => '$dcin', 'codetype' => '$codetype','cookieid' => '$cookieid', 'page' => '$page']]],
                ['$group' => ['_id'	=> ['dcac' => '$_id.dcac', 'dcin' => '$_id.dcin', 'codetype' => '$_id.codetype'], 'value' => ['$sum' => 1]]]
            ];
            $result_leads_url = $mongo_leads->collection($collection_name_leads)->aggregate($ops_leads_url);
            foreach ($result_leads_url as $v) {
                $data_leads_url[$v->_id->dcac . '||' . $v->_id->dcin][$v->_id->codetype] = $v->value;
            }


			$db = db::getInstance($config['_mysqls']['dns_autotracking_data_t']);
			$time = date('Y-m-d', strtotime($time));
			$create_time = date('Y-m-d H:i:s');
			$sqlArr = [];
			$db1 = db::getInstance($config['_mysqls']['dns_autotracking']);
			foreach ($data_pv as $k => $v) {
                list($dcac, $dcin) = explode('||', $k, 2);
                $dcac               = (int) $dcac;
                $cu = 0;

                $tmp_arr = explode('_', $dcin);
                if(count($tmp_arr) == 2)
                {
                    list($media, $ad) = $tmp_arr;
                }
                else
                {
                    list($cu,$media, $ad) = $tmp_arr;
                }

                $media              = (int) $media;
                $ad                 = (int) $ad;
                if(!$cu)
                {
                    $cus = $db1->getCol("select cu from auto_cu_pc_mt_trace where web_id = {$dcac} and pc = {$media} and mt = {$ad} AND ismoved = 0");
                    $cu                 = $cus[0]?(int)$cus[0]:1;//新加字段
                }

				$data_uv[$k] 		= isset($data_uv[$k]) ? $data_uv[$k] + 0 : 0;
				$data_vv[$k] 		= isset($data_vv[$k]) ? $data_vv[$k] + 0 : 0;
				$data_twojump[$k] 	= isset($data_twojump[$k]) ? $data_twojump[$k] + 0 : 0;
                $leads				= isset($data_leads[$k][1]) ? $data_leads[$k][1] + 0 : 0;
                $leads_uv				= isset($data_leads_uv[$k][1]) ? $data_leads_uv[$k][1] + 0 : 0;
                $leads_uv_url				= isset($data_leads_url[$k][1]) ? $data_leads_url[$k][1] + 0 : 0;
                $pays				= isset($data_leads[$k][2]) ? $data_leads[$k][2] + 0 : 0;
                $pays_uv				= isset($data_leads_uv[$k][2]) ? $data_leads_uv[$k][2] + 0 : 0;
                $pays_uv_url				= isset($data_leads_url[$k][2]) ? $data_leads_url[$k][2] + 0 : 0;
                $sqlArr[$dcac][] = "({$dcac}, '{$time}', {$cu},{$media}, {$ad}, {$v}, {$data_uv[$k]}, {$data_vv[$k]}, {$data_twojump[$k]}, {$leads_uv_url}, {$pays_uv_url}, {$leads_uv}, {$pays_uv},{$leads}, {$pays}, '{$create_time}')";
            }
            foreach($sqlArr as $web_id=>$sqls)
            {
                $SQL = "delete FROM `auto_report_in_drain` WHERE web_id = {$web_id} AND `time` = '{$time}'";
                err_log("{$time}|{$SQL}", $config_mongo['dbname']);
                $db->query($SQL);

                $SQL = implode(',', $sqls);
                $db->query("INSERT INTO `auto_report_in_drain` VALUES {$SQL}");
            }


			err_log("intdrainage_data success: {$time}", $config_mongo['dbname']);
			return true;
		} else {
			return false;
		}
	}
}

