<?php
// +----------------------------------------------------------------------
// | Copyright (c) 2016 All rights reserved.
// +----------------------------------------------------------------------
// | Author: Jerry.fang <fangzq@yiche.com>
// +----------------------------------------------------------------------

/**
 * 线索报告
 */
class DatasLeads
{
	/**
	 * 线索报告
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

		$mongo = new mongodbdo($config['_mongos']['dns_leads']);
		$collection_name = 'leads' . $time;

        // 2、精准带来的线索量
        $ops_dad = [
            ['$match' => ['rfpa_source' => ['$exists' => true]]],
            ['$group' => ['_id'	=> ['dcac' => '$dcac', 'varid' => '$varid'], 'value' => ['$sum' => 1]]]
        ];
        $result_dad = $mongo->collection($collection_name)->aggregate($ops_dad);
        foreach ($result_dad as $v) {
            $data_dad[implode('||', (array)$v->_id)] = $v->value;
        }

		// 1、线索量 和 成功支付量
		$ops_leads = [
			['$group' => ['_id'	=> ['dcac' => '$dcac', 'varid' => '$varid', 'codetype' => '$codetype'], 'value' => ['$sum' => 1]]]
		];
		$result_leads = $mongo->collection($collection_name)->aggregate($ops_leads);
		foreach ($result_leads as $v) {
			$data_leads[$v->_id->dcac . '||' . $v->_id->varid][$v->_id->codetype] = $v->value;
		}


        //3.新增字段 按cookeid和订单url去重
        $ops_leads_uv_url = [
            ['$group' => ['_id'	=> ['dcac' => '$dcac', 'varid' => '$varid', 'codetype' => '$codetype', 'cookieid' => '$cookieid', 'page' => '$page']]],
            ['$group' => ['_id'	=> ['dcac' => '$_id.dcac', 'varid' => '$_id.varid', 'codetype' => '$_id.codetype'], 'value' => ['$sum' => 1]]]
        ];
        $result_leads_uv_url = $mongo->collection($collection_name)->aggregate($ops_leads_uv_url);
        foreach ($result_leads_uv_url as $v) {
            $data_leads_uv_url[$v->_id->dcac . '||' . $v->_id->varid][$v->_id->codetype] = $v->value;
        }

        //3.新增字段 按cookeid
        $ops_leads_uv = [
            ['$group' => ['_id'	=> ['dcac' => '$dcac', 'varid' => '$varid', 'codetype' => '$codetype', 'cookieid' => '$cookieid']]],
            ['$group' => ['_id'	=> ['dcac' => '$_id.dcac', 'varid' => '$_id.varid', 'codetype' => '$_id.codetype'], 'value' => ['$sum' => 1]]]
        ];
        $result_leads_uv = $mongo->collection($collection_name)->aggregate($ops_leads_uv);
        foreach ($result_leads_uv as $v) {
            $data_leads_uv[$v->_id->dcac . '||' . $v->_id->varid][$v->_id->codetype] = $v->value;
        }



		if ($data_leads) {
			$db = db::getInstance($config['_mysqls']['dns_autotracking_data_t']);
			$time = date('Y-m-d', strtotime($time));
			$create_time = date('Y-m-d H:i:s');
			$sqlArr = [];
			foreach ($data_leads as $k => $v) {
				list($dcac, $varid) = explode('||', $k, 2);
				$dcac			= (int) $dcac;
				$varid			= (int) $varid;
				$leads 			= isset($v[1]) ? $v[1] : 0;					// 线索数
				$leads_uv 			= isset($data_leads_uv[$k][1]) ? $data_leads_uv[$k][1] : 0;					// 线索数
				$leads_uv_url 			= isset($data_leads_uv_url[$k][1]) ? $data_leads_uv_url[$k][1] : 0;					// 线索数
				$pays 			= isset($v[2]) ? $v[2] : 0;					// 成功数
				$pays_uv 			= isset($data_leads_uv[$k][2]) ? $data_leads_uv[$k][2] : 0;					// 成功数
				$pays_uv_url 			= isset($data_leads_uv_url[$k][2]) ? $data_leads_uv_url[$k][2] : 0;					// 成功数
				$data_dad[$k]	= isset($data_dad[$k]) ? $data_dad[$k] : 0;	// 精准带来的线索数

				$sqlArr[] = "('{$time}', {$varid}, {$dcac}, {$leads_uv_url}, {$data_dad[$k]}, {$pays_uv_url}, {$leads_uv}, {$pays_uv}, {$leads}, {$pays}, '{$create_time}')";
			}
			
			$SQL = "delete FROM `auto_report_events_leads` WHERE `time` = '{$time}'";
			err_log("{$time}|{$SQL}", atFLAG);
			$db->query($SQL);

			$SQL = implode(',', $sqlArr);
//            echo $SQL;
			$db->query("INSERT INTO `auto_report_events_leads` VALUES {$SQL}");

			err_log("leads_data success: {$time}");
			return true;
		} else {
			return false;
		}
	}
}

