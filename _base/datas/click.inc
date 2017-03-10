<?php
// +----------------------------------------------------------------------
// | Copyright (c) 2016 All rights reserved.
// +----------------------------------------------------------------------
// | *************
// +----------------------------------------------------------------------

/**
 * 点击报告
 */
class DatasClick
{
    /**
     * 点击报告
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

        $mongo_im = new mongodbdo($config['_mongos']['dns_impression']);
        $collection_name_im = 'impression' . $time;

        // 1、曝光量
        $ops_im = [
            ['$group' => ['_id'	=> ['cu' => '$cu', 'pc' => '$pc', 'mt' => '$mt'], 'value'	=> ['$sum' => 1]]]
        ];
        $result_im = $mongo_im->collection($collection_name_im)->aggregate($ops_im, true);
        foreach ($result_im as $v) {
            $data_im[implode('_', (array)$v->_id)] = $v->value;
        }

        $mongo = new mongodbdo($config['_mongos']['dns_click']);
        $collection_name = 'click' . $time;
        // 2、点击量
        $ops_click = [
            ['$group' => ['_id'	=> ['cu' => '$cu', 'pc' => '$pc', 'mt' => '$mt'], 'value'	=> ['$sum' => 1]]]
        ];
        $result_click = $mongo->collection($collection_name)->aggregate($ops_click, true);
        foreach ($result_click as $v) {
            $data_click[implode('_', (array)$v->_id)] = $v->value;
        }

        if ($data_click) {

            /*****************线索数调整从click日志读取********************/

            //total
            $mongo_leads = new mongodbdo($config['_mongos']['dns_leads']);
            $collection_name_leads = 'leads' . $time;
            $ops_leads = [
                ['$group' => ['_id'	=> ['dccta_p' => '$dccta_p', 'codetype' => '$codetype'], 'value'	=> ['$sum' => 1]]]
            ];
            $result_leads = $mongo_leads->collection($collection_name_leads)->aggregate($ops_leads, true);
            foreach ($result_leads as $v) {
                $data_leads[$v->_id->dccta_p][$v->_id->codetype] = $v->value;
            }


            //cookieid 去重
            $ops_leads_uv = [
                ['$group' => ['_id'	=> ['dccta_p' => '$dccta_p', 'codetype' => '$codetype','cookieid' => '$cookieid']]],
                ['$group' => ['_id'	=> ['dccta_p' => '$_id.dccta_p', 'codetype' => '$_id.codetype'], 'value'	=> ['$sum' => 1]]]
            ];
            $result_leads_uv = $mongo_leads->collection($collection_name_leads)->aggregate($ops_leads_uv, true);
            foreach ($result_leads_uv as $v) {
                $data_leads_uv[$v->_id->dccta_p][$v->_id->codetype] = $v->value;
            }

            // cookieid url 去重
            $ops_leads_uv_url = [
                ['$group' => ['_id'	=> ['dccta_p' => '$dccta_p', 'codetype' => '$codetype','cookieid' => '$cookieid', 'page' => '$page']]],
                ['$group' => ['_id'	=> ['dccta_p' => '$_id.dccta_p', 'codetype' => '$_id.codetype'], 'value'	=> ['$sum' => 1]]]
            ];
            $result_leads_uv_url = $mongo_leads->collection($collection_name_leads)->aggregate($ops_leads_uv_url, true);
            foreach ($result_leads_uv_url as $v) {
                $data_leads_uv_url[$v->_id->dccta_p][$v->_id->codetype] = $v->value;
            }

            /*****************线索数调整不按全部从click日志读取结束********************/

            // 将数据入到mysql库
            $db = db::getInstance($config['_mysqls']['dns_autotracking_data_t']);
            $db2 = db::getInstance($config['_mysqls']['dns_autotracking']);
            $time = date('Y-m-d', strtotime($time));
            $create_time = date('Y-m-d H:i:s');
            $sqlArr = [];
            foreach ($data_click as $_k => $_v) {
                list($cu, $pc, $mt) = explode('_', $_k, 3);
                $cu = (int) $cu;
                $pc = (int) $pc;
                $mt = (int) $mt;
                $dcac = $db2->getCol("select web_id from auto_item_click WHERE id = {$cu}");
                $dcac = intval($dcac[0]);
                if(!$dcac)
                {
                    continue;
                }

                $im_total 	 = isset($data_im[$_k]) ? $data_im[$_k] : 0;
                $click_total = $_v;
                $leads_total = isset($data_leads[$_k][1]) ? $data_leads[$_k][1] : 0;
                $pay_total 	 = isset($data_leads[$_k][2]) ? $data_leads[$_k][2] : 0;
                $leads_uv = isset($data_leads_uv[$_k][1]) ? $data_leads_uv[$_k][1] : 0;
                $pay_uv 	 = isset($data_leads_uv[$_k][2]) ? $data_leads_uv[$_k][2] : 0;
                $leads_uv_url = isset($data_leads_uv_url[$_k][1]) ? $data_leads_uv_url[$_k][1] : 0;
                $pay_uv_url 	 = isset($data_leads_uv_url[$_k][2]) ? $data_leads_uv_url[$_k][2] : 0;
                $sqlArr[] = "('{$time}', {$dcac}, {$cu}, {$pc}, {$mt}, {$im_total}, {$click_total}, {$leads_uv_url}, {$pay_uv_url},{$leads_uv},{$pay_uv},{$leads_total},{$pay_total}, '{$create_time}')";
            }

            $SQL = "delete FROM `auto_report_click` WHERE `time` = '{$time}'";
            err_log("{$time}|{$SQL}", atFLAG);
            $db->query($SQL);

            $SQL = implode(',', $sqlArr);
            $db->query("INSERT INTO `auto_report_click` VALUES {$SQL}");

            err_log("click_data success: {$time}");
            return true;
        } else {
            return false;
        }
    }
}

?>

