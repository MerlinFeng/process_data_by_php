<?php
// +----------------------------------------------------------------------
// | Copyright (c) 2016 All rights reserved.
// +----------------------------------------------------------------------
// | Author: Jerry.fang <fangzq@yiche.com>
// +----------------------------------------------------------------------

/**
 * 点击日志 清洗后入库到mongodb
 */
class FilesClicks{
	
	public static function mongoImport($time = '', $log, $config_mongos){
		$rtime = $time . '0000';
		$unix_rtime = strtotime($rtime);
        $day_time = date("Ymd", $unix_rtime);
		$_collect_name = $log . $day_time;
		
		//mongo init
		$_file_name =  $log. $time;
		$mongo = new mongodbdo($config_mongos);

		//collection drop
		$dataSizeS = $mongo->collection($_collect_name)->count();
        if($dataSizeS>0)
        {
//            $result_pv = $mongo->collection($_collect_name)->drop();
        }
		$dataSizeE = $mongo->collection($_collect_name)->count();
		err_log("{$time}|{$_collect_name}|{$_collect_name}|drop|{$dataSizeS}-{$dataSizeE}", atFLAG);
        if(in_array($log, ['dcclog', 'event']))
        {
            // json文件操作
            $dccJsonDir = LOCAL_JOSN_DIR . "dcclog/";

            //shell 合并文件
            if($log == 'dcclog')
            {
                $jsonFile = "{$dccJsonDir}{$_file_name}.{$config_mongos['web_id']}.json";
                $cmdcat = "cat {$dccJsonDir}{$_file_name}.{$config_mongos['web_id']}.*.json >> {$jsonFile}";
                exec($cmdcat);
                exec("rm -rf {$dccJsonDir}{$_file_name}.{$config_mongos['web_id']}.*.json");
            }
            else
            {
                $jsonFile = "{$dccJsonDir}{$_file_name}.json";
                $cmdcat = "cat {$dccJsonDir}{$_file_name}.*.json >> {$jsonFile}";
                err_log("{$cmdcat}", atFLAG);
                exec($cmdcat);
                err_log("rm -rf {$dccJsonDir}{$_file_name}.*.json", atFLAG);
                exec("rm -rf {$dccJsonDir}{$_file_name}.*.json");
            }
            $cmd  = "/usr/local/mongodb/bin/mongoimport -h {$config_mongos['host']} -d {$config_mongos['dbname']} -c {$_collect_name}  --file {$jsonFile}";
            err_log("{$time}|{$_collect_name}|{$cmd}|start", atFLAG);
            exec($cmd);
            $dataSizeS = $mongo->collection($_collect_name)->count();
            err_log("{$time}|{$_collect_name}|{$cmd}|end|count:{$dataSizeS}", atFLAG);
            unlink($jsonFile);
        }
        else
        {
            $_jsonfile = LOCAL_JOSN_DIR . $_file_name . '.json';
            // json文件操作
            $cmd  = "/usr/local/mongodb/bin/mongoimport -h {$config_mongos['host']} -d {$config_mongos['dbname']} -c {$_collect_name}  --file {$_jsonfile}";
            err_log("{$time}|{$_collect_name}|{$cmd}|start", atFLAG);
            exec($cmd);
            $dataSizeS = $mongo->collection($_collect_name)->count();
            err_log("{$time}|{$_collect_name}|{$cmd}|end|count:{$dataSizeS}", atFLAG);
            unlink($_jsonfile);
//            err_log("{$time}|{$_collect_name}|unlink|{$_jsonfile}", atFLAG);
        }
		//err_log("{$time}|{$_collect_name}|rtime:{$rtime}|unix_rtime:{$unix_rtime}|end", atFLAG);
		return true;
	}




	/**
	 * 处理1个小时的文件
	 * @param  string $time [description]
	 * @return [type]       [description]
	 */
	public static function dofiles_hour($time = '')
	{
		$rtime = $time . '0000';
		if (!check_time($rtime, 'YmdHis')) {
			return false;
		}
		$unix_rtime = strtotime($rtime);

		err_log("clicks_file start success: ======", atFLAG);

		// 配置信息
		$config = $_ENV['_db_configs'];

		// 日志配置
		$log_setting = $_ENV['_log_setting'];

		// 获取所有开通报告的网站ID
		$report_w_ids = get_report_w_ids($rtime);

		// 下载一个小时的文件
		$file_name = $log_setting['click']['file_prefix'] . '.' . $time . '.gz';
		$file_remote_fs = $log_setting['click']['file_dir'] . date('Y/m/', $unix_rtime) . $file_name;

		$file_local_dir = LOCAL_LOG_DIR . 'click' . date('/Y/m/d/', $unix_rtime);
		exec("mkdir -p {$file_local_dir}");
		exec("scp -P10001 " . LOG_SERVER_IP . ":{$file_remote_fs} {$file_local_dir}");
		err_log("clicks_file down success: {$file_name}", atFLAG);

		$_collect_name = 'click' . date('Ymd', $unix_rtime);
		$_collect_nameHour = 'click' . date('YmdH', $unix_rtime);
		
		// 打开json文件
		$_jsonfile = LOCAL_JOSN_DIR . $_collect_name . '.json';
		$_jsonfileHour = LOCAL_JOSN_DIR . $_collect_nameHour . '.json';
		if (date('H', $unix_rtime) == '00') {
			exec("rm -rf {$_jsonfile}");	// 跑0点的日志需要先删除json文件
		}

		$file_local_name = $file_local_dir . $file_name;
		$handle = gzopen($file_local_name, 'r');
		if ($handle) {
			while (!gzeof($handle)) {
				$line = gzgets($handle);
				if ($line === false || strpos($line, "/sy6/cu") !== 0 || !($line = trim($line, "\n"))) {
					continue;
				}

				$arr = explode("| |", $line);
				$codeidArr = explode('/', $arr[0]);
				// 如何不是/sy6/cu1/pc201/mt8这种格式就过滤掉
				if (!$arr[2] || !isset($codeidArr[4]) || strpos($codeidArr[4], 'mt') !== 0) {
					continue;
				}
				$_cu = substr($codeidArr[2], 2);
				$_pc = substr($codeidArr[3], 2);
				$_mt = substr($codeidArr[4], 2);
				if (!is_numeric($_cu) || !is_numeric($_pc) || !is_numeric($_mt)) {
					continue;
				}
				
				// cookie为空，MD5(ip . user agent)
				$arr[3] == "" && $arr[3] = md5($arr[2] . $arr[6]);

				$data = [
					'time'			=> strtotime($arr[1]),
					'hour'			=> date('H', strtotime($arr[1])),
					'ip'			=> sprintf('%u', ip2long($arr[2])),
					'cookieid'		=> substr($arr[3], 0, 32),
					'cu'			=> $_cu + 0,
					'pc'			=> $_pc + 0,
					'mt'			=> $_mt + 0
				];
				$data['provinceid'] = ip2provinceid($data['ip']);
//				file_put_contents($_jsonfile, json_encode($data) . "\r\n", FILE_APPEND);
				file_put_contents($_jsonfileHour, json_encode($data) . "\r\n", FILE_APPEND);
			}
		} else {
			err_log("clicks||nofile:{$file_local_name}", atFLAG);
			return false;
		}
		gzclose($handle);


		err_log("clicks_file josn success: {$file_name}\n", atFLAG);
		
		return true;
	}
	
}

