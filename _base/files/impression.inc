<?php
// +----------------------------------------------------------------------
// | Copyright (c) 2016 All rights reserved.
// +----------------------------------------------------------------------
// | *************
// +----------------------------------------------------------------------

/**
 * 曝光日志 清洗后入库到mongodb
 */
class FilesImpression
{
	/**
	 * 处理1天的文件
	 * @param  string $time [description]
	 * @return [type]       [description]
	 */
	public static function dofiles($time = '')
	{
		if (!check_time($time)) {
			return false;
		}
		// 配置信息
		$config = $_ENV['_db_configs'];

		// 日志配置
		$log_setting = $_ENV['_log_setting'];

		// 获取所有开通报告的网站ID
		$report_w_ids = get_report_w_ids($time);

		// 下载一天的24个文件（全部文件：日志*）
		$file_remote_fs = $log_setting['impression']['file_dir'] . date('Y/m/', strtotime($time));
		$file_remote_fs .= $log_setting['impression']['file_prefix'] . '.' . $time . '*';
		$file_local_dir = LOCAL_LOG_DIR . 'impression' . date('/Y/m/d/', strtotime($time));
		exec("mkdir -p {$file_local_dir}");
		exec("scp -P10001 " . LOG_SERVER_IP . ":{$file_remote_fs} {$file_local_dir}");

		$_collect_name = 'impression' . $time;
		// 打开json文件
		$_jsonfile = LOCAL_JOSN_DIR . $_collect_name . '.json';
		exec("rm -rf {$_jsonfile}");	// 先删除

		for ($i = 0; $i < 24; $i++) { 
			$filename = $file_local_dir . $log_setting['impression']['file_prefix'] . '.' . $time . sprintf("%02d", $i) . '.gz';
			$handle = gzopen($filename, 'r');
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
					file_put_contents($_jsonfile, json_encode($data) . "\r\n", FILE_APPEND);
				}
			} else {
				err_log("impression||nofile:{$filename}", atFLAG);
				return false;
			}
			gzclose($handle);
		}
		// 将json文件导入到mongo
		exec("/usr/local/mongodb/bin/mongoimport -h 172.21.0.90 -d {$config['_mongos']['dns_impression']['dbname']} -c {$_collect_name}  --file {$_jsonfile}");
		exec("rm -rf {$file_local_dir}");	// 删除24个日志文件

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

		err_log("impression_file start success: ======", atFLAG);

		// 配置信息
		$config = $_ENV['_db_configs'];

		// 日志配置
		$log_setting = $_ENV['_log_setting'];

		// 获取所有开通报告的网站ID
		$report_w_ids = get_report_w_ids($rtime);

		// 下载当前小时的文件
		$file_name = $log_setting['impression']['file_prefix'] . '.' . $time . '.gz';
		$file_remote_fs = $log_setting['impression']['file_dir'] . date('Y/m/', $unix_rtime) . $file_name;

		$file_local_dir = LOCAL_LOG_DIR . 'impression' . date('/Y/m/d/', $unix_rtime);
		exec("mkdir -p {$file_local_dir}");
		exec("scp -P10001 " . LOG_SERVER_IP . ":{$file_remote_fs} {$file_local_dir}");

		err_log("impression_file down success: {$file_name}", atFLAG);

		$_collect_name = 'impression' . date('Ymd', $unix_rtime);
		$_collect_nameHour = 'impression' . date('YmdH', $unix_rtime);
		
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
			err_log("impression||nofile:{$file_name}", atFLAG);
			return false;
		}
		gzclose($handle);
		unlink($file_local_name);

		err_log("impression_file json success: {$file_name}\n", atFLAG);

		return true;
	}
	
}

