<?php
// +----------------------------------------------------------------------
// | Copyright (c) 2016 All rights reserved.
// +----------------------------------------------------------------------
// | Author: Jerry.fang <fangzq@yiche.com>
// +----------------------------------------------------------------------

/**
 * 线索日志 清洗后入库到mongodb
 */
class FilesLeads
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

		$code_types = get_leads_type();


		// 下载一天的24个文件（全部文件：日志*）
		$file_remote_fs = $log_setting['leads']['file_dir'] . date('Y/m/', strtotime($time));
		$file_remote_fs .= $log_setting['leads']['file_prefix'] . '.' . $time . '*';
		$file_local_dir = LOCAL_LOG_DIR . 'leads' . date('/Y/m/d/', strtotime($time));
		exec("mkdir -p {$file_local_dir}");
		exec("scp -P10001 " . LOG_SERVER_IP . ":{$file_remote_fs} {$file_local_dir}");
		$_collect_name = 'leads' . $time;

		// 打开json文件
		$_jsonfile = LOCAL_JOSN_DIR . $_collect_name . '.json';
		exec("rm -rf {$_jsonfile}");	// 先删除

		for ($i = 0; $i < 24; $i++) { 
			$filename = $file_local_dir . $log_setting['leads']['file_prefix'] . '.' . $time . sprintf("%02d", $i) . '.gz';
			$handle = gzopen($filename, 'r');
			if ($handle) {
				while (!gzeof($handle)) {
					$line = gzgets($handle);
					if ($line === false || !($line = trim($line, "\n"))) {
						continue;
					}

					$arr = explode("| |", $line);
					// 如何参数不存在、当前页面URL不存在或者等于-，将不入库
					$varArr = [];
					if ($arr[0] != '/_ev.php' || !$arr[4] || !$arr[5] || $arr[5] == '-' || !preg_match('/dcev=fr:dad\|varid:([0-9]+).*/', $arr[4], $varArr) || !isset($varArr[1])) {
						continue;
					}
					
					// cookie为空，MD5(ip . user agent)
					$arr[3] == "" && $arr[3] = md5($arr[2] . $arr[6]); 

					$arr[5] = do_string_type($arr[5]);
					$_current_url = $arr[5];

					if (strpos($_current_url, '?') !== false) {
						list($_current_url, $__query) = explode('?', $_current_url, 2);
					}

					$_parse_url = parse_url($_current_url);
					$_current_url = str_replace(['http://', 'https://'], '', $_current_url); // 去除掉HTTP://

					// 不能检测到域名就屏蔽掉，并过滤不合规则的域名（目前只按照长度判断）
					if (!isset($_parse_url['host']) || strlen($_parse_url['host']) > 150) {
						continue;
					}

					$data = [
						'time'			=> strtotime($arr[1]),
						'hour'			=> date('H', strtotime($arr[1])),
						'ip'			=> sprintf('%u', ip2long($arr[2])),
						'cookieid'		=> substr($arr[3], 0, 32),
						'page'			=> strtolower($arr[5]),	// 原URL
						'url'			=> $_current_url,
						'url_md5'		=> md5($_current_url),
						'domain'		=> $_parse_url['host'], // 增加域名字段
						'dcdt'			=> ''
					];

					// 参数部分
					$paramArr = explode("&dc", $arr[4]);
					unset($paramArr[0]);
					foreach ($paramArr as $v) {
						$_v = explode("=", $v, 2);
						$_v[0] = 'dc' . $_v[0];
						switch ($_v[0]) {
							case 'dcdt':	// 页面标题
								($_v[1] && $_v[1] != '-') && $data['dcdt'] = urldecode(do_string_type($_v[1]));
								break;
							case 'dcev':	// 所属活动ID
								$data['varid'] = $varArr[1] + 0;
								// TODO 目前是按照库的记录，下一步直接按照日志的特殊字符处理【支付日志存在特殊字符参数】
                                if(!isset($code_types[$data['varid']]))  //如果时间删除
                                {
                                    break;
                                }
								$data['codetype'] = $code_types[$data['varid']] ? $code_types[$data['varid']] : 1; // 代码类型 1线索、2成功支付
								break;
							case 'dcad':	// 是否精准带来的线索
								if (preg_match('/rfpa_source:([0-9_]+).*/', $_v[1], $rfpa_sourceArr)) {
									$data['rfpa_source'] = $rfpa_sourceArr[1];
								}
								break;
							case 'dcvt':	// 访次信息
								$data['dcvt'] = explode('.', $_v[1]);
								break;
							case 'dcac':
								// 易车惠专题，判断url开头是否为专题
								if ($_v[1] != 155 && strpos($data['url'], 'www.bitauto.com/zhuanti') === 0) {
									$_v[1] = 155;
								}
								// 易车惠网站（PC && 移动）
								if ($_v[1] != 143 && in_array($data['domain'], ['shops.mai.m.yiche.com', 'mai.bitauto.com', 'mai.m.yiche.com', 'mai.yiche.com', 'mai.m.bitauto.com', 'item.mai.m.yiche.com','item.mai.bitauto.com','order.mai.m.yiche.com','pay.mai.m.yiche.com'])) {
									$_v[1] = 143;
								}
								// 淘车
								if ($_v[1] != 163 && strpos($data['domain'], 'taoche.com' !== false)) {
									$_v[1] = 163;
								}

								$data['dcac'] = $_v[1] + 0;
								break;
							case 'dcin':	// 内部引流编码
                                $_v[1] && $data['dcin_str'] = $_v[1];
                                if ($_v[1] && strpos($_v[1], '_') !== false) {		// 编码错误时
                                    $tmp_arr = explode('_', $_v[1]);
                                    if(count($tmp_arr) == 2)
                                    {
                                        list($__media, $__ad) = $tmp_arr;
                                        (int) $__media && (int) $__ad && $data['dcin'] = (int) $__media . '_' . (int) $__ad;
                                    }
                                    else
                                    {
                                        list($__cu,$__media, $__ad) = $tmp_arr;
                                        (int) $__cu &&  (int) $__media && (int) $__ad && $data['dcin'] = (int) $__cu .'_'.(int) $__media . '_' . (int) $__ad;
                                    }

                                }
                                $arrTimeLog['dcin']['start'] = microtime_float();//time count
                                $arrTimeLog['dcin']['end'] = microtime_float();//time count
                                $arrTimeLog['dcin']['use'] += $arrTimeLog['dcin']['end'] - $arrTimeLog['dcin']['start'];
                                break;
                            case 'dcbe':	// 内部引流编码
                                $_v[1] && $data['dcbe_str'] = $_v[1];
                                if ($_v[1] && strpos($_v[1], '_') !== false) {		// 编码错误时
                                    $tmp_arr = explode('_', $_v[1]);
                                    if(count($tmp_arr) == 2)
                                    {
                                        list($__media, $__ad) = $tmp_arr;
                                        (int) $__media && (int) $__ad && $data['dcin'] = (int) $__media . '_' . (int) $__ad;
                                    }
                                    else
                                    {
                                        list($__cu,$__media, $__ad) = $tmp_arr;
                                        (int) $__cu &&  (int) $__media && (int) $__ad && $data['dcin'] = (int) $__cu .'_'.(int) $__media . '_' . (int) $__ad;
                                    }

                                }
                                break;
							// case 'dcrf':	// 上一个页面URL，暂时没用
							// 	($_v[1] && $_v[1] != '-') && $data['dcrf'] = urldecode(do_string_type($_v[1]));
							// 	break;
						}
					}
					// 入库所有，不考虑开不开通报告
					$data['provinceid'] = ip2provinceid($data['ip']);	// 增加省份ID
					$data['domain_id']	= get_domain_id($data['dcac'], $data['domain']);
					if ($data['domain_id']) {
						$data['url_id'] = get_url_id($data['dcac'], $data['domain_id'], $data['url'], $data['dcdt']);
						if (!$data['url_id']) {
							continue;	// 无效URL不入库
						}
					} else {
						continue;		// 无效域名不入库
					}
					
					file_put_contents($_jsonfile, json_encode($data) . "\r\n", FILE_APPEND);
				}
			} else {
				err_log("leads||nofile:{$filename}", atFLAG);
				return false;
			}
			gzclose($handle);
		}
		// 将json文件导入到mongo
		exec("/usr/local/mongodb/bin/mongoimport -h 172.21.0.90 -d {$config['_mongos']['dns_leads']['dbname']} -c {$_collect_name}  --file {$_jsonfile}");
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

		err_log("leads_file start success: ======", atFLAG);

		// 配置信息
		$config = $_ENV['_db_configs'];

		// 日志配置
		$log_setting = $_ENV['_log_setting'];
		$code_types = get_leads_type();


		// 下载当前小时的文件
		$file_name = $log_setting['leads']['file_prefix'] . '.' . $time . '.gz';
		$file_remote_fs = $log_setting['leads']['file_dir'] . date('Y/m/', $unix_rtime) . $file_name;

		$file_local_dir = LOCAL_LOG_DIR . 'leads' . date('/Y/m/d/', strtotime($rtime));
		exec("mkdir -p {$file_local_dir}");
		exec("scp -P10001 " . LOG_SERVER_IP . ":{$file_remote_fs} {$file_local_dir}");
		err_log("leads_file down success: {$file_name}", atFLAG);

		$_collect_name = 'leads' . date('Ymd', $unix_rtime);
		$_collect_nameHour = 'leads' . date('YmdH', $unix_rtime);

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
				if ($line === false || !($line = trim($line, "\n"))) {
					continue;
				}

				$arr = explode("| |", $line);
				// 如何参数不存在、当前页面URL不存在或者等于-，将不入库
				$varArr = [];
				if ($arr[0] != '/_ev.php' || !$arr[4] || !$arr[5] || $arr[5] == '-' || !preg_match('/dcev=fr:dad\|varid:([0-9]+).*/', $arr[4], $varArr) || !isset($varArr[1])) {
					continue;
				}
				
				// cookie为空，MD5(ip . user agent)
				$arr[3] == "" && $arr[3] = md5($arr[2] . $arr[6]); 

				$arr[5] = do_string_type($arr[5]);
				$_current_url = $arr[5];

				if (strpos($_current_url, '?') !== false) {
					list($_current_url, $__query) = explode('?', $_current_url, 2);
				}

				$_parse_url = parse_url($_current_url);
				$_current_url = str_replace(['http://', 'https://'], '', $_current_url); // 去除掉HTTP://

				// 不能检测到域名就屏蔽掉，并过滤不合规则的域名（目前只按照长度判断）
				if (!isset($_parse_url['host']) || strlen($_parse_url['host']) > 150) {
					continue;
				}

				$data = [
					'time'			=> strtotime($arr[1]),
					'hour'			=> date('H', strtotime($arr[1])),
					'ip'			=> sprintf('%u', ip2long($arr[2])),
					'cookieid'		=> substr($arr[3], 0, 32),
//					'page'			=> strtolower($arr[5]),	// 原URL
					'page'			=> md5(str_replace(['http://', 'https://'], '', strtolower($arr[5]))),	// 原URL
					'url'			=> $_current_url,
					'url_md5'		=> md5($_current_url),
					'domain'		=> $_parse_url['host'], // 增加域名字段
					'dcdt'			=> ''
				];

				// 参数部分
				$paramArr = explode("&dc", $arr[4]);
				unset($paramArr[0]);
                if(!isset($code_types[$varArr[1]]))  //如果事件删除
                {
                    continue;
                }
				foreach ($paramArr as $v) {
					$_v = explode("=", $v, 2);
					$_v[0] = 'dc' . $_v[0];
					switch ($_v[0]) {
						case 'dcdt':	// 页面标题
							($_v[1] && $_v[1] != '-') && $data['dcdt'] = urldecode(do_string_type($_v[1]));
							break;
						case 'dcev':	// 所属活动ID
							$data['varid'] = $varArr[1] + 0;
                            if (preg_match('/codetype:([0-9_]+).*/', $_v[1], $codetypelog)) {
                                $data['codetype'] = $codetypelog[1];
                            }
                            else
                            {
                                $data['codetype'] = $code_types[$data['varid']] ? $code_types[$data['varid']] : 1; // 代码类型 1线索、2成功支付
                            }
                            break;

						case 'dcad':	// 是否精准带来的线索
							if (preg_match('/rfpa_source:([0-9_]+).*/', $_v[1], $rfpa_sourceArr)) {
								$data['rfpa_source'] = $rfpa_sourceArr[1];
							}
							break;
						case 'dcvt':	// 访次信息
							$data['dcvt'] = explode('.', $_v[1]);
							break;
						case 'dcac':
							// 易车惠专题
							if ($_v[1] != 155 && strpos($data['url'], 'www.bitauto.com/zhuanti') === 0) {
								$_v[1] = 155;
							}
							// 易车惠网站（PC && 移动）
							if ($_v[1] != 143 && in_array($data['domain'], ['shops.mai.m.yiche.com', 'mai.bitauto.com', 'mai.m.yiche.com', 'mai.yiche.com', 'mai.m.bitauto.com', 'item.mai.m.yiche.com','item.mai.bitauto.com','order.mai.m.yiche.com','pay.mai.m.yiche.com'])) {
								$_v[1] = 143;
							}
							// 淘车
							if ($_v[1] != 163 && strpos($data['domain'], 'taoche.com' !== false)) {
								$_v[1] = 163;
							}

							$data['dcac'] = $_v[1] + 0;
							break;


                        case 'dcin':	// 内部引流编码
                            $_v[1] && $data['dcin_str'] = $_v[1];
                            if ($_v[1] && strpos($_v[1], '_') !== false) {		// 编码错误时
                                $tmp_arr = explode('_', $_v[1]);
                                if(count($tmp_arr) == 2)
                                {
                                    list($__media, $__ad) = $tmp_arr;
                                    (int) $__media && (int) $__ad && $data['dcin'] = (int) $__media . '_' . (int) $__ad;
                                }
                                else
                                {
                                    list($__cu,$__media, $__ad) = $tmp_arr;
                                    (int) $__cu &&  (int) $__media && (int) $__ad && $data['dcin'] = (int) $__cu .'_'.(int) $__media . '_' . (int) $__ad;
                                }

                            }
                            $arrTimeLog['dcin']['start'] = microtime_float();//time count
                            $arrTimeLog['dcin']['end'] = microtime_float();//time count
                            $arrTimeLog['dcin']['use'] += $arrTimeLog['dcin']['end'] - $arrTimeLog['dcin']['start'];
                            break;
                        case 'dcbe':	// 内部引流编码
                            $_v[1] && $data['dcbe_str'] = $_v[1];
                            if ($_v[1] && strpos($_v[1], '_') !== false) {		// 编码错误时
                                $tmp_arr = explode('_', $_v[1]);
                                if(count($tmp_arr) == 2)
                                {
                                    list($__media, $__ad) = $tmp_arr;
                                    (int) $__media && (int) $__ad && $data['dcbe'] = (int) $__media . '_' . (int) $__ad;
                                }
                                else
                                {
                                    list($__cu,$__media, $__ad) = $tmp_arr;
                                    (int) $__cu &&  (int) $__media && (int) $__ad && $data['dcbe'] = (int) $__cu .'_'.(int) $__media . '_' . (int) $__ad;
                                }
                            }
                            break;
                   		case 'dccta_p':	// | &dccta_p=sy6_cu1_pc11_mt6,  最终点击
                   			//echo "{$_v[0]}|{$_v[1]}\n"; 
	                   		if($_v[1]){
	                   			$art4 = explode("_", $_v[1]);
						        $arrGet = array();
								foreach ($art4 as $krt4=>$vrt4){
									$vk = substr($vrt4, 0, 2);  
									$vv = substr($vrt4, 2);  
									if($vk && $vv) $arrGet[$vk] = intval($vv);
						        }
						        if($arrGet['cu'] and $arrGet['pc'] and $arrGet['mt']){
						        	$data['dccta_p'] = "{$arrGet['cu']}_{$arrGet['pc']}_{$arrGet['mt']}";
						        }
	                   		}
                            break;
                      	case 'dccta_pbe':	// &dccta_pbe=sy6_cu1_pc11_mt2,初始点击
                      		//echo "{$_v[0]}|{$_v[1]}\n"; 
	                   		if($_v[1]){
	                   			$art4 = explode("_", $_v[1]);
						        $arrGet = array();
								foreach ($art4 as $krt4=>$vrt4){
									$vk = substr($vrt4, 0, 2);  
									$vv = substr($vrt4, 2);  
									if($vk && $vv) $arrGet[$vk] = intval($vv);
						        }
						        if($arrGet['cu'] and $arrGet['pc'] and $arrGet['mt']){
						        	$data['dccta_pbe'] = "{$arrGet['cu']}_{$arrGet['pc']}_{$arrGet['mt']}";
						        }
	                   		}
                           	break;
                        /*********************************autotracking2.4新增*******************************************/
                        case 'dctp': //网站来源
                            $data['dctp'] = $_v[1]+0; //0直接登录，1推荐网站（友链等），2搜索引擎，3广告推广
                            $data['dctp'] = $data['dctp'] == 3?1:$data['dctp']+0; //如果为广告推广 算作外部链接
                            break;
                        case 'dcse':  //竖线分割，分别是搜索引擎|关键词
//                            $data['dcse'] = $_v[1];
                            if(!strstr($_v[1], '|'))
                            {
                                $_v[1] = urldecode($_v[1]);
                            }
                            $search_content = explode('|', $_v[1]);
                            $data['searchengine'] = $search_content[0]; //搜索引擎
                            $data['searchkeys'] = $search_content[1]; // 搜索关键词
                            break;
                        case 'dcisnw': //网站来源
                            $data['dcisnw'] = $_v[1]+0; //是否是新用户
                            break;
                        case 'dcrf':	// 上一个页面URL，暂时没用
                         	($_v[1] && $_v[1] != '-') && $data['dcrf'] = urldecode(do_string_type($_v[1]));
                         	break;
                        case 'dccv': //自定义变量
                            ($_v[1] && $_v[1] != '-') && $data['dccv'] = urldecode(do_string_type($_v[1]));
                            break;
                        case 'dcr': //自定义变量
                            if($data['dcac'] > 0)
                            {
                                $_v[1] = urldecode(do_string_type($_v[1]));
                                $recomand = explode('|', $_v[1]);
                                $data['recomand_domain']	= get_domain_id($data['dcac'], $recomand[0], true);
                                if ($data['recomand_domain']) {
                                    $data['recomand_url'] = get_url_id($data['dcac'], $data['domain_id'], $recomand[0].$recomand[1], '', true);
                                    if (!$data['url_id']) {
                                        continue;	// 无效URL不入库
                                    }
                                } else {
                                    continue;		// 无效域名不入库
                                }
                            }
                            break;
					}
				}
				// 入库所有，不考虑开不开通报告
				$data['provinceid'] = ip2provinceid($data['ip']);	// 增加省份ID
				$data['domain_id']	= get_domain_id($data['dcac'], $data['domain']);
				if ($data['domain_id']) {
                    unset($data['domain']);
					$data['url_id'] = get_url_id($data['dcac'], $data['domain_id'], $data['url'], $data['dcdt']);
                    unset($data['url']);
                    unset($data['url_md5']);
					if (!$data['url_id']) {
						continue;	// 无效URL不入库
					}
				} else {
					continue;		// 无效域名不入库
				}
				
//				file_put_contents($_jsonfile, json_encode($data) . "\r\n", FILE_APPEND);
				file_put_contents($_jsonfileHour, json_encode($data) . "\r\n", FILE_APPEND);
			}
		} else {
			err_log("leads||nofile:{$file_local_name}", atFLAG);
			return false;
		}
		gzclose($handle);

/*		// 跑完23点文件，并且文件数为24个,将json文件导入到mongo
		if (date('H', $unix_rtime) == '23') {// && system("ls -l {$file_local_dir} |grep '^-'|wc -l") == 24
			
			$cmd = "/usr/local/mongodb/bin/mongoimport -h 172.21.0.90 -d {$config['_mongos']['dns_leads']['dbname']} -c {$_collect_name}  --file {$_jsonfile}";
			err_log("{$time}|FilesLeads dofiles_hour|{$cmd}|start", atFLAG);
			exec($cmd);
			err_log("{$time}|FilesLeads dofiles_hour|{$cmd}|end", atFLAG);
			
			//unlink($_jsonfile);
			err_log("{$time}|FilesLeads dofiles_hour|unlink|{$_jsonfile}", atFLAG);
			
			$cmd = "rm -rf {$file_local_dir}";
			err_log("{$time}|FilesLeads dofiles_hour|{$cmd}", atFLAG);
			exec($cmd);	// 删除24个日志文件
		}*/

		err_log("leads_file json success: {$file_name}\n", atFLAG);

		return true;
	}
	
}

