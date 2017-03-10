<?php
// +----------------------------------------------------------------------
// | Copyright (c) 2016 All rights reserved.
// +----------------------------------------------------------------------
// | *************
// +----------------------------------------------------------------------

/**
 * 行为日志 清洗入库到mongodb
 */
class FilesDcclog{


    /**
     * 把dcclog解压拆分成10个文件
     * @param $file
     * @return bool
     * @author fengqiang@yiche.com
     */
    public static function splitGzFile($file)
    {
        // 解压文件
        $shell_1 = "gunzip $file";
        exec($shell_1);

        //拆分成10个解压文件
        $file = rtrim($file, '.gz');
        if(file_exists($file))
        {
            $size = filesize($file);
            $oneSize = ceil($size/1024/1024/9); //拆分成10个文件，每个文件大小 单位M
            $lines = $oneSize * 820; //经过测试大概802行/M

            $shell = "split -l {$lines} -d {$file} {$file}."; //shell拆分文件
            exec($shell);

            unlink($file); //已切割文件 删除解压的文件
        }
        return true;
    }


    public static function dofiles_hour_new($time = '')
    {
        $rtime = $time . '0000';
        if (!check_time($rtime, 'YmdHis')) {
            return false;
        }
        $unix_rtime = strtotime($rtime);

        err_log("{$time}|FilesDcclog dofiles_hour|rtime:{$rtime}|start", atFLAG);

        // 配置信息


        // 日志配置
        $log_setting = $_ENV['_log_setting'];
        // 下载一个小时的一个文件
        $file_name = $log_setting['dcclog']['file_prefix'] . '.' . $time . '.gz';
        $file_remote_fs = $log_setting['dcclog']['file_dir'] . date('Y/m/', $unix_rtime) . $file_name;
        $file_local_dir = LOCAL_LOG_DIR . 'dcclog' . date('/Y/m/d/', $unix_rtime);
        exec("mkdir -p {$file_local_dir}");
		exec("scp -P10001 " . LOG_SERVER_IP . ":{$file_remote_fs} {$file_local_dir}");
        err_log("{$time}|FilesDcclog dofiles_hour|dcclog_file down success: {$file_name}", atFLAG);

        $_collect_name = 'dcclog' . date('YmdH', $unix_rtime);
        $dcclogDir = LOCAL_JOSN_DIR."dcclog";
        if(!is_dir($dcclogDir)) {
            mkdir($dcclogDir, 0777, true);
        }

        // json文件操作
        $_jsonfile = $dcclogDir ."/". $_collect_name . '.json';

        $_jsonfileHour = $dcclogDir."/" . $_collect_name ;
        if (date('H', $unix_rtime) == '00') {
            exec("rm -rf {$_jsonfile}");	// 跑0点的日志需要先删除json文件
        }

        $arrCountLog = array();
        $arrTimeLog = array();

        $file_local_name = $file_local_dir . $file_name;
        if(!file_exists($file_local_name))
        {
            return false;
        }
        static::splitGzFile($file_local_name);
        $arrTimeLog['all']['start'] = microtime_float();//time count
        $arrTimeLog['all']['end'] = microtime_float();//time count
        $arrTimeLog['all']['use'] += $arrTimeLog['all']['end'] - $arrTimeLog['all']['start'];

        foreach ($arrTimeLog as $k1=>$v1){
            err_log("{$time}|FilesDcclog dofiles_hour|time|{$k1}:{$v1['use']}", atFLAG);
        }

        err_log("{$time}|FilesDcclog dofiles_hour|rtime:{$rtime}|end|muse:".ceil(memory_get_usage()/(1024*1024))."M", atFLAG);
        $file_local_name = rtrim($file_local_name, '.gz');
        return array($file_local_name, $_jsonfileHour);
    }

    /**
     * 处理1个小时的文件
     * @param  string $time [description]
     * @return [type]       [description]
     */
    public static function formatData2PutJsonFile($file_local_name, $_jsonfile, $time, $pid)
    {

        $cluster = RedisC::getRedis();
//        $cluster = new RedisCluster(NULL,['172.21.0.219:7000','172.21.0.220:7000','172.21.0.221:7000','172.21.0.219:7001','172.21.0.220:7001','172.21.0.221:7001']);
        $rtime = $time . '0000';
        if (!check_time($rtime, 'YmdHis')) {
            return false;
        }
        $unix_rtime = strtotime($rtime);
        $hour = date('H', $unix_rtime);           // 日期，只有小时
        $hour_time = $hour = date('YmdH', $unix_rtime);           // 日期，只有小时
        err_log("{$time}|FilesDcclog dofiles_hour|rtime:{$rtime}|start", atFLAG);

        // 配置信息

        // 日志配置
        // 获取所有开通报告的网站ID
//        $report_w_ids = get_report_w_ids($rtime);

        $arrCountLog = array();
        $arrTimeLog = array();

//        $file_local_name = $file_local_dir . $file_name;
        $handle = fopen($file_local_name, 'r');
        $arrTimeLog['all']['start'] = microtime_float();//time count

        if ($handle) {
            while (!feof($handle)) {
                $line = fgets($handle);
                $arrCountLog['all'] += 1;//log count

                /*				if ($line === false || !($line = trim($line, "\n"))) {
                                    continue;
                                }*/

                $arr = explode("| |", $line);
                // 如何参数不存在、当前页面URL不存在或者等于-，将不入库
                if (!$arr[4] || !$arr[5] || $arr[5] == '-') {
                    continue;
                }

                $arrCountLog['can'] += 1;//log count

                // cookie为空，MD5(ip . user agent)
                $arr[3] == "" && $arr[3] = md5($arr[2] . $arr[6]);

                $arrTimeLog['url']['start'] = microtime_float();//time count

                $arrTimeLog['do_string_type']['start'] = microtime_float();//time count
                $arr[5] = do_string_type($arr[5]);
//                $arr[5] = str_replace(['http://', 'https://'], '',$arr[5]); // 去除掉HTTP://
//                $arr[5] = rtrim($arr[5], '/');
                $arrTimeLog['do_string_type']['end'] = microtime_float();//time count
                $arrTimeLog['do_string_type']['use'] += $arrTimeLog['do_string_type']['end'] - $arrTimeLog['do_string_type']['start'];

                $_current_url = $arr[5];

                $arrTimeLog['list']['start'] = microtime_float();//time count
                if (strpos($_current_url, '?') !== false) {
                    list($_current_url, $__query) = explode('?', $_current_url, 2);
                }
                $arrTimeLog['list']['end'] = microtime_float();//time count
                $arrTimeLog['list']['use'] += $arrTimeLog['list']['end'] - $arrTimeLog['list']['start'];

                $arrTimeLog['parse_url']['start'] = microtime_float();//time count
                $_parse_url = parse_url($_current_url);
                $arrTimeLog['parse_url']['end'] = microtime_float();//time count
                $arrTimeLog['parse_url']['use'] += $arrTimeLog['parse_url']['end'] - $arrTimeLog['parse_url']['start'];

                $arrTimeLog['str_replace']['start'] = microtime_float();//time count
                $_current_url = str_replace(['http://', 'https://'], '', $_current_url); // 去除掉HTTP://
                $_current_url = rtrim($_current_url, '/'); //去除最后一个/
                $arrTimeLog['str_replace']['end'] = microtime_float();//time count
                $arrTimeLog['str_replace']['use'] += $arrTimeLog['str_replace']['end'] - $arrTimeLog['str_replace']['start'];

                // 不能检测到域名就屏蔽掉，并过滤不合规则的域名（目前只按照长度判断）,并过滤IP地址
                if (!isset($_parse_url['host']) || strlen($_parse_url['host']) > 150 || ip2long($_parse_url['host'])) {
                    continue;
                }
                $arrCountLog['host'] += 1;//log count

                $data = [
                    'time'			=> strtotime($arr[1]),
                    'hour'			=> date('H', strtotime($arr[1])),
                    'ip'			=> sprintf('%u', ip2long($arr[2])),
                    'cookieid'		=> substr($arr[3], 0, 32),
//                    'page'			=> strtolower($arr[5]),	// 原URL
                    'page'			=> md5(strtolower($arr[5])),	// 原URL
                    'url'			=> $_current_url,
                    'url_md5'		=> md5($_current_url),
                    'domain'		=> $_parse_url['host'], // 增加域名字段
                    'dcdt'			=> ''
                ];
                $arrTimeLog['url']['end'] = microtime_float();//time count
                $arrTimeLog['url']['use'] += $arrTimeLog['url']['end'] - $arrTimeLog['url']['start'];

                $arrTimeLog['switch']['start'] = microtime_float();//time count
                // 参数部分
                $event_flag = false;
                //判断是否为事件
                if(strstr($arr[4], "&dct=2"))
                {
                    $event_flag = true;
                }
                $paramArr = explode("&dc", $arr[4]);
                unset($paramArr[0]);
                foreach ($paramArr as $v) {
                    $_v = explode("=", $v, 2);
                    $_v[0] = 'dc' . $_v[0];
                    switch ($_v[0]) {
                        case 'dcdt':	// 页面标题
                            ($_v[1] && $_v[1] != '-') && $data['dcdt'] = urldecode(do_string_type($_v[1]));
                            break;
                        case 'dcv':		// 访次.PV
                            $_v_dcv = explode(".", $_v[1], 2);
                            $data['dcv_0'] = $_v_dcv[0] + 0;
                            $data['dcv_1'] = $_v_dcv[1] + 0;
                            $data['dcv'] = $_v[1];
                            break;
                        case 'dcvt':	// 访次信息
                            $data['dcvt'] = explode('.', $_v[1]);
                            break;
                        case 'dcac':
                            $arrTimeLog['155']['start'] = microtime_float();//time count
                            // 易车惠专题
                            if ($_v[1] != 155) {
                                if (   strpos($data['url'], 'www.bitauto.com/topics/ad_topic/') === 0
                                    or strpos($data['url'], 'www.bitauto.com/topics/adtopic/') === 0
                                    or strpos($data['url'], 'www.bitauto.com/zhuanti/ad_topic/') === 0
                                    or strpos($data['url'], 'www.bitauto.com/zhuanti/adtopic/') === 0
                                ) {
                                    $_v[1] = 155;
                                }
                            }
                            $arrTimeLog['155']['end'] = microtime_float();//time count
                            $arrTimeLog['155']['use'] += $arrTimeLog['155']['end'] - $arrTimeLog['155']['start'];

                            $arrTimeLog['143']['start'] = microtime_float();//time count
                            // 易车惠网站（PC && 移动）
                            if ($_v[1] != 143 && in_array($data['domain'], ['shops.mai.m.yiche.com', 'mai.bitauto.com', 'mai.m.yiche.com', 'mai.yiche.com', 'mai.m.bitauto.com', 'item.mai.m.yiche.com','item.mai.bitauto.com','order.mai.m.yiche.com','pay.mai.m.yiche.com'])) {
                                $_v[1] = 143;
                            }
                            $arrTimeLog['143']['end'] = microtime_float();//time count
                            $arrTimeLog['143']['use'] += $arrTimeLog['143']['end'] - $arrTimeLog['143']['start'];

                            $arrTimeLog['163']['start'] = microtime_float();//time count
                            // 淘车
                            if ($_v[1] != 163 && strpos($data['domain'], 'taoche.com' !== false)) {
                                $_v[1] = 163;
                            }
                            $arrTimeLog['163']['end'] = microtime_float();//time count
                            $arrTimeLog['163']['use'] += $arrTimeLog['163']['end'] - $arrTimeLog['163']['start'];

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
                            $arrTimeLog['dcbe']['start'] = microtime_float();//time count
                            $arrTimeLog['dcbe']['end'] = microtime_float();//time count
                            $arrTimeLog['dcbe']['use'] += $arrTimeLog['dcbe']['end'] - $arrTimeLog['dcbe']['start'];
                            break;
                        case 'dce':
                            ($_v[1] && $_v[1] != '-') && $data['dce'] = urldecode(do_string_type($_v[1]));
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
                            $_v[1] = urldecode(do_string_type($_v[1]));
                            $search_content = explode('|', $_v[1]);
                            $data['searchengine'] = $search_content[0]; //搜索引擎
                            $data['searchkeys'] = $search_content[1]; // 搜索关键词
                            break;
                        case 'dcisnw': //网站来源
                            $data['dcisnw'] = $_v[1]+0; //是否是新用户
                            break;
                        case 'dcrf':	// 上一个页面URL，如果不存在表示用户直接输入
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

                $arrTimeLog['switch']['end'] = microtime_float();//time count
                $arrTimeLog['switch']['use'] += $arrTimeLog['switch']['end'] - $arrTimeLog['switch']['start'];

                $arrTimeLog['file']['start'] = microtime_float();//time count
                // 只入库开通报告的网站日志
//                if (isset($data['dcac']) && in_array($data['dcac'], $report_w_ids)) {
                if ($data['dcac']>0) {
                    $arrTimeLog['ip2provinceid2']['start'] = microtime_float();//time count
                    $data['provinceid'] = ip2provinceid2($arr[2]);	// 增加省份ID

                    $arrTimeLog['ip2provinceid2']['end'] = microtime_float();//time count
                    $arrTimeLog['ip2provinceid2']['use'] += $arrTimeLog['ip2provinceid2']['end'] - $arrTimeLog['ip2provinceid2']['start'];

                    //echo "1:{$data['provinceid']} 2:{$strt}\n";
                    //if($arrCountLog['ok']>10) exit();

                    $arrTimeLog['get_domain_id']['start'] = microtime_float();//time count
                    $data['domain_id']	= get_domain_id($data['dcac'], $data['domain']);

                    $arrTimeLog['get_domain_id']['end'] = microtime_float();//time count
                    $arrTimeLog['get_domain_id']['use'] += $arrTimeLog['get_domain_id']['end'] - $arrTimeLog['get_domain_id']['start'];

                    $arrTimeLog['get_url_id']['start'] = microtime_float();//time count
                    if ($data['domain_id']) {
                        unset($data['domain']);
                        $data['url_id'] = get_url_id($data['dcac'], $data['domain_id'], $data['url'], $data['dcdt']);
                        unset($data['url']);
                        unset($data['url_md5']);
                        unset($data['dcdt']);
                        if (!$data['url_id']) {
                            continue;	// 无效URL不入库
                        }
                    } else {
                        continue;		// 无效域名不入库
                    }
                    $arrTimeLog['get_url_id']['end'] = microtime_float();//time count
                    $arrTimeLog['get_url_id']['use'] += $arrTimeLog['get_url_id']['end'] - $arrTimeLog['get_url_id']['start'];
                    //	continue;

                    $arrCountLog['ok'] += 1;//log count

                    $arrTimeLog['file_put_contents']['start'] = microtime_float();//time count
                    $webids = $_ENV['mongo_wids'];
                    if(in_array($data['dcac'], $webids))
                    {
                        $_newjsonfile =$_jsonfile.".".$data['dcac'].".".$pid.".json";
                    }
                    else
                    {
                        $_newjsonfile = $_jsonfile.".other".".".$pid.".json";
                    }
                    if($event_flag)
                    {
                        $dcclogDir = LOCAL_JOSN_DIR."dcclog";
                        // json文件操作
                        $_collect_name = "event".$hour_time.".".$pid;
                        $eventjson = $dcclogDir ."/". $_collect_name . '.json';
                        file_put_contents($eventjson, json_encode($data) . "\r\n", FILE_APPEND);
                    }
                    else
                    {
                        file_put_contents($_newjsonfile, json_encode($data) . "\r\n", FILE_APPEND);
                    }

                    $arrTimeLog['file_put_contents']['end'] = microtime_float();//time count
                    $arrTimeLog['file_put_contents']['use'] += $arrTimeLog['file_put_contents']['end'] - $arrTimeLog['file_put_contents']['start'];

                }
                $arrTimeLog['file']['end'] = microtime_float();//time count
                $arrTimeLog['file']['use'] += $arrTimeLog['file']['end'] - $arrTimeLog['file']['start'];

            }
        	fclose($handle);
        	unlink($file_local_name);
        } else {
            err_log("{$time}|FilesDcclog dofiles_hour||nofile:{$file_local_name}|all:{$arrCountLog['all']}|ok:{$arrCountLog['ok']}", atFLAG);
            return false;
        }
        $arrTimeLog['all']['end'] = microtime_float();//time count
        $arrTimeLog['all']['use'] += $arrTimeLog['all']['end'] - $arrTimeLog['all']['start'];
        foreach ($arrTimeLog as $k1=>$v1){
            err_log("{$time}|FilesDcclog dofiles_hour|time|{$k1}:{$v1['use']}", atFLAG);
        }
        err_log("{$time}|FilesDcclog dofiles_hour|rtime:{$rtime}|end|muse:".ceil(memory_get_usage()/(1024*1024))."M", atFLAG);

        return true;
    }



    /**
     * 处理1个小时的文件
     * @param  string $time [description]
     * @return [type]       [description]
     */
    public static function do_files_hour($time)
    {
        $cluster = new RedisCluster(NULL,['172.21.1.62:6379','172.21.1.62:6380','172.21.1.63:6379','172.21.1.63:6380','172.21.1.64:6379','172.21.1.64:6380']);
//        $cluster = new RedisCluster(NULL,['172.21.0.219:7000','172.21.0.220:7000','172.21.0.221:7000','172.21.0.219:7001','172.21.0.220:7001','172.21.0.221:7001']);
        $rtime = $time . '0000';
        if (!check_time($rtime, 'YmdHis')) {
            return false;
        }
        $unix_rtime = strtotime($rtime);

        err_log("{$time}|FilesDcclog dofiles_hour|rtime:{$rtime}|start", atFLAG);

        // 配置信息
        $config = $_ENV['_db_configs'];

        // 日志配置
        $log_setting = $_ENV['_log_setting'];

        // 获取所有开通报告的网站ID
//        $report_w_ids = get_report_w_ids($rtime);
        // 下载一个小时的一个文件
        $file_name = $log_setting['dcclog']['file_prefix'] . '.' . $time . '.gz';
        $file_remote_fs = $log_setting['dcclog']['file_dir'] . date('Y/m/', $unix_rtime) . $file_name;

        $file_local_dir = LOCAL_LOG_DIR . 'dcclog' . date('/Y/m/d/', $unix_rtime);
        exec("mkdir -p {$file_local_dir}");
        exec("scp -P10001 " . LOG_SERVER_IP . ":{$file_remote_fs} {$file_local_dir}");

        err_log("{$time}|FilesDcclog dofiles_hour|dcclog_file down success: {$file_name}", atFLAG);

        $_collect_name = 'dcclog' . date('Ymd', $unix_rtime);
        $_collect_nameHour = 'dcclog' . date('YmdH', $unix_rtime);

        // json文件操作
        $_jsonfile = LOCAL_JOSN_DIR . $_collect_name . '.json';
//        $_jsonfileHour = LOCAL_JOSN_DIR . $_collect_nameHour . '.json';
         if (date('H', $unix_rtime) == '00') {
             exec("rm -rf {$_jsonfile}");	// 跑0点的日志需要先删除json文件
         }

        $arrCountLog = array();
        $arrTimeLog = array();

        $file_local_name = $file_local_dir . $file_name;
        $handle = gzopen($file_local_name, 'r');
        $arrTimeLog['all']['start'] = microtime_float();//time count

        if ($handle) {
            while (!gzeof($handle)) {
                $line = gzgets($handle);
                $arrCountLog['all'] += 1;//log count

                /*				if ($line === false || !($line = trim($line, "\n"))) {
                                    continue;
                                }*/

                $arr = explode("| |", $line);
                // 如何参数不存在、当前页面URL不存在或者等于-，将不入库
                if (!$arr[4] || !$arr[5] || $arr[5] == '-') {
                    continue;
                }

                $arrCountLog['can'] += 1;//log count

                // cookie为空，MD5(ip . user agent)
                $arr[3] == "" && $arr[3] = md5($arr[2] . $arr[6]);

                $arrTimeLog['url']['start'] = microtime_float();//time count

                $arrTimeLog['do_string_type']['start'] = microtime_float();//time count
                $arr[5] = do_string_type($arr[5]);
                $arrTimeLog['do_string_type']['end'] = microtime_float();//time count
                $arrTimeLog['do_string_type']['use'] += $arrTimeLog['do_string_type']['end'] - $arrTimeLog['do_string_type']['start'];

                $_current_url = $arr[5];

                $arrTimeLog['list']['start'] = microtime_float();//time count
                if (strpos($_current_url, '?') !== false) {
                    list($_current_url, $__query) = explode('?', $_current_url, 2);
                }
                $arrTimeLog['list']['end'] = microtime_float();//time count
                $arrTimeLog['list']['use'] += $arrTimeLog['list']['end'] - $arrTimeLog['list']['start'];

                $arrTimeLog['parse_url']['start'] = microtime_float();//time count
                $_parse_url = parse_url($_current_url);
                $arrTimeLog['parse_url']['end'] = microtime_float();//time count
                $arrTimeLog['parse_url']['use'] += $arrTimeLog['parse_url']['end'] - $arrTimeLog['parse_url']['start'];

                $arrTimeLog['str_replace']['start'] = microtime_float();//time count
                $_current_url = str_replace(['http://', 'https://'], '', $_current_url); // 去除掉HTTP://
                $arrTimeLog['str_replace']['end'] = microtime_float();//time count
                $arrTimeLog['str_replace']['use'] += $arrTimeLog['str_replace']['end'] - $arrTimeLog['str_replace']['start'];

                // 不能检测到域名就屏蔽掉，并过滤不合规则的域名（目前只按照长度判断）,并过滤IP地址
                if (!isset($_parse_url['host']) || strlen($_parse_url['host']) > 150 || ip2long($_parse_url['host'])) {
                    continue;
                }
                $arrCountLog['host'] += 1;//log count

                $data = [
                    'time'			=> strtotime($arr[1]),
                    'hour'			=> date('H', strtotime($arr[1])),
                    'ip'			=> sprintf('%u', ip2long($arr[2])),
                    'cookieid'		=> substr($arr[3], 0, 32),
//                    'page'			=> strtolower($arr[5]),	// 原URL
                    'page'			=> md5(strtolower($arr[5])),	// 原URL
                    'url'			=> $_current_url,
                    'url_md5'		=> md5($_current_url),
                    'domain'		=> $_parse_url['host'], // 增加域名字段
                    'dcdt'			=> ''
                ];
                $arrTimeLog['url']['end'] = microtime_float();//time count
                $arrTimeLog['url']['use'] += $arrTimeLog['url']['end'] - $arrTimeLog['url']['start'];

                $arrTimeLog['switch']['start'] = microtime_float();//time count
                // 参数部分
                $event_flag = false;
                //判断是否为事件
                if(strstr($arr[4], "&dct=2"))
                {
                    $event_flag = true;
                }
                $event_fileds = array('dcdp', 'dcxc_id', 'dce', 'dccv', 'dcbtun', 'dcxn', 'dcr', 'dcin');
                $paramArr = explode("&dc", $arr[4]);
                unset($paramArr[0]);
                foreach ($paramArr as $v) {
                    $_v = explode("=", $v, 2);
                    $_v[0] = 'dc' . $_v[0];
                    switch ($_v[0]) {
                        case 'dcdt':	// 页面标题
                            ($_v[1] && $_v[1] != '-') && $data['dcdt'] = urldecode(do_string_type($_v[1]));
                            break;
                        case 'dcv':		// 访次.PV
                            $_v_dcv = explode(".", $_v[1], 2);
                            $data['dcv_0'] = $_v_dcv[0] + 0;
                            $data['dcv_1'] = $_v_dcv[1] + 0;
                            $data['dcv'] = $_v_dcv[1];
                            break;
                        case 'dcvt':	// 访次信息
                            $data['dcvt'] = explode('.', $_v[1]);
                            break;
                        case 'dcac':
                            //先从redis里取，如果没有，再走逻辑
//                            $acurlmd5 = 'AC'.md5($data['url']);
//                            $t = $cluster->HGET($acurlmd5, 'ac');
//                            $data['dcac'] = $t + 0;

//                            if(!$t){
                                $arrTimeLog['155']['start'] = microtime_float();//time count
                                // 易车惠专题
                                if ($_v[1] != 155) {
                                    if (   strpos($data['url'], 'www.bitauto.com/topics/ad_topic/') === 0
                                        or strpos($data['url'], 'www.bitauto.com/topics/adtopic/') === 0
                                        or strpos($data['url'], 'www.bitauto.com/zhuanti/ad_topic/') === 0
                                        or strpos($data['url'], 'www.bitauto.com/zhuanti/adtopic/') === 0
                                    ) {
                                        $_v[1] = 155;
                                    }
                                }
                                $arrTimeLog['155']['end'] = microtime_float();//time count
                                $arrTimeLog['155']['use'] += $arrTimeLog['155']['end'] - $arrTimeLog['155']['start'];

                                $arrTimeLog['143']['start'] = microtime_float();//time count
                                // 易车惠网站（PC && 移动）
                                if ($_v[1] != 143 && in_array($data['domain'], ['shops.mai.m.yiche.com', 'mai.bitauto.com', 'mai.m.yiche.com', 'mai.yiche.com', 'mai.m.bitauto.com', 'item.mai.m.yiche.com','item.mai.bitauto.com','order.mai.m.yiche.com','pay.mai.m.yiche.com'])) {
                                    $_v[1] = 143;
                                }
                                $arrTimeLog['143']['end'] = microtime_float();//time count
                                $arrTimeLog['143']['use'] += $arrTimeLog['143']['end'] - $arrTimeLog['143']['start'];

                                $arrTimeLog['163']['start'] = microtime_float();//time count
                                // 淘车
                                if ($_v[1] != 163 && strpos($data['domain'], 'taoche.com' !== false)) {
                                    $_v[1] = 163;
                                }
                                $arrTimeLog['163']['end'] = microtime_float();//time count
                                $arrTimeLog['163']['use'] += $arrTimeLog['163']['end'] - $arrTimeLog['163']['start'];

                                $data['dcac'] = $_v[1] + 0;
//                                $t = $cluster->del($acurlmd5);
//                                $t = $cluster->HSET($acurlmd5, 'ac', $data['dcac']);
//                            }
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
                        // case 'dcrf':	// 上一个页面URL，暂时没用
                        // 	($_v[1] && $_v[1] != '-') && $data['dcrf'] = urldecode(do_string_type($_v[1]));
                        // 	break;

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
                            $arrTimeLog['dcbe']['start'] = microtime_float();//time count
                            $arrTimeLog['dcbe']['end'] = microtime_float();//time count
                            $arrTimeLog['dcbe']['use'] += $arrTimeLog['dcbe']['end'] - $arrTimeLog['dcbe']['start'];
                            break;
                        // case 'dcrf':	// 上一个页面URL，暂时没用
                        // 	($_v[1] && $_v[1] != '-') && $data['dcrf'] = urldecode(do_string_type($_v[1]));
                        // 	break;
                        case in_array($_v[0], $event_fileds) && $event_flag:
                            $data[$_v[0]] = $_v[1];
                            break;
                        /*********************************autotracking2.4新增*******************************************/
                        case 'dctp': //网站来源
                            $data['dctp'] = $_v[1]; //0直接登录，1推荐网站（友链等），2搜索引擎，3广告推广
                            $data['dctp'] = $data['dctp'] == 3?1:$data['dctp']; //如果为广告推广 算作外部链接
                            break;
                        case 'dcse':  //竖线分割，分别是搜索引擎|关键词
                            $data['dcse'] = $_v[1];
                            $search_content = explode('|', $_v[1]);
                            $data['searchengine'] = $search_content[0]; //搜索引擎
                            $data['searchkeys'] = $search_content[1]; // 搜索关键词
                            break;
                    }
                }
                $arrTimeLog['switch']['end'] = microtime_float();//time count
                $arrTimeLog['switch']['use'] += $arrTimeLog['switch']['end'] - $arrTimeLog['switch']['start'];

                $arrTimeLog['file']['start'] = microtime_float();//time count
                // 只入库开通报告的网站日志
//                if (isset($data['dcac']) && in_array($data['dcac'], $report_w_ids)) {
                if (isset($data['dcac'])) {

                    /*					$arrTimeLog['ip2provinceid']['start'] = microtime_float();//time count
                                        $data['provinceid'] = ip2provinceid($data['ip']);	// 增加省份ID
                                        $arrTimeLog['ip2provinceid']['end'] = microtime_float();//time count
                                        $arrTimeLog['ip2provinceid']['use'] += $arrTimeLog['ip2provinceid']['end'] - $arrTimeLog['ip2provinceid']['start'];
                                        */
                    $arrTimeLog['ip2provinceid2']['start'] = microtime_float();//time count
                    $data['provinceid'] = ip2provinceid2($arr[2]);	// 增加省份ID

                    $arrTimeLog['ip2provinceid2']['end'] = microtime_float();//time count
                    $arrTimeLog['ip2provinceid2']['use'] += $arrTimeLog['ip2provinceid2']['end'] - $arrTimeLog['ip2provinceid2']['start'];

                    //echo "1:{$data['provinceid']} 2:{$strt}\n";
                    //if($arrCountLog['ok']>10) exit();

                    $arrTimeLog['get_domain_id']['start'] = microtime_float();//time count
                    $data['domain_id']	= get_domain_id($data['dcac'], $data['domain']);

                    $arrTimeLog['get_domain_id']['end'] = microtime_float();//time count
                    $arrTimeLog['get_domain_id']['use'] += $arrTimeLog['get_domain_id']['end'] - $arrTimeLog['get_domain_id']['start'];

                    $arrTimeLog['get_url_id']['start'] = microtime_float();//time count
                    if ($data['domain_id']) {
                        unset($data['domain']);
                        $data['url_id'] = get_url_id($data['dcac'], $data['domain_id'], $data['url'], $data['dcdt']);
                        unset($data['url']);
                        unset($data['url_md5']);
                        unset($data['dcdt']);
                        if (!$data['url_id']) {
                            continue;	// 无效URL不入库
                        }
                    } else {
                        continue;		// 无效域名不入库
                    }
                    $arrTimeLog['get_url_id']['end'] = microtime_float();//time count
                    $arrTimeLog['get_url_id']['use'] += $arrTimeLog['get_url_id']['end'] - $arrTimeLog['get_url_id']['start'];
                    //	continue;

                    $arrCountLog['ok'] += 1;//log count

                    $arrTimeLog['file_put_contents']['start'] = microtime_float();//time count
                    file_put_contents($_jsonfile, json_encode($data) . "\r\n", FILE_APPEND);
                    //file_put_contents($_jsonfileHour, json_encode($data) . "\r\n", FILE_APPEND);
                    $arrTimeLog['file_put_contents']['end'] = microtime_float();//time count
                    $arrTimeLog['file_put_contents']['use'] += $arrTimeLog['file_put_contents']['end'] - $arrTimeLog['file_put_contents']['start'];

                }
                $arrTimeLog['file']['end'] = microtime_float();//time count
                $arrTimeLog['file']['use'] += $arrTimeLog['file']['end'] - $arrTimeLog['file']['start'];

            }
            gzclose($handle);
            unlink($file_local_name);
        } else {
            err_log("{$time}|FilesDcclog dofiles_hour||nofile:{$file_local_name}|all:{$arrCountLog['all']}|ok:{$arrCountLog['ok']}", atFLAG);
            return false;
        }

        $arrTimeLog['all']['end'] = microtime_float();//time count
        $arrTimeLog['all']['use'] += $arrTimeLog['all']['end'] - $arrTimeLog['all']['start'];

        foreach ($arrTimeLog as $k1=>$v1){
            err_log("{$time}|FilesDcclog dofiles_hour|time|{$k1}:{$v1['use']}", atFLAG);
        }

        err_log("{$time}|FilesDcclog dofiles_hour|rtime:{$rtime}|end|muse:".ceil(memory_get_usage()/(1024*1024))."M", atFLAG);

        /*		// 跑完23点文件，并且文件数为24个,将json文件导入到mongo
                if (date('H', $unix_rtime) == '23') {// && system("ls -l {$file_local_dir} |grep '^-'|wc -l") == 24
                    $cmd  = "/usr/local/mongodb/bin/mongoimport -h 172.21.0.90 -d {$config['_mongos']['dns_dcclog']['dbname']} -c {$_collect_name}  --file {$_jsonfile}";
                    err_log("{$time}|FilesDcclog dofiles_hour|{$cmd}|start", atFLAG);
                    exec($cmd);
                    err_log("{$time}|FilesDcclog dofiles_hour|{$cmd}|end", atFLAG);

                    //unlink($_jsonfile);
                    err_log("{$time}|FilesDcclog dofiles_hour|unlink|{$_jsonfile}", atFLAG);

                    $cmd  = "rm -rf {$file_local_dir}";
                    exec($cmd);	// 删除24个日志文件
                    err_log("{$time}|FilesDcclog dofiles_hour|{$cmd}", atFLAG);
                }*/

        return true;
    }
}
