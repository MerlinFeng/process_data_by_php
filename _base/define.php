<?php
// +----------------------------------------------------------------------
// | Copyright (c) 2015 All rights reserved.
// +----------------------------------------------------------------------
// | *************
// +----------------------------------------------------------------------

//define('LOCAL_DIR', '/data/wwwroot/autotracking2.2/');	// 本地根目录
define('LOCAL_DIR', '/data/wwwroot/at2.2_test_php7/');	// 本地根目录
define('LOCAL_LOG_DIR', LOCAL_DIR . '_logfiles/');		// 本地日志文件暂存路径
define('LOCAL_JOSN_DIR', LOCAL_DIR . '_jsonfiles/');	// 本地JSON文件暂存路径
define('LOCAL_PAGE_DIR', LOCAL_DIR . '_pagecsvs/');	// 本地page sql文件暂存路径

define('LOG_SERVER_IP', '172.21.0.999'); // 远程日志文件服务器IP

define("JOBKEEPER_GET_URL", 'http://openapi.autodmp.com/jobkeeper/v1.0/index.php/jobtask/status/get?key=1afa34a7f984eeabdbb0a7d494132ee5&group=Autotracking_test'); //jobkeeper url
define("JOBKEEPER_PUT_URL", 'http://openapi.autodmp.com/jobkeeper/v1.0/index.php/jobtask/status/put?key=9b8619251a19057cff70779273e95aa6&group=Autotracking_test'); //jobkeeper url
define("REDIS_IP", '192.168.56.62');
define("JOBKEEPER_URL_GET", 'http://openapi.autodmp.com/jobkeeper/v1.0/jobtask/status/get?key=1afa34a7f984eeabdbb0a7d494132ee5');


$_ENV = [
    // 数据库配置
    '_db_configs'	=> [
        // mysql数据库配置
        '_mysqls'	=> [
            'dns_tagmanager' => [
                'host'		=> '172.21.1.56',
                'user'		=> 'autotracking_test',
                'pass'		=> '*************',
                'port'		=> 3306,
                'dbname'	=> 'auto_tracking'
            ],
            'dns_autotracking' => [
                'host'		=> '172.21.1.56',
                'user'		=> 'autotracking_test',
                'pass'		=> '*************',
                'port'		=> 3306,
                'dbname'	=> 'auto_tracking'
            ],
            'dns_autotracking_t' => [
                'host'		=> '172.21.1.56',
                'user'		=> 'autotracking_test',
                'pass'		=> '*************',
                'port'		=> 3306,
                'dbname'	=> 't_auto_tracking'
            ],
            'dns_autotracking_data' => [
                'host'		=> '172.21.1.56 ',
                'user'		=> 'autotracking_test',
                'pass'		=> '*************',
                'port'		=> 3306,
                'dbname'	=> 'auto_tracking_data'
            ],
            'dns_autotracking_data_t' => [
                'host'		=> '172.21.1.56 ',
                'user'		=> 'autotracking_test',
                'pass'		=> '*************',
                'port'		=> 3306,
                'dbname'	=> 't_auto_tracking_data'
            ],
            'dns_autotracking_data_domain' => [
                'host'		=> '172.21.1.56',
                'user'		=> 'autotracking_test',
                'pass'		=> '*************',
                'port'		=> 3306,
                'dbname'	=> 't_auto_tracking_data_domain'
            ],
            'dns_autotracking_data_page' => [
                'host'		=> '172.21.1.56 ',
                'user'		=> 'autotracking_test',
                'pass'		=> '*************',
                'port'		=> 3306,
                'dbname'	=> 't_auto_tracking_data_page'
            ],
            'dns_autotracking_data_page_in' => [
                'host'		=> '172.21.1.56 ',
                'user'		=> 'autotracking_test',
                'pass'		=> '*************',
                'port'		=> 3306,
                'dbname'	=> 't_auto_tracking_data_page_in'
            ],
            'dns_autotracking_data_page_v24' => [
                'host'		=> '172.21.1.56 ',
                'user'		=> 'autotracking_test',
                'pass'		=> '*************',
                'port'		=> 3306,
                'dbname'	=> 't_auto_tracking_data_page_v2.4'
            ],
            'dns_autotracking_data_outdomain' => [
                'host'		=> '172.21.1.56 ',
                'user'		=> 'autotracking_test',
                'pass'		=> '*************',
                'port'		=> 3306,
                'dbname'	=> 't_auto_tracking_data_outdomain_look'
            ],
            'dns_autotracking_data_outurl' => [
                'host'		=> '172.21.1.56 ',
                'user'		=> 'autotracking_test',
                'pass'		=> '*************',
                'port'		=> 3306,
                'dbname'	=> 't_auto_tracking_data_outurl_look'
            ],
            'dns_autotracking_lead_outdomain' => [
                'host'		=> '172.21.1.56 ',
                'user'		=> 'autotracking_test',
                'pass'		=> '*************',
                'port'		=> 3306,
                'dbname'	=> 't_auto_tracking_data_outdomain_lead'
            ],
            'dns_autotracking_lead_outurl' => [
                'host'		=> '172.21.1.56 ',
                'user'		=> 'autotracking_test',
                'pass'		=> '*************',
                'port'		=> 3306,
                'dbname'	=> 't_auto_tracking_data_outurl_lead'
            ],
            'dns_url' => [
                'host'		=> '172.21.1.56 ',
                'user'		=> 'autotracking_test',
                'pass'		=> '*************',
                'port'		=> 3306,
                'dbname'	=> 't_auto_url'
            ],
            'dns_outurl' => [
                'host'		=> '172.21.1.56 ',
                'user'		=> 'autotracking_test',
                'pass'		=> '*************',
                'port'		=> 3306,
                'dbname'	=> 't_auto_outlink_url'
            ],
            'dns_tmp' => [
                'host'		=> '172.21.1.56 ',
                'user'		=> 'autotracking_test',
                'pass'		=> '*************',
                'port'		=> 3306,
                'dbname'	=> 'auto_url'
            ],
        ],

        // mongodb数据库配置
        '_mongos'   => [
            // 行为日志
            'dns_dcclog_10'    => [
                'host'      => '172.21.0.90',
                'username'  => '',
                'password'  => '',
                'dbname'    => 'dcclogs_10',
                'port'      => 27017,
                'web_id'    => 10,
            ],
            'dns_dcclog_163'    => [
                'host'      => '172.21.1.60',
                'username'  => '',
                'password'  => '',
                'dbname'    => 'dcclogs_163',
                'port'      => 27017,
                'web_id'    => 163,
            ],
            'dns_dcclog_other'    => [
                'host'      => '172.21.1.61',
                'username'  => '',
                'password'  => '',
                'dbname'    => 'dcclogs_other',
                'port'      => 27017,
                'web_id'    => 'other',
            ],
            // 线索日志
            'dns_leads' => [
                'host'      => '172.21.0.90',
                'username'  => '',
                'password'  => '',
                'dbname'    => 'leads_test',
                'port'      => 27017,
            ],
            // 点击日志
            'dns_click' => [
                'host'      => '172.21.0.90',
                'username'  => '',
                'password'  => '',
                'dbname'    => 'clicks_test',
                'port'      => 27017,
            ],
            // 曝光日志
            'dns_impression'    => [
                'host'      => '172.21.0.90',
                'username'  => '',
                'password'  => '',
                'dbname'    => 'impression_test',
                'port'      => 27017,
            ],
            // 事件日志
            'dns_event'    => [
                'host'      => '172.21.0.90',
                'username'  => '',
                'password'  => '',
                'dbname'    => 'event_test',
                'port'      => 27017,
                'web_id'    => 'event',
            ]
        ],
        //redis集群
        '_redis' => [
            '172.21.1.62:6379',
            '172.21.1.62:6380',
            '172.21.1.63:6379',
            '172.21.1.63:6380',
            '172.21.1.64:6379',
            '172.21.1.64:6380'
        ]
    ],

    // 日志文件配置【文件前缀, 文件目录】
    '_log_setting'	=> [
        'dcclog'		=> ['file_prefix' => 'dcclog', 'file_dir' => '/data/wwwroot/logfiles/logpool2/'],	// 行为日志
        'leads'			=> ['file_prefix' => 'ev.dc.ctags.cn', 'file_dir' => '/data/wwwroot/logfiles/tagmanagerlog/'],	// 线索日志
        'click'			=> ['file_prefix' => 'tdc2', 'file_dir' => '/data/wwwroot/logfiles/clickphp2/'],	// 点击日志
        'impression'	=> ['file_prefix' => 'idc2', 'file_dir' => '/data/wwwroot/logfiles/impressionphp2/']	// 曝光日志
    ],

    'mongo_wids' =>[
        10,163
    ],

    // 省份ID列表
    '_provinceid_list' => [110000,120000,130000,140000,150000,210000,220000,230000,310000,320000,330000,340000,350000,360000,370000,410000,420000,430000,440000,450000,460000,500000,510000,520000,530000,540000,610000,620000,630000,640000,650000,710000,810000,820000,0],
];


