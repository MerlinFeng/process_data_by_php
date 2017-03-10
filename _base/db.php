<?php
// +----------------------------------------------------------------------
// | Copyright (c) 2015 All rights reserved.
// +----------------------------------------------------------------------
// | Author: Jerry.fang <fangzq@yiche.com>
// +----------------------------------------------------------------------

/**
 * 数据库实例、数据库连接实例、查询实例（每一个查询、操作）
 *
 * 步骤：操作数据库之前先实例化数据库，每一个查询之前先实例化数据库连接，释放上次查询缓存
 * 
 */

class db
{
	// 数据库实例
	static private  $instance   =  array();
	// 当前数据库实例
    static private  $_instance  =  null;
    // 数据库连接ID 支持多个连接
    private $linkID     = array();
    // 当前连接ID
    private $_linkID    = null;
    
    // 当前配置
    private $_config = array();
    // 当前DNS
    private $_dns = null;
    // 当前PDO操作实例
    private $PDOStatement = null;

    public function __construct($config = array())
    {
    	$this->_dns = "mysql:dbname={$config['dbname']};host={$config['host']}";
    	$this->_config = $config;
        $this->connect();
    }

    /**
     * 实例化数据库链接
     */
    static function getInstance($config = array())
    {
    	$md5 = md5(serialize($config));
    	if(!isset(self::$instance[$md5]) || !self::$instance[$md5]) {
    		self::$instance[$md5]   =   new db($config);
    	}
    	self::$_instance = self::$instance[$md5];
    	return self::$_instance;
    }

    /**
     * 链接数据库
     */
    private function connect()
    {
    	$linkNumStr = md5($this->_dns);
    	if (!isset($this->linkID[$linkNumStr]) && empty($this->linkID[$linkNumStr])) {
    		$this->linkID[$linkNumStr] = new PDO($this->_dns, $this->_config['user'], $this->_config['pass'], [PDO::ATTR_PERSISTENT => true]);
    		$this->linkID[$linkNumStr]->exec("SET NAMES utf8");
    	}
    	$this->_linkID = $this->linkID[$linkNumStr];
    	return $this->_linkID;
    }

    /**
     * 执行查询
     * @param  string $sql [description]
     * @return [type]      [description]
     */
    public function query($sql = '')
    {
    	if (!$sql) {
    		return $this->_linkID;
    	}
    	// 释放之前的查询缓存
//    	if ( !empty($this->PDOStatement) ) $this->free();

//    	$this->connect();

    	$this->PDOStatement = $this->_linkID->prepare($sql);
    	$result = $this->PDOStatement->execute();
    	if (!$result) {
    		throw new Exception(implode("||", $this->PDOStatement->errorInfo()) . "||sql=" . $sql, 2);
    	}
    	return $this->PDOStatement;
    }

    public function getAll($sql = '')
    {
    	return $this->query($sql)->fetchAll(PDO::FETCH_ASSOC);
    }

    /**
     * 获取第1行的结果数据
     *
     * @param string $sql SQL 语句
     * @return array 一维数组
     */
    public function getOne($sql = '')
    {
        return $this->query($sql)->fetch(PDO::FETCH_ASSOC);
    }

    /**
     * 获取第1列的结果数据
     *
     * @param string $sql SQL 语句
     * @return array 一维数组
     */
    public function getCol($sql = '')
    {
        return $this->query($sql)->fetchAll(PDO::FETCH_COLUMN, 0);
    }

    /**
     * 插入数据， 若数据库表没有主键，则可以使用change()
     *
     * @param string $tbl 表名
     * @return int 返回 Last id
     */
    public function add($sql)
    {
        $this->query($sql);
        return intval($this->_linkID->lastInsertId());
    }

    /**
     * 修改数据
     *
     * @param string $sql SQL 语句
     * @return int 影响行数
     */
    public function change($sql)
    {
        return $this->query($sql)->rowCount();
    }

    /**
     * 释放查询结果
     * @access public
     */
    public function free() {
        $this->PDOStatement = null;
    }

    /**
     * 关闭数据库
     * @access public
     */
    public function close() {
        $this->_linkID = null;
    }

    /**
     * 析构方法
     * @access public
     */
    public function __destruct() {
        // 释放查询
        if ($this->PDOStatement){
//            $this->free();
        }
        // 关闭连接
//        $this->close();
    }
}

