<?php
/**
 * Created by PhpStorm.
 * User: fengqiang
 * Date: 2017/1/10
 * Time: 16:43
 */
class mongodo {

    protected static $instance;

    protected $db_link;

    protected $_mongo;

    protected $collection;

    /**
     * 构造函数
     *
     * @access public
     * @param array $params 数据库连接参数,如主机名,数据库用户名,密码等
     * @return boolean
     */
    public function __construct(array $params = null) {

        if (!extension_loaded('mongo')) {
            die('The mongo extension must be loaded!');
        }

        if (empty($params)) {
            $params['dsn'] 		= 'mongodb://localhost:27017';
            $params['option'] 	= array('connect' => true);
        } else {
            //分析dsn信息
            if (!isset($params['dsn'])) {
                $params['dsn'] = 'mongodb://'. (trim($params['host']) ? $params['host'] : "127.0.0.1") . ':' . ($params['port'] ? $params['port'] : '27017');
            }
            $params['option'] = (!isset($params['option'])) ? array('connect' => true) : trim($params['option']);
        }

        //实例化mongo
        //$this->_mongo = new Mongo($params['dsn'], $params['option']);

        if (class_exists("MongoClient")) {
            $this->_mongo = new MongoClient($params['dsn'], $params['option']);
        }else {
            $this->_mongo = new Mongo($params['dsn'], $params['option']);
        }


        if ($params['dbname']) {
            $this->db_link = $this->_mongo->selectDB($params['dbname']);
        }

        //用户登录
        if ($params['username'] && $params['password']) {
            $result = $this->db_link->authenticate($params['username'], $params['password']);
            if (!$result) {
                die('Mongo Auth Failed: bad user or password.');
            }
        }
        return true;
    }

    /**
     * Select Collection
     *
     * @param string $collection
     * @return MogoCollection
     */
    public function collection($collection)
    {
        return $this->collection =  $this->db_link->selectCollection($collection);
    }


    /**
     * 根据条件查找多条数据，分页
     * @param array $query 查询条件
     * @param array $sort  排序条件 array('age' => -1, 'username' => 1)
     * @param int   $limit 页面
     * @param int   $limit 查询到的数据条数
     * @param array $fields返回的字段
     */
    public function find($collnections, $query, $sort = array(), $skip = 0, $limit = 0, $fields = array()) {
        $cursor = $this->collection($collnections)->find($query, $fields);
        if ($sort)  $cursor->sort($sort);
        if ($skip)  $cursor->skip($skip);
        if ($limit) $cursor->limit($limit);
        return iterator_to_array($cursor);
    }

    /**
     * 查询一条记录
     *
     * @access public
     * @param string $collnections	集合名称(相当于关系数据库中的表)
     * @param array	 $query			查询的条件array(key=>value) 相当于key=value
     * @param array  $filed			需要列表的字段信息array(filed1,filed2)
     * @return array
     */
    public function fetch_row($collnections, $query, $filed=array()) {
        return $this->collection($collnections)->findOne($query, $filed);
    }

    public function findOne($collnections, $query, $filed=array()) {
        return $this->fetch_row($collnections, $query, $filed=array());
    }


    /**
     * 查询多条记录
     *
     * @access public
     * @param string $collnections	集合名称(相当于关系数据库中的表)
     * @param array	 $query			查询的条件array(key=>value) 相当于key=value
     * @param array  $filed			需要列表的字段信息array(filed1,filed2)
     * @return array
     */
    public function fetch_all($collnections, $query = array(), $filed=array()) {

        $result = array();
        $cursor = $this->collection($collnections)->find($query, $filed);
        while ($cursor->hasNext()) {
            $result[] = $cursor->getNext();
        }

        return $result;
    }
    public function findAll($collnections, $query = array(), $filed=array()) {
        return $this->fetch_all($collnections, $query = array(), $filed=array());
    }

    /**
     * 插入数据
     *
     * @access public
     * @param string	$collnections	集合名称(相当于关系数据库中的表)
     * @param array		$data_array
     * @return boolean
     */
    public function insert($collnections, $data_array) {
        return $this->collection($collnections)->insert($data_array);
    }

    /**
     * 更改数据
     *
     * @access public
     * @param string	$collnections	集合名称(相当于关系数据库中的表)
     * @param array		$query
     * @param array		$update_data
     * @param array     $options
     * @return boolean
     */
    public function update($collection, $query, $update_data, $options=array('safe'=>true,'multiple'=>true)) {
        return $this->collection($collection)->update($query, $update_data, $options);
    }

    /**
     * 删除数据
     *
     * @access public
     * @param string	$collnections	集合名称(相当于关系数据库中的表)
     * @param array	    $query
     * @param array     $option
     * @return unknow
     */
    public function delete($collection, $query, $option=array("justOne"=>false)) {
        return $this->collection($collection)->remove($query, $option);
    }

    /*    public function drop($collection) {
            return $this->collection($collection)->drop();
        }
        public function collectionSize($collection) {
            $arrt['dataSize'] = $this->collection($collection)->dataSize();
            $arrt['storageSize'] = $this->collection($collection)->storageSize();
            $arrt['totalIndexSize'] = $this->collection($collection)->totalIndexSize();
            $arrt['totalSize'] = $this->collection($collection)->totalSize();
            return $arrt;
        }
        public function dbStats() {
            $arrt = $this->stats();
            return $arrt;
        }*/

    /**
     * MongoId
     *
     * @author ColaPHP
     * @param string $id
     * @return MongoId
     */
    public static function id($id = null)
    {
        return new MongoId($id);
    }

    /**
     * MongoTimestamp
     *
     * @author ColaPHP
     * @param int $sec
     * @param int $inc
     * @return MongoTimestamp
     */
    public static function Timestamp($sec = null, $inc = 0)
    {
        if (!$sec) $sec = time();
        return new MongoTimestamp($sec, $inc);
    }

    /**
     * GridFS
     *
     * @author ColaPHP
     * @return MongoGridFS
     */
    public function gridFS($prefix = 'fs')
    {
        return $this->db_link->getGridFS($prefix);
    }

    /**
     * 获取集合对象
     */
    public function getCollection() {
        return $this->collection;
    }

    /**
     * 获取DB对象
     */
    public function getDb() {
        return $this->db_link;
    }

    /**
     * 数据统计
     */
    public function count($collection) {
        return $this->collection($collection)->count();
    }

    /**
     * 创建单一索引
     * @return bool
     */
    public function index($collection,$filed){
        if(!$collection || !is_array($filed)) return false;
        return $this->collection($collection)->ensureIndex($filed);
    }

    /**
     * 错误信息
     */
    public function error() {
        return $this->db_link->lastError();
    }
    /**
     * 析构函数
     *
     * @access public
     * @return void
     */
    public function __destruct() {
        if ($this->_mongo) {
            $this->_mongo->close();
        }
    }

    /**
     * 本类单例实例化函数
     *
     * @access public
     * @param array $params 数据库连接参数,如数据库服务器名,用户名,密码等
     * @return object
     */
    public static function getInstance($params) {

        if (!self::$instance) {
            self::$instance = new self($params);
        }

        return self::$instance;
    }
}

