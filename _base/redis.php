<?php
/**
 * redis集群
 * Created by PhpStorm.
 * User: fengqiang
 * Date: 2017/1/16
 * Time: 10:38
 */

class RedisC
{
    /**
     * @var \Redis
     */
    private static $_redis = null;
    /**
     * 获取数据类库对象
     * @return \Redis
     */
    public static function getRedis()
    {
        $config = $_ENV['_db_configs'];
        if (self::$_redis instanceof \RedisCluster == false) {
            self::$_redis = new RedisCluster(NULL,$config['_redis']);
//            self::$_redis->connect(REDIS_IP, 6379);
//            self::$_redis->connect('172.21.0.111', 6379);
        }
        return self::$_redis;
    }
    /**
     * 静态魔术方法
     * @param string $name 调用的方法名
     * @param string $args 方法的参数
     * @return void
     */
   /* public static function __callStatic($name, $args)
    {
        $callback = [
            self::getRedis(),
            $name
        ];
        return call_user_func_array($callback, $args);
    }*/

}

