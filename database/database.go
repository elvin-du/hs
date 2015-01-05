package database

import (
	"encoding/json"
	"errors"
	//"github.com/vuleetu/gorp"
	"github.com/vuleetu/levelog"
	"github.com/vuleetu/pools"
	"github.com/ziutek/mymysql/autorc"
	_ "github.com/ziutek/mymysql/thrsafe" // Thread safe engine
	"gopkg.in/redis.v2"
	"pickup/config"
	"time"
)

func init() {
	levelog.SetFlags(levelog.Lshortfile)
}

const MAX_TRIED = 3

var memPool *pools.RoundRobin
var (
	mysqlPools   = map[string]*pools.RoundRobin{}
	redisClients = map[string]*redis.Client{}
)

func Start() {
	StartMysql()
	StartRedis()
}

func StartMysql() {
	var mysqlSpecs map[interface{}]interface{}
	err := config.Get("mysql", &mysqlSpecs)
	if err != nil {
		levelog.Fatal(err)
	}

	levelog.Debug(mysqlSpecs)
	for name, spec := range mysqlSpecs {
		if name != "debug" {
			newMysqlPool(name.(string), spec.(map[interface{}]interface{}))
		}
	}
}

type MysqlResource struct {
	kind string
	db   *autorc.Conn
	spec *MysqlSpec
	//orm  *gorp.DbMap
}

func (r *MysqlResource) Db() *autorc.Conn {
	return r.db
}

func (r *MysqlResource) Close() {
	r.db.Raw.Close()
}

func (r *MysqlResource) IsClosed() bool {
	return r.db.Raw.Ping() != nil
}

func (r *MysqlResource) Release() {
	levelog.Info("release", r.kind, "resource")
	mysqlPools[r.kind].Put(r)
}

func newMysqlFactory(kind string, db *autorc.Conn, spec *MysqlSpec) pools.Factory {
	return func() (pools.Resource, error) {
		//conn := db.Clone()
		//err := conn.Raw.Ping()
		//if nil != err {
		//	levelog.Error("ping failed", err)
		//}

		conn := autorc.New("tcp", "", spec.Addr, spec.User, spec.Passwd, spec.DBName)
		conn.Debug = spec.Debug

		err := conn.Raw.Connect()
		if err != nil {
			levelog.Fatal("Connect to mysql failed", err, ", info", spec)
		} else {
			levelog.Debug("connect success")
		}
		return &MysqlResource{kind, conn, spec}, err
	}
}

func newMysqlPool(name string, rawSpec YAML_MAP) {
	var spec MysqlSpec
	err := unmarshal(rawSpec, &spec)
	if err != nil {
		levelog.Fatal(err)
	}
	levelog.Debug("Mysql setting for", name, ":", spec)

	if spec.Addr == "" {
		spec.Addr = "localhost"
	}

	if spec.Pool < 1 {
		spec.Pool = DEFAULT_MYSQL_POOL_SIZE
	}

	levelog.Debug("Final mysql setting for", name, ":", spec)
	conn := autorc.New("tcp", "", spec.Addr, spec.User, spec.Passwd, spec.DBName)
	conn.Debug = spec.Debug

	err = conn.Raw.Connect()
	if err != nil {
		levelog.Fatal("Connect to mysql failed", err, ", info", spec)
	} else {
		levelog.Debug("connect success")
	}

	p := pools.NewRoundRobin(spec.Pool, time.Minute*10)
	p.Open(newMysqlFactory(name, conn, &spec))
	mysqlPools[name] = p
}

//func (r *MysqlResource) Update(table string, q map[string]interface{}, where string, where_args ...interface{}) error {
//	if len(q) < 1 {
//		levelog.Warn("No need to do update", q)
//		return nil
//	}

//	var (
//		sql   bytes.Buffer
//		args  = make([]interface{}, 0, len(q)+len(where_args))
//		first = true
//	)

//	sql.WriteString("UPDATE ")
//	sql.WriteString(table)
//	sql.WriteString(" SET ")

//	for k, v := range q {
//		if !first {
//			sql.WriteString(",")
//		}
//		first = false
//		sql.WriteString(k)

//		if rv, ok := v.(*MysqlRawValue); ok {
//			sql.WriteString(" = ")
//			sql.WriteString(rv.Value)
//			args = append(args, rv.Args...)
//		} else {
//			sql.WriteString(" = ?")
//			args = append(args, v)
//		}
//	}

//	if where != "" {
//		sql.WriteString(" WHERE ")
//		sql.WriteString(where)
//		args = append(args, where_args...)
//	}

//	levelog.Debug("Execute query:", sql.String())
//	rs, err := r.orm.Exec(sql.String(), args...)
//	if err != nil {
//		return err
//	}

//	if n, err := rs.RowsAffected(); err != nil {
//		levelog.Error(err)
//		return err
//	} else if n < 1 {
//		levelog.Warn("Affected rows less than 1")
//		return UPDATE_RECORD_NOT_FOUND
//	}

//	return nil
//}

func StartRedis() {
	var redisSpecs map[interface{}]interface{}
	err := config.Get("redis", &redisSpecs)
	if err != nil {
		levelog.Fatal(err)
	}

	levelog.Debug(redisSpecs)
	for name, spec := range redisSpecs {
		if name != "debug" {
			newRedisPool(name.(string), spec.(map[interface{}]interface{}))
		}
	}
}

func newRedisPool(name string, rawSpec YAML_MAP) {
	var spec RedisSpec
	err := unmarshal(rawSpec, &spec)
	if err != nil {
		levelog.Fatal(err)
	}
	levelog.Debug("Redis setting for", name, ":", spec)

	if spec.Addr == "" {
		levelog.Fatal("Addr not set for", name)
	}

	if spec.Pool < 1 {
		spec.Pool = DEFAULT_REDIS_POOL_SIZE
	}

	if spec.Db < 0 {
		spec.Db = 0
	}

	levelog.Debug("Final redis setting for", name, ":", spec)
	var opt redis.Options
	opt.Addr = spec.Addr
	opt.DB = int64(spec.Db)
	opt.PoolSize = spec.Pool

	client := redis.NewTCPClient(&opt)
	err = client.Ping().Err()
	if err != nil {
		levelog.Fatal("Connect to redis failed", err, ", info", spec)
	}

	redisClients[name] = client
}

func GetMRedis() (*redis.Client, error) {
	return GetRedis("master")
}

func GetRedis(name string) (*redis.Client, error) {
	client, ok := redisClients[name]
	if !ok {
		levelog.Error("Pool not found: #", name, ", type: redis")
		return nil, POOL_NOT_FOUND
	}

	levelog.Info("Got client, #", name, ", type: redis")
	return client, nil
}

func unmarshal(data YAML_MAP, target interface{}) error {
	var m = map[string]interface{}{}
	for k, v := range data {
		m[k.(string)] = v
	}

	str, err := json.Marshal(m)
	if err != nil {
		return err
	}

	return json.Unmarshal(str, target)
}

func GetMMySQL() (*MysqlResource, error) {
	return GetMySQL("master")
}

func GetMySQL(name string) (*MysqlResource, error) {
	//hard code to master
	masterPool, ok := mysqlPools[name]
	if !ok {
		levelog.Error("Pool not found: #", name, ", type: mysql")
		return nil, POOL_NOT_FOUND
	}

	levelog.Info("#", name, "#", masterPool.StatsJSON(), ", type: mysql")
	r, err := masterPool.Get()
	if err != nil {
		levelog.Error("Get resource from pool failed", err, "#", name, ", type: mysql")
		return nil, err
	}

	mr, ok := r.(*MysqlResource)
	if !ok {
		levelog.Error("Convert resource to mysql session failed, #", name, ", type: mysql")
		return nil, TYPE_CONVERSION_FAILED
	}

	levelog.Info("Check if mongo connection is alive, #", name)
	if err = mr.Db().Raw.Ping(); err != nil {
		levelog.Warn("Ping failed", err, "#", name)
		levelog.Warn("Try reconect, #", name)
		err = mr.Db().Raw.Reconnect()
		if nil != err {
			levelog.Error("Reconect reached maximum times:", mr.Db().MaxRetries, "#", name)
			return nil, errors.New("Can not establish connection to mysql")
		}
	}

	levelog.Info("Ping success, #", name)
	levelog.Info("Mysql connection is alive now, #", name)
	levelog.Info("Got resource, #", name)
	return mr, nil
}
