package database

import (
	"errors"
	//"fmt"
	//"github.com/vuleetu/levelog"
)

var (
	POOL_NOT_FOUND          = errors.New("Pool not found")
	TYPE_CONVERSION_FAILED  = errors.New("Type conversion failed")
	UPDATE_RECORD_NOT_FOUND = errors.New("Recorcomd not found when do update")
)

const (
	DEFAULT_MYSQL_POOL_SIZE = 500
	DEFAULT_REDIS_POOL_SIZE = 500
)

type YAML_MAP map[interface{}]interface{}

//type MysqlRawValue struct {
//	Value string
//	Args  []interface{}
//}

//func NewMysqlRawValue(v string, args ...interface{}) *MysqlRawValue {
//	return &MysqlRawValue{v, args}
//}

type MysqlSpec struct {
	Addr   string
	User   string
	Passwd string
	DBName string `json:"dbname"`
	Pool   int
	Debug  bool
}

type RedisSpec struct {
	Addr string
	Pool int
	Db   int
}
