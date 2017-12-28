package repository

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"gitlab.gridsum.com/devops-k8s-mgmt-api/apis/entity"
	"gitlab.gridsum.com/devops-k8s-mgmt-api/managers"

	_ "github.com/go-sql-driver/mysql"
)

var (
	db          *sql.DB
	inputCache  *SafeMap
	outputCache *SafeMap
)

//添加一条记录并返回对应记录的自增ID
func InsertAndReturnId(entity entity.IInputEntity) (int, error) {
	fields, err := getInputFields(entity)
	if err != nil {
		return 0, err
	}
	rows, err := db.Query(generateStmtString(entity.GetProcedureName(), len(fields)), generateStmtParams(entity, fields)...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var id int
	if rows.Next() {
		err = rows.Scan(&id)
		if err != nil {
			return 0, err
		}
		return id, nil
	} else {
		return 0, nil
	}
}

//添加一条记录
func Insert(entity entity.IInputEntity) error {
	return exec(entity)
}

//批量添加同一类型的记录
func BatchInsert(entities []entity.IInputEntity) error {
	fields, err := getInputFields(entities[0])
	if err != nil {
		return err
	}
	//Begin函数内部会去获取连接
	tx, _ := db.Begin()
	for i := 0; i < len(entities); i++ {
		//每次循环用的都是tx内部的连接，没有新建连接，效率高
		entity := entities[i]
		_, err = tx.Exec(generateStmtString(entity.GetProcedureName(), len(fields)), generateStmtParams(entity, fields)...)
		if err != nil {
			tx.Rollback()
		}
	}
	//最后释放tx内部的连接
	tx.Commit()
	return nil
}

//删除记录
func Delete(entity entity.IInputEntity) error {
	return exec(entity)
}

//查询
func Query(entity entity.IInputEntity) ([]interface{}, error) {
	fields, err := getInputFields(entity)
	if err != nil {
		return nil, err
	}
	rows, err := db.Query(generateStmtString(entity.GetProcedureName(), len(fields)), generateStmtParams(entity, fields)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	outputFields, err := getOutputFields(entity)
	if err != nil {
		return nil, err
	}
	result := make([]interface{}, 0)
	s := reflect.ValueOf(entity.GetEmptyOutputEntity()).Elem()
	length := s.NumField()
	item := make([]interface{}, length)
	for k, v := range outputFields {
		field := s.FieldByName(v)
		item[k] = field.Addr().Interface()
	}
	for rows.Next() {
		err = rows.Scan(item...)
		if err != nil {
			return nil, err
		}
		result = append(result, s.Interface())
	}
	return result, nil
}

//更新
func Update(entity entity.IInputEntity) error {
	return exec(entity)
}

//还原表，单元测试用
func truncate(name string) error {
	_, err := db.Exec(fmt.Sprintf("TRUNCATE %s", name))
	if err != nil {
		return err
	}
	return nil
}

func exec(entity entity.IInputEntity) error {
	fields, err := getInputFields(entity)
	_, err = db.Exec(generateStmtString(entity.GetProcedureName(), len(fields)), generateStmtParams(entity, fields)...)
	if err != nil {
		return err
	}
	return nil
}

func getInputFields(entity entity.IInputEntity) ([]string, error) {
	return getFields(entity.GetInputTypeName(), entity, inputCache)
}

func getOutputFields(entity entity.IInputEntity) ([]string, error) {
	return getFields(entity.GetOutputTypeName(), entity.GetEmptyOutputEntity(), outputCache)
}

func getFields(typeName string, entity interface{}, cache *SafeMap) ([]string, error) {
	tmp := cache.Get(typeName)
	var fields []string
	length := 0
	var err error

	//通过反射获取struct tag，约定名称为order，值为对应在存储过程中的顺序，从1开始
	if tmp == nil {
		t := reflect.TypeOf(entity).Elem()
		max := t.NumField()
		fields = make([]string, max)
		for i := 0; i < max; i++ {
			field := t.Field(i)
			order := field.Tag.Get("order")
			if order != "" {
				index, err := strconv.Atoi(order)
				if err != nil {
					return nil, nil
				}
				if fields[index-1] != "" {
					return nil, err
				}
				fields[index-1] = field.Name
			}
		}
		length, err = validateFieldOrders(fields)
		if err != nil {
			return nil, err
		}

		if length == 0 {
			return nil, fmt.Errorf("Type %s has no field specified with order.", typeName)
		}
		//note: 保存的是有效字段的长度，没有order标识的已经被过滤掉
		cache.Set(typeName, fields[:length])
	} else {
		fields = tmp.([]string)
	}
	return fields, nil
}

func generateStmtString(procedure string, length int) string {
	return fmt.Sprintf("Call %s(%s)", procedure, strings.TrimRight(strings.Repeat("?,", length), ","))
}

func generateStmtParams(entity entity.IInputEntity, fields []string) []interface{} {
	length := len(fields)
	result := make([]interface{}, length)
	v := reflect.ValueOf(entity).Elem()
	for i := 0; i < length; i++ {
		result[i] = v.FieldByName(fields[i]).Interface()
	}
	return result
}

func validateFieldOrders(fields []string) (int, error) {
	//判断是否序号连续
	count := 0
	for k, v := range fields {
		count = k
		if v == "" {
			for i := k + 1; i < len(fields); i++ {
				if fields[i] != "" {
					return 0, fmt.Errorf("Order is not continues, missing order number %d", i)
				}
			}
		}
	}
	//判断order是否从1开始
	if fields[0] == "" {
		return 0, errors.New("Order is not started with 1")
	}
	//返回有效的字段的长度
	return count + 1, nil
}

func init() {
	managers.Initialize()
	db = managers.ApiManager.DBManager.DB
	inputCache = NewSafeMap()
	outputCache = NewSafeMap()
}
