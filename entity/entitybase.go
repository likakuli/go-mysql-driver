package entity

type IInputEntity interface {
	GetInputTypeName() string
	GetProcedureName() string
	GetOutputTypeName() string
	GetEmptyOutputEntity() interface{}
}
