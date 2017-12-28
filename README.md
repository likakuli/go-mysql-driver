# go-mysql-driver
go mysql driver based on go-sql-driver/mysql:v1.3 supported bool by parseBool


DSN changes

##### `parseBool`

```
Type:           bool
Valid Values:   true, false
Default:        false
```

`parseBool=true` changes the output type of `BIT` values to `bool` instead of `[]byte` / `string`
