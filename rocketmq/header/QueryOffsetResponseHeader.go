package header

//QueryOffsetResponseHeader of CustomerHeader
type QueryOffsetResponseHeader struct {
	Offset int64 `json:"offset"`
}

//FromMap convert map[string]interface to struct
func (q *QueryOffsetResponseHeader) FromMap(headerMap map[string]interface{}) {
	//q.Offset = util.StrToInt64WithDefaultValue(headerMap["offset"].(string), -1)
	return
}
