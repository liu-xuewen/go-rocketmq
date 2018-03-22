package header

//GetRouteInfoRequestHeader of CustomerHeader
type GetRouteInfoRequestHeader struct {
	Topic string `json:"topic"`
}

//FromMap convert map[string]interface to struct
func (g *GetRouteInfoRequestHeader) FromMap(headerMap map[string]interface{}) {
	return
}

//func (self *GetRouteInfoRequestHeader) MarshalJSON() ([]byte, error) {
//	var buf bytes.Buffer
//	buf.WriteString("{\"topic\":\"")
//	buf.WriteString(self.topic)
//	buf.WriteString("\"}")
//	return buf.Bytes(), nil
//}
