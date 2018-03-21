package rocketmq

import (
	"flag"
	"log"
	"net"
	"os"
	"strings"
)

var (
	logger *log.Logger
	debug  bool
)

func init() {
	logger = log.New(os.Stdout, "[rocketMQ]", log.Ldate|log.Ltime|log.Lshortfile)
	flag.BoolVar(&debug, "debug", false, "set this true to output debug log")
	flag.Parse()
}

func Printf(format string, v ...interface{}) {
	if debug {
		logger.Printf(format, v)
	}
}

func Println(v ...interface{}) {
	if debug {
		logger.Println(v)
	}
}

//GetIp4Bytes get ip4 byte array
func GetIp4Bytes() (ret []byte) {
	ip := getIp()
	ret = ip[len(ip)-4:]
	return
}

//GetLocalIp4 get local ip4
func GetLocalIp4() (ip4 string) {
	ip := getIp()
	if ip.To4() != nil {
		currIp := ip.String()
		if !strings.Contains(currIp, ":") && currIp != "127.0.0.1" && isIntranetIpv4(currIp) {
			ip4 = currIp
		}
	}
	return
}

func getIp() (ip net.IP) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return
	}

	for _, face := range interfaces {
		if strings.Contains(face.Name, "lo") {
			continue
		}
		addrs, err := face.Addrs()
		if err != nil {
			return
		}

		for _, addr := range addrs {
			if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
				if ipnet.IP.To4() != nil {
					currIp := ipnet.IP.String()
					if !strings.Contains(currIp, ":") && currIp != "127.0.0.1" && isIntranetIpv4(currIp) {
						ip = ipnet.IP
					}
				}
			}
		}
	}
	return
}

func isIntranetIpv4(ip string) bool {
	if strings.HasPrefix(ip, "192.168.") || strings.HasPrefix(ip, "169.254.") {
		return true
	}
	return false
}
