package util

import (
	"math/big"
	"reflect"
	"runtime"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/sirupsen/logrus"
)

const (
	ZeroAddress = "0x0000000000000000000000000000000000000000"
)

func ToBlockNumArg(height *big.Int) string {
	if height == nil {
		return "latest"
	}
	pending := big.NewInt(-1)
	if height.Cmp(pending) == 0 {
		return "pending"
	}

	return hexutil.EncodeBig(height)
}

func HandleErrorWithRetry(handle func() error, retryTimes int, interval time.Duration) error {
	inc := 0
	funcName := "unknown"
	if handle != nil {
		if fn := runtime.FuncForPC(reflect.ValueOf(handle).Pointer()); fn != nil {
			funcName = fn.Name()
		}
	}
	for {
		if handle == nil {
			return nil
		}
		err := handle()
		if err == nil {
			return nil
		}
		if inc >= retryTimes {
			return err
		}
		time.Sleep(interval)
		logrus.Warnf("%v handle error with retry: %v", funcName, err)

		inc++
	}
}

var (
	NoMoreRetryErrors = []string{
		"execution reverted",
		"invalid jump destination",
		"invalid opcode",
		"stack limit reached 1024",
	}
)

func HitNoMoreRetryErrors(err error) bool {
	if err == nil {
		return false
	}

	for _, msg := range NoMoreRetryErrors {
		if strings.Contains(err.Error(), msg) {
			return true
		}
	}

	return false
}
