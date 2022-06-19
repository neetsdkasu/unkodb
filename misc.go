// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import (
	"log"
)

var logger = log.New(log.Writer(), "unkodb", log.Flags())

func minValue(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}
