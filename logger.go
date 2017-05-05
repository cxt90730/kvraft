package kvraft

import (
	"io"
	"log"
)

func InitServerLogger(out io.Writer, flag string) *log.Logger {
	return log.New(out, flag, log.LstdFlags)
}
