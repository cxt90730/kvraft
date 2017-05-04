package kvraft

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"io"
	"log"
	"time"
)

func InitServerLogger(out io.Writer, flag string) *log.Logger {
	return log.New(out, flag, log.LstdFlags)
}
