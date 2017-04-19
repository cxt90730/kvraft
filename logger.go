package main

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"io"
	"log"
	"time"
)

// LoggerWithWriter instance a Logger middleware with the specified writter buffer.
// Example: os.Stdout, a file opened in write mode, a socket...
func LoggerWithWriter(out io.Writer, flag string, notlogged ...string) gin.HandlerFunc {
	var skip map[string]struct{}

	if length := len(notlogged); length > 0 {
		skip = make(map[string]struct{}, length)

		for _, path := range notlogged {
			skip[path] = struct{}{}
		}
	}

	return func(c *gin.Context) {
		// Start timer
		start := time.Now()
		path := c.Request.URL.Path
		query := c.Request.URL.RawQuery

		// Process request
		c.Next()

		// Log only when path is not being skipped
		if _, ok := skip[path]; !ok {
			// Stop timer
			end := time.Now()
			latency := end.Sub(start)

			clientIP := c.ClientIP()
			method := c.Request.Method
			statusCode := c.Writer.Status()
			comment := c.Errors.ByType(gin.ErrorTypePrivate).String()

			if query != "" {
				fmt.Fprintf(out, "[kvraft] %v | %3d | %13v | %s | %-7s %s?%s\n%s",
					end.Format("2006/01/02 - 15:04:05"),
					statusCode,
					latency,
					clientIP,
					method,
					path,
					query,
					comment,
				)
			} else {
				fmt.Fprintf(out, "[kvraft] %v | %3d | %13v | %s | %-7s %s\n%s",
					end.Format("2006/01/02 - 15:04:05"),
					statusCode,
					latency,
					clientIP,
					method,
					path,
					comment,
				)
			}

		}
	}
}

func InitServerLog(out io.Writer, flag string) *log.Logger {
	return log.New(out, flag, log.LstdFlags)
}
