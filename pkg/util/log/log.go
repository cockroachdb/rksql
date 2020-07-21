// A very basic implementation of a logging library

package log

import (
	"context"
	"log"
	"os"
)

func Infof(ctx context.Context, format string, args ...interface{}) {
	log.Printf(format, args...)
}

func Info(ctx context.Context, args ...interface{}) {
	log.Print(args...)
}

func Warningf(ctx context.Context, format string, args ...interface{}) {
	log.Printf(format, args...)
}

func Errorf(ctx context.Context, format string, args ...interface{}) {
	log.Printf(format, args...)
}

func Fatalf(ctx context.Context, format string, args ...interface{}) {
	log.Printf(format, args...)
	os.Exit(1)
}

func Fatal(ctx context.Context, args ...interface{}) {
	log.Print(args...)
	os.Exit(1)
}

func V(depth int) bool {
	return false
}

func WithLogTag(ctx context.Context, name string, value interface{}) context.Context {
	return ctx
}

func Flush() { }
