package errl

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

type ErrorWithLocation struct {
	location string
	err      error
}

// Error returns the string including location and error
func (e *ErrorWithLocation) Error() string {
	return fmt.Sprintf("%s: %s", e.location, e.err.Error())
}

func (e *ErrorWithLocation) Unwrap() error {
	return e.err
}

// Naked returns the error without location info, possibly to use in a logging system which already includes location info
func (e *ErrorWithLocation) Naked() error {
	return e.err
}

func Errorf(format string, a ...any) *ErrorWithLocation {
	return buildError(fmt.Errorf(format, a...))
}

func Error(err error) *ErrorWithLocation {
	if err == nil {
		return nil
	}
	return buildError(err)
}

func buildError(theError error) *ErrorWithLocation {
	if theError == nil {
		return nil
	}
	var pc uintptr
	var pcs [1]uintptr
	// skip [runtime.Callers, this function, this function caller]
	runtime.Callers(3, pcs[:])
	pc = pcs[0]

	fs := runtime.CallersFrames([]uintptr{pc})
	f, _ := fs.Next()

	dir, file := filepath.Split(f.File)

	// Trim the root directory prefix to get the relative directory of the source file
	fullFileName := f.File
	cwd, err := os.Getwd()
	if err == nil {
		relativeDir, err := filepath.Rel(cwd, filepath.Dir(dir))
		if err == nil {
			fullFileName = filepath.Join(relativeDir, file)
		}
	}

	funcName := f.Function
	idx := strings.LastIndex(funcName, ".")
	if idx > 0 {
		funcName = funcName[idx+1:]
	}

	location := fmt.Sprintf("%s:%d %s", fullFileName, f.Line, funcName)

	e := &ErrorWithLocation{
		location: location,
		err:      theError,
	}

	return e

}

// Errorf2 provides error location for the caller of the caller of this function.
// This enables error handling patterns where a function is called when
// an error occurs, and this function uses Errorf2 to wrap the error.
func Errorf2(format string, a ...any) *ErrorWithLocation {
	return buildError2(fmt.Errorf(format, a...))
}

// Error2 provides error location for the caller of the caller of this function.
// This enables error handling patterns where a function is called when
// an error occurs, and this function uses Error2 to wrap the error.
func Error2(err error) *ErrorWithLocation {
	if err == nil {
		return nil
	}
	return buildError2(err)
}

func buildError2(theError error) *ErrorWithLocation {
	if theError == nil {
		return nil
	}
	var pc uintptr
	var pcs [1]uintptr
	// skip runtime.Callers, this function (buildError2), this function caller (Error2 or Errorf2),
	// and the caller of Error2 or Errorf2, which is a function to manage errors used at the location of the error.
	runtime.Callers(4, pcs[:])
	pc = pcs[0]

	fs := runtime.CallersFrames([]uintptr{pc})
	f, _ := fs.Next()

	dir, file := filepath.Split(f.File)

	// Trim the root directory prefix to get the relative directory of the source file
	fullFileName := f.File
	cwd, err := os.Getwd()
	if err == nil {
		relativeDir, err := filepath.Rel(cwd, filepath.Dir(dir))
		if err == nil {
			fullFileName = filepath.Join(relativeDir, file)
		}
	}

	funcName := f.Function
	idx := strings.LastIndex(funcName, ".")
	if idx > 0 {
		funcName = funcName[idx+1:]
	}

	location := fmt.Sprintf("%s:%d %s", fullFileName, f.Line, funcName)

	e := &ErrorWithLocation{
		location: location,
		err:      theError,
	}

	return e

}

type SeverityLevel int

const (
	DebugM SeverityLevel = iota
	InfoM
	WarnM
	ErrorM
)

func (s SeverityLevel) String() string {
	switch s {
	case DebugM:
		return "Debug"
	case InfoM:
		return "Info"
	case WarnM:
		return "Warn"
	case ErrorM:
		return "Error"
	default:
		return "Unknown"
	}
}

type ValidationMessage struct {
	Severity SeverityLevel
	Message  string
}

type ValidationMessages []ValidationMessage

func (m *ValidationMessages) Add(severity SeverityLevel, message string) {
	*m = append(*m, ValidationMessage{Severity: severity, Message: message})
}

func (m *ValidationMessages) Addf(severity SeverityLevel, format string, args ...any) {
	m.Add(severity, fmt.Sprintf(format, args...))
}

func (m *ValidationMessages) String() string {
	var ss strings.Builder
	for _, v := range *m {
		ss.WriteString(v.Severity.String())
		ss.WriteString(": ")
		ss.WriteString(v.Message)
		ss.WriteString("\n")
	}
	return ss.String()
}
