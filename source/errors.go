package source

import "io"

// errEOF 复用标准 io.EOF，源正常结束时返回。
var errEOF = io.EOF
