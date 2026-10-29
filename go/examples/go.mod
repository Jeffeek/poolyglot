module github.com/Jeffeek/poolyglot/go/examples

go 1.26.0

// v0.0.0 + the replace below means the examples always build against the local parent
// sources, never a published version — so this never needs a release-time version bump.
require github.com/Jeffeek/poolyglot/go v0.0.0

require modernc.org/sqlite v1.61.0

require (
	github.com/dustin/go-humanize v1.1.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/mattn/go-isatty v0.0.24 // indirect
	github.com/ncruces/go-strftime v1.1.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	golang.org/x/sys v0.49.0 // indirect
	modernc.org/libc v1.79.1 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.12.1 // indirect
)

replace github.com/Jeffeek/poolyglot/go => ../
