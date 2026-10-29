module github.com/Jeffeek/poolyglot/go/examples

go 1.21

// v0.0.0 + the replace below means the examples always build against the local parent
// sources, never a published version — so this never needs a release-time version bump.
require github.com/Jeffeek/poolyglot/go v0.0.0

replace github.com/Jeffeek/poolyglot/go => ../
