package testdata

// Basic is the simplest struct that can be used with Genji: No tags, no methods, no comments.
// This must not be generated by the generator.
type Basic struct {
	A    string
	B    int64
	C, D int64
}

// unexportedBasic is like Basic except that it is unexported.
type unexportedBasic struct {
	A    string
	B    int64
	C, D int64
}

// Pk is a struct that selects its own primary key.
// This must not be generated by the generator.
type Pk struct {
	A string
	B int64 `genji:"pk"`
}
