package cdc

type Chunk struct {
	Offset uint64
	Length uint32
	Data   []byte
	Hash   [32]byte
}
