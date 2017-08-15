package types

type EmptyStruct struct{}

var EmptyStructVal = EmptyStruct{}

func (es EmptyStruct) String() string { return "" }
