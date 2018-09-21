package main

import (
	"os"
	"testing"
)

// TestMainProgram No realiza el test de main.go pasandole el argumento nodebug para que no muestre log, solo ejecuta
// la funcion main
func TestMainProgram(t *testing.T) {
	os.Args = []string{"main.go", "nodebug"}
	main()
}

// TestExtractValues Testea una linea de entrada
func TestExtractValues(t *testing.T) {
	line := "[user:Keanu] [type:inversión] [ammount:1115401358389555]"
	t.Log(extractValues(line))
}

// BenchmarkMainProgram realiza el benchmark de main.go pasandole el argumento nodebug para que no muestre log
// Correr con go test -bench=. -benchtime=20s
func BenchmarkMainProgram(b *testing.B) {
	for n := 0; n < b.N; n++ {
		os.Args = []string{"main.go", "nodebug"}
		main()
	}
}
