package main

import (
	"bufio"
	"errors"
	"io/ioutil"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
)

/*
 * Enunciado:
 * La idea es poder calcular, de los movimientos logueados, lo siguiente:
 * Para cada tipo de operación:
 *   El promedio de movimientos
 *   El usuario con mayor cantidad de movimientos
 * Bonus:
 *   Percentil 95 para cada tipo de operación
 */

type promedio struct {
	promedioParcial int64 // valor parcial del promedio (se calcula sin suma/cant)
	cantidad        int64 // cantidad de amounts, podria sacarse de len(amounts) de raiz
}

// Metrica de Una categoria
type metricaOperacion struct {
	usuarios    map[string]int64 // Mapa de key: usuarioNombre, value: cantidad de operaciones del tipo raiz
	promedio    promedio // struct
	usuarioTop  string // nombreUsuaio del que mas operaciones realizo para esta categoria
	amounts     []int64 // lista de amounts
	perc95thInt int64 // valor de la lista de amounts mas cercano al percentil 95vo
	perc95th    float64 // no se usa, percentil en float
}

func main() {

	// Si el argumento es nodebug, no se muestran valores de log. Para no ensuciar los test y benchmarks
	if len(os.Args) > 1 && os.Args[1] == "nodebug" {
		log.SetFlags(0)
		log.SetOutput(ioutil.Discard)
	}

	// Abro el archivo
	file, err := os.Open("./movements.log")
	if err != nil {
		log.Fatal(err)
	}
	// Difiero cerrar el archivo
	defer file.Close()

	// Estructura de datos
	var mapMetricas map[string]metricaOperacion
	mapMetricas = make(map[string]metricaOperacion)
	cantidadLogsNoParseables := 0

	// Loop linea por linea
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		// Extraigo los valores
		newUser, newType, newAmount, err := extractValues(line)

		if err == nil {

			v, ok := mapMetricas[newType]
			if ok {
				// Existe la Categoria
				_, ok := v.usuarios[newUser]
				if ok {
					// Existe el usuario
					v.usuarios[newUser]++
				} else {
					// Nuevo usuario
					v.usuarios[newUser] = 1
				}
				v.promedio.promedioParcial = partialAverage(v.promedio.promedioParcial, newAmount, v.promedio.cantidad)
				v.promedio.cantidad++
				v.amounts = insertSort(v.amounts, newAmount)
				mapMetricas[newType] = v
			} else {
				// Nueva Categoria
				newMetricaUsuario := make(map[string]int64)
				newMetricaUsuario[newUser] = 1
				newPromedio := promedio{newAmount, 1}
				var newAmounts []int64
				newAmounts = append(newAmounts, newAmount)
				newMetricaOperacion := metricaOperacion{newMetricaUsuario, newPromedio, "", newAmounts, 0, 0}
				mapMetricas[newType] = newMetricaOperacion
			}
		} else {
			cantidadLogsNoParseables++
		}
	}

	// Post Process
	// Operacion Usuario Top
	// Percentil95
	mapMetricas = postProcess(mapMetricas)

	// Muestra las metricas
	mapMetricas = showData(mapMetricas)

	// Errores
	log.Println("Lineas no parseables del archivo: ", strconv.Itoa(cantidadLogsNoParseables))

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

// extractValues extrae los valores user, type y amount de la linea leida del archivo,
// si no puede da error
func extractValues(line string) (string, string, int64, error) {
	newUser := ""
	if strings.Contains(line, "user:") && strings.Contains(line, "] ") {
		start := strings.Index(line, "user:") + 5
		end := strings.Index(line, "] ")
		newUser = line[start:end]
	}
	newType := ""
	if strings.Contains(line, "type:") && strings.Contains(line, "] [ammount:") {
		start := strings.Index(line, "type:") + 5
		end := strings.Index(line, "] [ammount:")
		newType = line[start:end]
	}
	var newNumber int64
	if strings.Contains(line, "ammount:") && strings.Contains(line,"]") {
		start := strings.Index(line, "ammount:") + 8
		end := strings.LastIndex(line, "]")
		newNumber, _ = strconv.ParseInt(line[start:end], 10, 64)
	}

	if newNumber == 0 {
		err := errors.New("")
		return newUser, newType, newNumber, err
	}

	return newUser, newType, newNumber, nil
}

// partialAverage realiza el calculo parcial del promedio
func partialAverage(currentAvg int64, newNumber int64, count int64) int64 {
	currentAvg += (newNumber - currentAvg) / count
	count++
	return currentAvg
}

// insertSort inserta en una lista de int64 un valor de manera ordenada de menor a mayor
func insertSort(data []int64, el int64) []int64 {
	index := sort.Search(len(data), func(i int) bool { return data[i] > el })
	data = append(data, 0)
	copy(data[index+1:], data[index:])
	data[index] = el
	return data
}


// postProcess recibe un mapa de metricaOperacion y calcula el usuario con mayor cantidad de movimientos
// y el percentil 95vo.
func postProcess(mapa map[string]metricaOperacion) (map[string]metricaOperacion) {
	var vAnterior int64
	mult := float64(0.95)
	for k, v := range mapa {
		v.perc95thInt = v.amounts[int64(math.Round(float64(len(v.amounts))*mult))]
		for kk, vv := range v.usuarios {
			if v.usuarioTop == "" {
				v.usuarioTop = kk
				vAnterior = vv
			} else {
				if vAnterior < vv {
					v.usuarioTop = kk
				}
			}
		}
		mapa[k] = v
	}
	return mapa
}

// showData recibe un mapa de metricaOperacion y muestra por consola el resultado del proceso
func showData(mapa map[string]metricaOperacion) (map[string]metricaOperacion) {
	for k, v := range mapa {
		log.Println("Type: ", k)
		log.Println("  Usuario Top:     ", v.usuarioTop)
		log.Println("  promedio:        ", v.promedio.promedioParcial)
		log.Println("  Percentile 95th: ", v.perc95thInt)
	}
	return mapa
}

