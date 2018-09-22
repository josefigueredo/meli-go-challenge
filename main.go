package main

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/caio/go-tdigest"
	"io/ioutil"
	"log"
	"os"
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

 /*
  * Documentacion:
  * Average/Nean
  *  http://www.heikohoffmann.de/htmlthesis/node134.html
  * Percentile
  *  https://www.stevenengelhardt.com/postseries/calculating-percentiles-on-streaming-data/
  *  https://mapr.com/blog/better-anomaly-detection-t-digest-whiteboard-walkthrough/
  */


type promedio struct {
	promedioParcial float64 // valor parcial del promedio (se calcula sin suma/cant)
	cantidad        int64   // cantidad de amounts, podria sacarse de len(amounts) de raiz
}

// Metrica de Una categoria
type metricaOperacion struct {
	usuarios   map[string]int64 // Mapa de key: usuarioNombre, value: cantidad de operaciones del tipo raiz
	promedio   promedio         // struct
	usuarioTop string           // nombreUsuaio del que mas operaciones realizo para esta categoria
	lib        tdigest.TDigest  // lib que implementa streamed percentile, via ranged quantiles
	perc95th   float64          // percentil 95vo en float
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
				u, ok := v.usuarios[newUser]
				if ok {
					// Existe el usuario
					v.usuarios[newUser] = u + 1
				} else {
					// Nuevo usuario
					v.usuarios[newUser] = 1
				}
				v.promedio.promedioParcial, v.promedio.cantidad = partialAverage(v.promedio.promedioParcial, newAmount, v.promedio.cantidad)
				v.lib.Add(newAmount)
				mapMetricas[newType] = v
			} else {
				// Nueva Categoria
				newMetricaUsuario := make(map[string]int64)
				newMetricaUsuario[newUser] = 1
				newPromedio := promedio{newAmount, 1}
				newPromedio.promedioParcial, newPromedio.cantidad = partialAverage(newPromedio.promedioParcial, newAmount, newPromedio.cantidad)
				t, _ := tdigest.New()
				t.Add(newAmount)
				newMetricaOperacion := metricaOperacion{newMetricaUsuario, newPromedio, "", *t, 0}
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
func extractValues(line string) (string, string, float64, error) {
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
	var newNumber float64
	if strings.Contains(line, "ammount:") && strings.Contains(line, "]") {
		start := strings.Index(line, "ammount:") + 8
		end := strings.LastIndex(line, "]")
		newNumber, _ = strconv.ParseFloat(line[start:end], 64)
	}

	if newNumber == 0 {
		err := errors.New("")
		return newUser, newType, newNumber, err
	}

	return newUser, newType, newNumber, nil
}

// partialAverage realiza el calculo parcial del promedio
func partialAverage(currentAvg float64, newNumber float64, count int64) (float64, int64) {
	currentAvg += ((newNumber - currentAvg) / float64(count))
	count++
	return currentAvg, count
}

// postProcess recibe un mapa de metricaOperacion y calcula el usuario con mayor cantidad de movimientos
// y el percentil 95vo.
func postProcess(mapa map[string]metricaOperacion) map[string]metricaOperacion {
	var vAnterior int64
	for k, v := range mapa {
		vAnterior = 0
		v.perc95th = v.lib.Quantile(0.95)
		for kk, vv := range v.usuarios {
			if v.usuarioTop == "" {
				v.usuarioTop = kk
				vAnterior = vv
			} else {
				if vAnterior < vv {
					v.usuarioTop = kk
					vAnterior = vv
				}
			}
		}
		mapa[k] = v
	}
	return mapa
}

// showData recibe un mapa de metricaOperacion y muestra por consola el resultado del proceso
func showData(mapa map[string]metricaOperacion) map[string]metricaOperacion {
	for k, v := range mapa {
		log.Println("Type: ", k)
		log.Println("  Usuario Top:     ", v.usuarioTop)
		log.Println("  Promedio:        ", fmt.Sprintf("%.f", v.promedio.promedioParcial))
		log.Println("  Percentile 95th: ", fmt.Sprintf("%.f", v.perc95th))
	}
	return mapa
}
