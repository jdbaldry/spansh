package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"text/template"

	"github.com/bcicen/jstream"
)

// usage prints the usage of the command line tool.
// Output varies depending on whether the tool is run inside a container or not.
func usage() string {
	const tmpl = `Description:
  Filter JSON streamed from spansh galaxy files.

Arguments:
{{ range .Arguments }}{{ printf "  %s\n" . }}{{ end }}
Usage:
  {{ .Usage }}

Examples:
{{ range .Examples }}{{ printf "  %s\n" . }}{{ end }}`

	type specifics struct {
		Arguments []string
		Usage     string
		Examples  []string
	}
	var spec specifics
	if _, inContainer := os.LookupEnv("IN_CONTAINER"); inContainer {

		spec.Arguments = []string{
			"'GALAXY ABSOLUTE PATH' is the absolute path to the GALAXY file.",
		}
		spec.Usage = `docker run -i -v "<GALAXY ABSOLUTE PATH>:/galaxy.json.gz" galaxy.json.gz`
		spec.Examples = []string{
			`docker run -i -v "$(pwd)/galaxy.json.gz:/galaxy.json.gz" galaxy.json.gz`}
	} else {
		spec.Arguments = []string{"'GALAXY PATH' is the path to the GALAXY file."}
		spec.Usage = os.Args[0]
		spec.Examples = []string{
			fmt.Sprintf("%s galaxy.json.gz", os.Args[0]),
		}
	}

	buf := &bytes.Buffer{}
	_ = template.Must(template.New("usage").Parse(tmpl)).Execute(buf, spec)
	return buf.String()
}

type Coords struct {
	X float64
	Y float64
	Z float64
}
type Body struct {
	ID64   int64
	Name   string
	Coords Coords
	Stars  []Star
}

type Star struct {
	ID64              int64
	BodyID            int64
	Name              string
	SubType           string
	DistanceToArrival float64
}

func decodeStar(logger *log.Logger, val map[string]any) Star {
	star := Star{}

	if id, ok := val["id64"]; ok {
		star.ID64 = int64(id.(float64))
	} else {
		// logger.Println("WARNING: no id64 found")
	}

	if bodyID, ok := val["bodyId"]; ok {
		star.BodyID = int64(bodyID.(float64))
	} else {
		// logger.Println("WARNING: no bodyId found")
	}

	if name, ok := val["name"]; ok {
		star.Name = name.(string)
	} else {
		// logger.Println("WARNING: no name found")
	}

	if subType, ok := val["subType"]; ok {
		star.SubType = subType.(string)
	} else {
		// logger.Println("WARNING: no subType found")
	}

	if dta, ok := val["distanceToArrival"]; ok {
		star.DistanceToArrival = dta.(float64)
	} else {
		// logger.Println("WARNING: no distanceToArrival found")
	}

	return star
}

func decodeBody(logger *log.Logger, val map[string]any) Body {
	body := Body{}

	if id, ok := val["id64"]; ok {
		body.ID64 = int64(id.(float64))
	} else {
		// logger.Println("WARNING: no id64 found")
	}

	if name, ok := val["name"]; ok {
		body.Name = name.(string)
	} else {
		// logger.Println("WARNING: no name found")
	}

	if c, ok := val["coords"].(map[string]any); ok {
		coords := Coords{}
		if x, ok := c["x"]; ok {
			coords.X = x.(float64)
		}
		if y, ok := c["y"]; ok {
			coords.Y = y.(float64)
		}
		if z, ok := c["z"]; ok {
			coords.Z = z.(float64)
		}
		body.Coords = coords
	} else {
		// logger.Println("WARNING: no coords found")
	}

	if bodyCount, ok := val["bodyCount"].(float64); ok {
		if bodyCount := int(bodyCount); bodyCount != 0 {
			body.Stars = make([]Star, bodyCount, bodyCount)

			starCount := 0
			if bodies, ok := val["bodies"]; ok {
				if bodies, ok := bodies.([]any); ok {
					for _, b := range bodies {
						if b, ok := b.(map[string]any); ok {
							if typ, ok := b["type"]; ok {
								if typ, ok := typ.(string); ok {
									if typ == "Star" {
										body.Stars[starCount] = decodeStar(logger, b)
									}
								}
							} else {
								// logger.Println("WARNING: no type found in child body")
							}
						} else {
							// logger.Printf("WARNING: not a map but a %T\n", bodies)
						}
					}
				} else {
					// logger.Printf("WARNING: not a slice but a %T\n", bodies)
				}
			} else {
				// logger.Println("WARNING: no bodies in object even though bodyCount is non-zero")
			}

			body.Stars = body.Stars[:starCount]
		}
	} else {
		// logger.Println("WARNING: no bodyCount found")
	}

	return body
}

func main() {
	logger := log.New(os.Stderr, "", log.Lmsgprefix)

	if len(os.Args) != 2 {
		// logger.Fatalln(usage())
	}

	f, err := os.Open(os.Args[1])
	if err != nil {
		// logger.Fatalf("ERROR: could not open file %q: %v\n", os.Args[1], err)
	}

	zr, err := gzip.NewReader(f)
	if err != nil {
		// logger.Fatalf("ERROR: could not create gzip reader: %v\n", err)
	}

	lr := io.LimitReader(zr, 5*10e6)
	// Decode values at the first level (inside the array).
	decoder := jstream.NewDecoder(lr, 1)
	encoder := json.NewEncoder(os.Stdout)
	for mv := range decoder.Stream() {
		switch val := mv.Value.(type) {
		case map[string]any:
			body := decodeBody(logger, val)
			err := encoder.Encode(body)
			if err != nil {
				// logger.Printf("ERROR: could not marshal JSON: %v\n", err)
			}
		}
	}
}
