// TODO
// 1. Download and dedupe postalCodes
// 2. DRY populate functions (?)
// 3. Download and unzip Geoname file
// 4. Dockerize local dev
// 5. Index
// 6. Test data

package main

import (
	"fmt"
	"sync"
	"github.com/spf13/viper"
	"github.com/framis/gocity/config"
	"github.com/framis/gocity/lib"
	"strings"
)

type admin struct {
	code string
	name string
}

type country struct {
	iso string
	name string
}
type postalCode struct {
	country string
	postalCode string
	name string
	admin1Code string
}

type Geoname struct {
	admin1Map        map[string]admin
	admin2Map        map[string]admin
	countryMap       map[string]country
	postalCodeMap    map[string]postalCode
	recordErrChan      chan error
}

func NewGeoname(recordErrChan chan error) *Geoname {
	g := Geoname{recordErrChan: recordErrChan}
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		g.populateAdmin1()
		wg.Done()
	}()
	go func() {
		g.populateAdmin2()
		wg.Done()
	}()
	go func() {
		g.populateCountry()
		wg.Done()
	}()
	wg.Wait()
	return &g
}

func (g *Geoname) populateAdmin1() {
	g.admin1Map = make(map[string]admin)
	recordChan := make(chan([]string))
	URL := viper.GetString("geonames.admin1")

	go csv.ReadRemoteCSV(URL, recordChan, g.recordErrChan)
	for record := range recordChan {
		if len(record) < 2 {
			g.recordErrChan <- csv.NewRecordError(URL, record)
			continue
		}
		g.admin1Map[record[0]] = admin{code: record[0], name: record[1]}
	}
}

func (g *Geoname) populateAdmin2() {
	g.admin2Map = make(map[string]admin)
	recordChan := make(chan([]string))
	URL := viper.GetString("geonames.admin2")

	go csv.ReadRemoteCSV(URL, recordChan, g.recordErrChan)
	for record := range recordChan {
		isInvalid := len(record) < 2
		if isInvalid {
			g.recordErrChan <- csv.NewRecordError(URL, record)
			continue
		}
		g.admin2Map[record[0]] = admin{code: record[0], name: record[1]}
	}
}

func (g *Geoname) populateCountry() {
	g.countryMap = make(map[string]country)
	recordChan := make(chan([]string))
	URL := viper.GetString("geonames.country")

	go csv.ReadRemoteCSV(URL, recordChan, g.recordErrChan)
	for record := range recordChan {
		isInvalid := len(record) < 5 || strings.HasPrefix(record[0], "#")
		if isInvalid {
			g.recordErrChan <- csv.NewRecordError(URL, record)
			continue
		}
		g.countryMap[record[0]] = country{iso: record[0], name: record[4]}
	}
}

func (g *Geoname) populateostalCode() {
	g.postalCodeMap = make(map[string]postalCode)
	recordChan := make(chan([]string))
	URL := viper.GetString("geonames.country")

	go csv.ReadRemoteCSV(URL, recordChan, g.recordErrChan)
	for record := range recordChan {
		isInvalid := len(record) < 5 || strings.HasPrefix(record[0], "#")
		if isInvalid {
			g.recordErrChan <- csv.NewRecordError(URL, record)
			continue
		}
		g.countryMap[record[0]] = country{iso: record[0], name: record[4]}
	}
}

func main() {
	config.Init()
	recordErrChan := make(chan error)
	go func() {
		for recordErr := range recordErrChan {
			fmt.Println(recordErr)
		}
	}()
	g := NewGeoname(recordErrChan)
	fmt.Println(g.countryMap)
}