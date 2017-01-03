package main

import (
	"sync"
	"github.com/spf13/viper"
	"github.com/framis/gocity/lib"
	"strings"
	"github.com/framis/gocity/config"
	"fmt"
	"strconv"
	"errors"
	"github.com/framis/gocity/model"
	"time"
)

type admin struct {
	code string
	name string
}

type country struct {
	iso string
	name string
}

type hierarchy struct {
	parent string
	child string
}

type postalCode struct {
	country string
	postalCode string
	name string
	admin1Code string
}

type cityDupCandidate struct {
	city model.City
	hierarchy hierarchy
}

type GeonameImporter struct {
	admin1Map        	map[string]admin
	admin2Map        	map[string]admin
	countryMap       	map[string]country
	postalCodeMap    	map[string]postalCode
	hierarchyParentMap     	map[string]hierarchy
	isACityMap     		map[string]bool
	hierarchyChildMap     	map[string]hierarchy
	cityInChan       	chan([]string)
	cityDupCandidates    	[]cityDupCandidate
	cityOutChan           	chan model.City
	recordErrChan    	chan error
	filePath         	string
}

type importError struct {
	file string
	record interface{}
	err  error
}

func newParseError(URL string, record interface{}, err error) *importError {
	return &importError{URL, record, errors.New("ParseError, " + err.Error())}
}

func newValidationError(URL string, record interface{}) *importError {
	return &importError{URL, record, errors.New("record is invalid")}
}

func newDupError(URL string, record interface{}) *importError {
	return &importError{URL, record, errors.New("record is a duplicate")}
}

func (e *importError) Error() string { return fmt.Sprintf("ImportError - File %s - record %s - %s", e.file, e.record, e.err) }


func NewGeonameImporter(recordErrChan chan error, cityOutChan chan model.City) *GeonameImporter {

	mainFile := viper.GetString("geonames.mainFile")

	g := GeonameImporter{
		recordErrChan: recordErrChan,
		cityInChan: make(chan []string),
		cityOutChan: cityOutChan,
		cityDupCandidates: make([]cityDupCandidate, 0),
		filePath: viper.GetString("download.folder") + "/" + mainFile[0:len(mainFile)-4] + ".txt",
	}
	var wg sync.WaitGroup
	wg.Add(5)
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
	go func() {
		g.populatePostalCode()
		wg.Done()
	}()
	go func() {
		g.populateHierarchy()
		wg.Done()
	}()
	wg.Wait()
	return &g
}

func (g *GeonameImporter) populateAdmin1() {
	g.admin1Map = make(map[string]admin)
	recordChan := make(chan([]string))
	URL :=  viper.GetString("geonames.baseURL") + viper.GetString("geonames.admin1File")

	go lib.ReadRemoteCSV(URL, recordChan, g.recordErrChan)
	for record := range recordChan {
		if len(record) < 2 {
			g.recordErrChan <- newValidationError(URL, record)
			continue
		}
		g.admin1Map[record[0]] = admin{code: record[0], name: record[1]}
	}
}

func (g *GeonameImporter) populateAdmin2() {
	g.admin2Map = make(map[string]admin)
	recordChan := make(chan([]string))
	URL := viper.GetString("geonames.baseURL") + viper.GetString("geonames.admin2File")

	go lib.ReadRemoteCSV(URL, recordChan, g.recordErrChan)
	for record := range recordChan {
		isInvalid := len(record) < 2
		if isInvalid {
			g.recordErrChan <- newValidationError(URL, record)
			continue
		}
		g.admin2Map[record[0]] = admin{code: record[0], name: record[1]}
	}
}

func (g *GeonameImporter) populateCountry() {
	g.countryMap = make(map[string]country)
	recordChan := make(chan([]string))
	URL := viper.GetString("geonames.baseURL") + viper.GetString("geonames.countryFile")

	go lib.ReadRemoteCSV(URL, recordChan, g.recordErrChan)
	for record := range recordChan {
		isInvalid := len(record) < 5 || strings.HasPrefix(record[0], "#")
		if isInvalid {
			g.recordErrChan <- newValidationError(URL, record)
			continue
		}
		g.countryMap[record[0]] = country{iso: record[0], name: record[4]}
	}
}

func (g *GeonameImporter) populatePostalCode() {
	g.postalCodeMap = make(map[string]postalCode)
	recordChan := make(chan([]string))

	zipFile := viper.GetString("geonames.zipFile")

	lib.DownloadAndUnzip(viper.GetString("geonames.zipBaseURL") + zipFile, viper.GetString("download.zipFolder"))

	filePath := viper.GetString("download.zipFolder") + "/" + zipFile[0:len(zipFile)-4] + ".txt"
	go lib.ReadLocalCSV(filePath, recordChan, g.recordErrChan)

	for record := range recordChan {
		isInvalid := len(record) < 5
		if isInvalid {
			g.recordErrChan <- newValidationError(filePath, record)
			continue
		}
		postalCode := postalCode{
			country: record[0],
			postalCode: record[1],
			name: record[2],
			admin1Code: record[4],
		}
		key := fmt.Sprintf("%s.%s.%s",
			postalCode.country,postalCode.name,postalCode.admin1Code)
		// Simple duplicate resolution: take the first one
		if _, present := g.postalCodeMap[key]; !present {
			g.postalCodeMap[key] = postalCode
		}

	}
}

func (g *GeonameImporter) populateHierarchy() {
	g.hierarchyChildMap = make(map[string]hierarchy)
	g.hierarchyParentMap = make(map[string]hierarchy)
	g.isACityMap = make(map[string]bool)
	recordChan := make(chan([]string))

	hierarchyFile := viper.GetString("geonames.hierarchyFile")
	lib.DownloadAndUnzip(viper.GetString("geonames.baseURL") + hierarchyFile,
		viper.GetString("download.folder"))

	filePath := viper.GetString("download.folder") + "/" + hierarchyFile[0:len(hierarchyFile)-4] + ".txt"
	go lib.ReadLocalCSV(filePath, recordChan, g.recordErrChan)

	for record := range recordChan {
		isInvalid := len(record) < 2
		if isInvalid {
			g.recordErrChan <- newValidationError(filePath, record)
			continue
		}
		hierarchy := hierarchy{ parent: record[0], child: record[1] }
		g.hierarchyChildMap[record[1]] = hierarchy
		g.hierarchyParentMap[record[0]] = hierarchy

	}
}

func (g *GeonameImporter) importCities() {
	go lib.ReadLocalCSV(g.filePath, g.cityInChan, g.recordErrChan)
	for record := range g.cityInChan {
		if !g.isValid(record) {
			continue
		}
		city := g.mapToCity(record)
		if !g.filter(city) {
			continue
		}
		g.enrich(&city)
		if potentialDup, cityDupCandidate := g.isDupCandidate(city); potentialDup {
			g.cityDupCandidates = append(g.cityDupCandidates, cityDupCandidate)
		} else {
			g.cityOutChan <- city
		}
	}
}

func (g *GeonameImporter) isValid(record []string) bool {
	valid := len(record) > 14
	if !valid {
		g.recordErrChan <- newValidationError(g.filePath, record)
	}
	return valid
}

func (g *GeonameImporter) mapToCity(record []string) model.City {
	latitude, err := strconv.ParseFloat(record[4], 64)
	if err != nil {
		g.recordErrChan <- newParseError(g.filePath, record, err)
	}
	longitude, err := strconv.ParseFloat(record[5], 64)
	if err != nil {
		g.recordErrChan <- newParseError(g.filePath, record, err)
	}
	population, err := strconv.Atoi(record[14])
	if err != nil {
		g.recordErrChan <- newParseError(g.filePath, record, err)
	}

	return model.City{
		GeonameId: record[0],
		Name: record[1],
		AlternateNames: record[3],
		Latitude: latitude,
		Longitude: longitude,
		FClass: record[6],
		FCode: record[7],
		CountryCode: record[8],
		AdministrativeCode: record[10],
		Administrative2Code: record[11],
		Population: population,
	}
}

func (g *GeonameImporter) filter(city model.City) bool {

	// TODO validate that population=0 is reliable
	if city.Population == 0 {
		return false
	}

	ignoreFCodes := map[string]bool{ "PPLH": true, "PPLX": true }
	if _, present := ignoreFCodes[city.FCode]; present {
		return false
	}

	if city.FClass != viper.GetString("geonames.cityFClass"){
		return false
	}
	return true
}

func (g *GeonameImporter) enrich(city *model.City) {

	if country, present := g.countryMap[city.CountryCode]; present {
		city.Country = country.name
	}

	admin1Key := fmt.Sprintf("%s.%s", city.CountryCode, city.AdministrativeCode)
	if administrative, present := g.admin1Map[admin1Key]; present {
		city.Administrative = administrative.name
	}

	admin2Key := fmt.Sprintf("%s.%s.%s", city.CountryCode,
		city.AdministrativeCode, city.Administrative2Code)
	if administrative2, present := g.admin2Map[admin2Key]; present {
		city.Administrative2 = administrative2.name
	}

	postalCodeKey := fmt.Sprintf("%s.%s.%s", city.CountryCode,
		city.Name, city.AdministrativeCode)

	if postalCode, present := g.postalCodeMap[postalCodeKey]; present {
		city.PostalCode = postalCode.postalCode
	}
}

// We need both parent and children cities to dedup
func (g *GeonameImporter) isDupCandidate(city model.City) (bool, cityDupCandidate) {
	if _, present := g.hierarchyParentMap[city.GeonameId]; present {
		g.isACityMap[city.GeonameId] = true
	}
	if hierarchy, present := g.hierarchyChildMap[city.GeonameId]; present {
		return true, cityDupCandidate{city, hierarchy}
	}
	return false, cityDupCandidate{}
}

// Some cities, such as Marseille in France, have duplicates such as Marseille 01.
// This method uses hierarchy.txt to take the higher hierarchy city only
func (g *GeonameImporter) handleDupCandidates() {
	for _, cityDupCandidate := range g.cityDupCandidates {
		parentId := cityDupCandidate.hierarchy.parent
		isParentACity, _ := g.isACityMap[parentId]

		if isParentACity {
			g.recordErrChan <- newDupError(g.filePath, cityDupCandidate.city)
			continue
		}
		g.cityOutChan <- cityDupCandidate.city
	}
}

func (g *GeonameImporter) teardown() {
	close(g.cityOutChan)
}

func main() {
	config.Init()
	recordErrChan := make(chan error)
	go func() {
		for recordErr := range recordErrChan {
			fmt.Println(recordErr)
		}
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		lib.DownloadAndUnzip(viper.GetString("geonames.baseURL") + viper.GetString("geonames.mainFile"),
			viper.GetString("download.folder"))
		wg.Done()
	}()
	wg.Wait()

	cityOutChan := make(chan model.City)
	g := NewGeonameImporter(recordErrChan, cityOutChan)

	wg.Add(1)
	go func() {
		indexer := lib.NewAlgoliaIndexer(
			viper.GetString("algolia.appId"),
			viper.GetString("algolia.appSecret"),
			viper.GetString("algolia.indexName"),
			cityOutChan)
		indexer.Index()
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		g.importCities()
		g.handleDupCandidates()
		time.Sleep(30*1000)
		close(cityOutChan)
		wg.Done()
	}()

	wg.Wait()
}