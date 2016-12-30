package csv

import (
	"encoding/csv"
	"log"
	"fmt"
	"net/http"
	"io"
	"errors"
)

type RecordError struct {
	file string
	err  error
}

func NewRecordError(URL string, record []string) *RecordError {
	return &RecordError{ URL, errors.New(fmt.Sprintf("Invalid record %s", record)) }
}

func (e *RecordError) Error() string { return fmt.Sprintf("%s in file %s", e.err, e.file) }

func ReadRemoteCSV(URL string, recordChan chan []string, recordErrChan chan error) {
	fmt.Printf("Downloading %s\n", URL)
	resp, err := http.Get(URL)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	reader := csv.NewReader(resp.Body)
	reader.Comma = '\t'
	reader.LazyQuotes = true
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			recordErrChan <- &RecordError{ URL, err }
		}
		recordChan <- record
	}
	close(recordChan)
}