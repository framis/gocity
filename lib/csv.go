package csv

import (
	"encoding/csv"
	"log"
	"fmt"
	"net/http"
	"io"
	"os"
	"strings"
	"path/filepath"
	"archive/zip"
	"bufio"
)

type CsvError struct {
	file string
	line int
	err  error
}

func (e *CsvError) Error() string { return fmt.Sprintf("File %s - line %d - error %s", e.file, e.line, e.err) }

// Reading a remote CSV file and sending each record to recordChan and parse errors to recordErrChan
// Throws Fatal error if download fails
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
	line := 0
	for {
		line++
		record, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			recordErrChan <- &CsvError{ URL, line, err }
		}
		recordChan <- record
	}
	close(recordChan)
}

// Reading a remote CSV file and sending each record to recordChan and parse errors to recordErrChan
// Throws Fatal error if download fails
func ReadLocalCSV(filePath string, recordChan chan []string, recordErrChan chan error) {
	fmt.Printf("Reading %s\n", filePath)
	f, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	reader := csv.NewReader(bufio.NewReader(f))
	reader.Comma = '\t'
	reader.LazyQuotes = true
	line := 0
	for {
		line++
		record, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			recordErrChan <- &CsvError{ filePath, line, err }
		}
		recordChan <- record
	}
	close(recordChan)
}

// Downloading remote file to dest folder
// Throws Fatal error if download fails
func DownloadAndUnzip(url string, dest string) {
	tokens := strings.Split(url, "/")
	fileName := tokens[len(tokens)-1]
	err := os.MkdirAll(dest, 0755)
	if err != nil {
		log.Fatal("Error while folder", dest, "-", err)
		return
	}
	filePath := dest+"/"+fileName

	fmt.Println("Downloading", url, "to", filePath)
	output, err := os.Create(filePath)
	if err != nil {
		log.Fatal("Error while creating", fileName, "-", err)
		return
	}
	defer output.Close()

	response, err := http.Get(url)
	if err != nil {
		log.Fatal("Error while downloading", url, "-", err)
		return
	}
	defer response.Body.Close()

	n, err := io.Copy(output, response.Body)
	if err != nil {
		log.Fatal("Error while downloading", url, "-", err)
		return
	}
	fmt.Println(n, "bytes downloaded.")
	err = Unzip(filePath, dest)
	if err != nil {
		log.Fatal("Error while unzipping", filePath, "to", dest, "-", err)
		return
	}
}

// Utility function to Unzip a zip file
func Unzip(src, dest string) error {
	fmt.Println("Unzipping", src, "to", dest)
	r, err := zip.OpenReader(src)
	if err != nil {
		return err
	}
	defer func() {
		if err := r.Close(); err != nil {
			panic(err)
		}
	}()

	os.MkdirAll(dest, 0755)

	// Closure to address file descriptors issue with all the deferred .Close() methods
	extractAndWriteFile := func(f *zip.File) error {
		rc, err := f.Open()
		if err != nil {
			return err
		}
		defer func() {
			if err := rc.Close(); err != nil {
				panic(err)
			}
		}()

		path := filepath.Join(dest, f.Name)

		if f.FileInfo().IsDir() {
			os.MkdirAll(path, f.Mode())
		} else {
			os.MkdirAll(filepath.Dir(path), f.Mode())
			f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
			if err != nil {
				return err
			}
			defer func() {
				if err := f.Close(); err != nil {
					panic(err)
				}
			}()

			_, err = io.Copy(f, rc)
			if err != nil {
				return err
			}
		}
		return nil
	}

	for _, f := range r.File {
		err := extractAndWriteFile(f)
		if err != nil {
			return err
		}
	}

	return nil
}