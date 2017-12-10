package main

// Author: Valentin Kuznetsov < vkuznet {at} gmail [dot] com >
// mergePhedex is a Go implementation of mergePhedex.py script in CMSSpark package
// 1 year of Phedex data is parsed in about 40min

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"runtime/trace"
	"strconv"
	"strings"
	"sync"
	"time"
)

func main() {
	var dates string
	flag.StringVar(&dates, "dates", "", "Date range")
	var idir string
	flag.StringVar(&idir, "idir", "", "Input directory")
	var fout string
	flag.StringVar(&fout, "fout", "", "Output file")
	var ftrace string
	flag.StringVar(&ftrace, "ftrace", "", "Output trace file")
	flag.Parse()
	if fout == "" || idir == "" || dates == "" {
		log.Fatal("Please check input variables")
	}
	file, err := os.Create(ftrace)
	checkError("Cannot create file", err)
	defer file.Close()
	trace.Start(file)
	defer trace.Stop()
	process(idir, dates, fout)
}

// CSV parser/unmarsheler
// FiledMismatch structure holds mismatched attributes found by unmarshalling CSV records
type FieldMismatch struct {
	expected, found int
}

// Error produces error message of mismatched fields
func (e *FieldMismatch) Error() string {
	return "CSV line fields mismatch. Expected " + strconv.Itoa(e.expected) + " found " + strconv.Itoa(e.found)
}

// UnsupportedType provides new structure for unsupported type
type UnsupportedType struct {
	Type string
}

// Error produces error message of unsupported type
func (e *UnsupportedType) Error() string {
	return "Unsupported type: " + e.Type
}

// Unmarshal provide functionality to unmarshl CSV records
func Unmarshal(reader *csv.Reader, v interface{}) error {
	record, err := reader.Read()
	if err != nil {
		return err
	}
	s := reflect.ValueOf(v).Elem()
	if s.NumField() != len(record) {
		return &FieldMismatch{s.NumField(), len(record)}
	}
	for i := 0; i < s.NumField(); i++ {
		f := s.Field(i)
		switch f.Type().String() {
		case "string":
			f.SetString(record[i])
		case "int64":
			ival, err := strconv.ParseInt(record[i], 10, 0)
			if err != nil {
				return err
			}
			f.SetInt(ival)
		default:
			return &UnsupportedType{f.Type().String()}
		}
	}
	return nil
}

// helper min function
func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// helper min function
func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// PhedexRecord
type PhedexRecord struct {
	Date  int64
	Site  string
	Name  string
	Size  int64
	RDate int64
}

// Key is a key structure of Phedex map
type Key struct {
	Site string
	Name string
}

// Value is a value structure of Phedex map
type Value struct {
	MinDate  int64
	MaxDate  int64
	MinRDate int64
	MaxRDate int64
	MinSize  int64
	MaxSize  int64
	Days     int64
}

// Map is new type for phedex map
type Map struct {
	sync.RWMutex
	m map[Key]Value
}

// Rdict is a global dict which keep phedex map
var Rdict Map

// helper function to process phedex directory produced on HDFS with given dates
func process(idir, idates, fout string) {
	Rdict.m = make(map[Key]Value)
	dates := strings.Split(idates, "-")
	mind, _ := strconv.Atoi(dates[0])
	maxd, _ := strconv.Atoi(dates[len(dates)-1])
	for d := mind; d < maxd; d++ {
		fname := fmt.Sprintf("%s/%d", idir, d)
		if _, err := os.Stat(fname); os.IsNotExist(err) {
			continue
		}
		fmt.Println(fname)
		files, err := ioutil.ReadDir(fname)
		if err != nil {
			panic(err)
		}
		var parts []string
		for _, v := range files {
			if strings.HasPrefix(v.Name(), "part-") {
				f := fmt.Sprintf("%s/%s", fname, v.Name())
				parts = append(parts, f)
			}
		}
		var wg sync.WaitGroup
		for _, f := range parts {
			wg.Add(1)
			go processFile(f, &wg)
		}
		wg.Wait()
	}
	fmt.Println("### final map", len(Rdict.m))
	file, err := os.Create(fout)
	checkError("Cannot create file", err)
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	o := []string{"size", "dataset", "min_date", "max_date", "min_rdate", "max_rdate", "min_size", "max_size", "days"}
	err = writer.Write(o)
	checkError("Cannot write to file", err)
	for k, v := range Rdict.m {
		minDate := fmt.Sprintf("%d", v.MinDate)
		maxDate := fmt.Sprintf("%d", v.MaxDate)
		minRDate := fmt.Sprintf("%d", v.MinRDate)
		maxRDate := fmt.Sprintf("%d", v.MaxRDate)
		minSize := fmt.Sprintf("%d", v.MinSize)
		maxSize := fmt.Sprintf("%d", v.MaxSize)
		days := fmt.Sprintf("%d", v.Days)
		o := []string{k.Site, k.Name, minDate, maxDate, minRDate, maxRDate, minSize, maxSize, days}
		err := writer.Write(o)
		checkError("Cannot write to file", err)
	}
}

// helper function to report the error
func checkError(message string, err error) {
	if err != nil {
		log.Fatal(message, err)
	}
}

// helper function to process file with phedex records
func processFile(fname string, wg *sync.WaitGroup) {
	defer wg.Done()
	file, e := os.Open(fname)
	if e != nil {
		panic(e)
	}
	var reader = csv.NewReader(file)
	var rec PhedexRecord
	var records []PhedexRecord
	for {
		err := Unmarshal(reader, &rec)
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		records = append(records, rec)
	}
	fmt.Println(fname, len(records))
	updateMap(records)
}

// helper function to return seconds since epoch for given YYYYDDMM date
func unix(d int64) int64 {
	s := fmt.Sprintf("%d", d)
	var month string
	m := s[4:6]
	switch m {
	case "01":
		month = "Jan"
	case "02":
		month = "Feb"
	case "03":
		month = "Mar"
	case "04":
		month = "Apr"
	case "05":
		month = "May"
	case "06":
		month = "Jun"
	case "07":
		month = "Jul"
	case "08":
		month = "Aug"
	case "09":
		month = "Sep"
	case "10":
		month = "Oct"
	case "11":
		month = "Nov"
	case "12":
		month = "Dec"
	}
	const longForm = "Jan 2, 2006"
	t, _ := time.Parse(longForm, fmt.Sprintf("%s %s, %s", month, s[6:8], s[0:4]))
	return t.Unix()
}

// helper function to calculate number of days between given dates
func daysPresent(minDate, maxDate, minRDate, maxRDate int64) int64 {
	mind := min(minDate, minRDate)
	maxd := max(maxDate, maxRDate)
	secs := unix(maxd) - unix(mind)
	return secs / (60 * 60 * 24)
}

// helper function to update global phedex map (Rdict). It parse given set of records
// 20170101 T2_CH_CERN /AlCaLumiPixels/Run2012A-ALCARECOLumiPixels-v1/ALCARECO 4655060 20120504
// and produce site, dataset, min_date, max_date, min_rdate, max_rdate, min_size, max_size, days
// dataframe
func updateMap(records []PhedexRecord) {
	for _, r := range records {
		key := Key{Site: r.Site, Name: r.Name}
		var val Value
		Rdict.RLock()
		v, ok := Rdict.m[key]
		Rdict.RUnlock()
		if ok {
			minDate := min(v.MinDate, r.Date)
			maxDate := max(v.MaxDate, r.Date)
			minRDate := min(v.MinRDate, r.RDate)
			maxRDate := max(v.MaxRDate, r.RDate)
			minSize := min(v.MinSize, r.Size)
			maxSize := max(v.MaxSize, r.Size)
			days := daysPresent(minDate, maxDate, minRDate, maxRDate)
			val = Value{MinDate: minDate, MaxDate: maxDate, MinRDate: minRDate, MaxRDate: maxRDate, MinSize: minSize, MaxSize: maxSize, Days: days}
		} else {
			val = Value{MinDate: r.Date, MaxDate: r.Date, MinRDate: r.RDate, MaxRDate: r.RDate, MinSize: r.Size, MaxSize: r.Size, Days: -1}
		}
		Rdict.Lock()
		Rdict.m[key] = val
		Rdict.Unlock()
	}
}
