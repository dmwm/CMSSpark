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
	if ftrace != "" {
		file, err := os.Create(ftrace)
		checkError("Cannot create file", err)
		defer file.Close()
		trace.Start(file)
		defer trace.Stop()
	}
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
				//                 return err
				f.SetInt(-1)
			} else {
				f.SetInt(ival)
			}
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
// 'date','site','dataset','size','replica_date', 'groupid'
type PhedexRecord struct {
	Date  int64
	Site  string
	Name  string
	Size  int64
	RDate int64
	Gid   int64
}

// Key is a key structure of Phedex map
type Key struct {
	Site  string
	Name  string
	RDate int64
	Gid   int64
}

// Value is a value structure of Phedex map
type Value struct {
	MinDate  int64
	MaxDate  int64
	AveSize  int64
	MaxSize  int64
	Days     int64
	LastSize int64
}

// Map is new type for phedex map
type Map struct {
	sync.RWMutex
	m map[Key]Value
	g map[Key]int64
}

// Rdict is a global dict which keep phedex map
var Rdict Map

// helper function to process phedex directory produced on HDFS with given dates
func process(idir, idates, fout string) {
	Rdict.m = make(map[Key]Value)
	Rdict.g = make(map[Key]int64)
	dates := strings.Split(idates, "-")
	mind, _ := strconv.Atoi(dates[0])
	maxd, _ := strconv.Atoi(dates[len(dates)-1])
	for d := mind; d <= maxd; d++ {
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

	postProcess()

	fmt.Println("### final map", len(Rdict.m))
	file, err := os.Create(fout)
	checkError("Cannot create file", err)
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	o := []string{"site", "dataset", "rdate", "gid", "min_date", "max_date", "ave_size", "max_size", "days"}
	err = writer.Write(o)
	checkError("Cannot write to file", err)
	for k, v := range Rdict.m {
		rDate := fmt.Sprintf("%d", k.RDate)
		gid := fmt.Sprintf("%d", k.Gid)
		minDate := fmt.Sprintf("%d", v.MinDate)
		maxDate := fmt.Sprintf("%d", v.MaxDate)
		aveSize := fmt.Sprintf("%d", v.AveSize)
		maxSize := fmt.Sprintf("%d", v.MaxSize)
		days := fmt.Sprintf("%d", v.Days)
		o := []string{k.Site, k.Name, rDate, gid, minDate, maxDate, aveSize, maxSize, days}
		err := writer.Write(o)
		checkError("Cannot write to file", err)
	}
}

// helper function to perform post-process action
func postProcess() {
	// we don't need locks here since this function is not running from go-routine
	m := make(map[Key]Value)
	for key, v := range Rdict.m {
		aveSize := v.AveSize
		if v.Days == 1 {
			aveSize = v.LastSize
		} else {
			aveSize = (v.AveSize*v.Days + v.LastSize) / (v.Days + 1)
		}
		m[key] = Value{MinDate: v.MinDate, MaxDate: v.MaxDate, AveSize: aveSize, MaxSize: v.MaxSize, Days: v.Days, LastSize: v.LastSize}
	}
	Rdict.m = m
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

// helper function to update global phedex map (Rdict). It parses given set of records
// date site dataset size replica_date, e.g.
// 20170101 T2_CH_CERN /AlCaLumiPixels/Run2012A-ALCARECOLumiPixels-v1/ALCARECO 4655060 20120504
// and produces new dataframe
// site dataset min_date max_date ave_size last_size days
func updateMap(records []PhedexRecord) {
	for _, r := range records {
		gid := r.Gid
		if gid == 0 { // null value in CSV
			gid = -1
		}
		key := Key{Site: r.Site, Name: r.Name, RDate: r.RDate, Gid: gid}
		keyGid := Key{Site: r.Site, Name: r.Name, RDate: r.RDate}

		// look-up giddict
		Rdict.RLock()
		g, ok := Rdict.g[keyGid]
		Rdict.RUnlock()
		if ok {
			// look for a convertion from valid->-1 or -1->valid
			lastGid := g
			if lastGid != gid {
				if gid == -1 && lastGid != -1 {
					gid = lastGid
					key = Key{Site: r.Site, Name: r.Name, RDate: r.RDate, Gid: gid}
				}
				if lastGid == -1 && gid != -1 {
					keyDel := Key{Site: r.Site, Name: r.Name, RDate: r.RDate, Gid: lastGid}
					Rdict.Lock()
					Rdict.m[key] = Rdict.m[keyDel]
					delete(Rdict.m, keyDel)
					Rdict.Unlock()
				}
			}
		}

		var val Value
		Rdict.RLock()
		v, ok := Rdict.m[key]
		Rdict.RUnlock()
		if ok {
			lastSize := v.LastSize
			aveSize := v.AveSize
			days := v.Days
			if r.Date != v.MaxDate {
				if v.Days == 1 {
					aveSize = v.LastSize
				} else {
					aveSize = (v.AveSize*v.Days + v.LastSize) / (v.Days + 1)
				}
				days = v.Days + 1
				lastSize = 0
			}
			minDate := min(r.Date, v.MinDate)
			maxDate := max(r.Date, v.MaxDate)
			lastSize += r.Size
			maxSize := v.MaxSize
			if lastSize > maxSize {
				maxSize = lastSize
			}
			val = Value{MinDate: minDate, MaxDate: maxDate, AveSize: aveSize, MaxSize: maxSize, Days: days, LastSize: lastSize}
		} else {
			val = Value{MinDate: r.Date, MaxDate: r.Date, AveSize: r.Size, MaxSize: r.Size, Days: 1, LastSize: r.Size}
		}
		Rdict.Lock()
		Rdict.m[key] = val
		Rdict.g[keyGid] = gid
		Rdict.Unlock()
	}
}
