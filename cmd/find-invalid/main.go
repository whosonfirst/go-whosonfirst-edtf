package main

import (
	"context"
	"encoding/csv"
	"flag"
	"github.com/tidwall/gjson"
	"github.com/whosonfirst/go-edtf/parser"
	"github.com/whosonfirst/go-whosonfirst-index"
	_ "github.com/whosonfirst/go-whosonfirst-index/fs"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

func main() {

	indexer_uri := flag.String("indexer-uri", "repo://", "A valid whosonfirst/go-whosonfirst-index URI.")

	include_path := flag.Bool("include-path", true, "Include path of relevant Who's On First record in output.")
	include_key := flag.Bool("include-key", true, "Include edtf: property of relevant Who's On First record in output.")

	flag.Parse()

	wr := io.MultiWriter(os.Stdout)
	csv_wr := csv.NewWriter(wr)

	done_ch := make(chan bool)
	write_ch := make(chan []string)

	go func() {

		// error handling...

		for {
			select {
			case <-done_ch:
				csv_wr.Flush()
				return
			case out := <-write_ch:
				csv_wr.Write(out)
			}
		}
	}()

	cb := func(ctx context.Context, fh io.Reader, args ...interface{}) error {

		path, err := index.PathForContext(ctx)

		if err != nil {
			return err
		}

		body, err := ioutil.ReadAll(fh)

		if err != nil {
			return err
		}

		candidates := make(map[string]string)

		props := gjson.GetBytes(body, "properties")

		if !props.Exists() {
			return nil
		}

		for k, v := range props.Map() {

			if !strings.HasPrefix(k, "edtf:") {
				continue
			}

			candidates[k] = v.String()
		}

		for k, v := range candidates {

			if parser.IsValid(v) {
				continue
			}

			out := make([]string, 0)

			if *include_path {
				out = append(out, path)
			}

			if *include_key {
				out = append(out, k)
			}

			out = append(out, v)

			write_ch <- out
			return nil
		}

		return nil
	}

	i, err := index.NewIndexer(*indexer_uri, cb)

	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	paths := flag.Args()

	err = i.Index(ctx, paths...)

	done_ch <- true

	if err != nil {
		log.Fatal(err)
	}
}
