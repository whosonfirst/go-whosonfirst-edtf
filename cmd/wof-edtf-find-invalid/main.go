package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"github.com/sfomuseum/go-edtf/parser"
	"github.com/tidwall/gjson"
	"github.com/whosonfirst/go-whosonfirst-iterate/emitter"
	"github.com/whosonfirst/go-whosonfirst-iterate/iterator"
	"io"
	"log"
	"os"
	"strings"
)

func main() {

	emitter_schemes := strings.Join(emitter.Schemes(), ",")
	iterator_desc := fmt.Sprintf("A valid whosonfirst/go-whosonfirst-iterate/emitter URI. Supported emitter URI schemes are: %s", emitter_schemes)

	iterator_uri := flag.String("iterator-uri", "repo://", iterator_desc)

	include_path := flag.Bool("include-path", true, "Include path of relevant Who's On First record in output.")
	include_key := flag.Bool("include-key", true, "Include edtf: property of relevant Who's On First record in output.")

	flag.Parse()

	uris := flag.Args()

	ctx := context.Background()

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

	iter_cb := func(ctx context.Context, fh io.ReadSeeker, args ...interface{}) error {

		path, err := emitter.PathForContext(ctx)

		if err != nil {
			return err
		}

		body, err := io.ReadAll(fh)

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

	iter, err := iterator.NewIterator(ctx, *iterator_uri, iter_cb)

	if err != nil {
		log.Fatal(err)
	}

	err = iter.IterateURIs(ctx, uris...)

	done_ch <- true

	if err != nil {
		log.Fatal(err)
	}
}
