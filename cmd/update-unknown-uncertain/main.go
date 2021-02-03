package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/sfomuseum/go-edtf"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"github.com/whosonfirst/go-whosonfirst-export/v2"
	"github.com/whosonfirst/go-whosonfirst-index"
	_ "github.com/whosonfirst/go-whosonfirst-index/fs"
	wof_writer "github.com/whosonfirst/go-whosonfirst-writer"
	"github.com/whosonfirst/go-writer"
	"io"
	"io/ioutil"
	"log"
	"strings"
)

func main() {

	indexer_uri := flag.String("indexer-uri", "repo://", "A valid whosonfirst/go-whosonfirst-index URI.")
	exporter_uri := flag.String("exporter-uri", "whosonfirst://", "A valid whosonfirst/go-whosonfirst-export URI.")
	writer_uri := flag.String("writer-uri", "null://", "A valid whosonfirst/go-writer URI.")

	flag.Parse()

	ctx := context.Background()

	ex, err := export.NewExporter(ctx, *exporter_uri)

	if err != nil {
		log.Fatalf("Failed to create exporter for '%s', %v", *exporter_uri, err)
	}

	wr, err := writer.NewWriter(ctx, *writer_uri)

	if err != nil {
		log.Fatalf("Failed to create writer for '%s', %v", *writer_uri, err)
	}

	cb := func(ctx context.Context, fh io.Reader, args ...interface{}) error {

		path, err := index.PathForContext(ctx)

		if err != nil {
			return err
		}

		body, err := ioutil.ReadAll(fh)

		if err != nil {
			return err
		}

		props := gjson.GetBytes(body, "properties")

		if !props.Exists() {
			return nil
		}

		id_rsp := gjson.GetBytes(body, "properties.wof:id")

		if !id_rsp.Exists() {
			return errors.New("Missing wof:id property")
		}

		id := id_rsp.Int()

		changed := false

		for k, v := range props.Map() {

			if !strings.HasPrefix(k, "edtf:") {
				continue
			}

			path := fmt.Sprintf("properties.%s", k)

			var err error

			switch v.String() {
			case "open":

				body, err = sjson.SetBytes(body, path, edtf.OPEN)

				if err != nil {
					return err
				}

				changed = true

			case "uuuu":

				body, err = sjson.SetBytes(body, path, edtf.UNKNOWN)

				if err != nil {
					return err
				}

				changed = true

			default:
				// pass
			}
		}

		if !changed {
			return nil
		}

		new_body, err := ex.Export(ctx, body)

		if err != nil {
			return err
		}

		err = wof_writer.WriteFeatureBytes(ctx, wr, new_body)

		if err != nil {
			return err
		}

		log.Printf("Updated %d (%s)\n", id, path)
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

	if err != nil {
		log.Fatal(err)
	}
}
